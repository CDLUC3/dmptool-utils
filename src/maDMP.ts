import { Validator } from 'jsonschema';
import { Logger } from 'pino';
import {ConnectionParams, queryTable} from './rds';
import {
  convertMySQLDateTimeToRFC3339,
  EnvironmentEnum,
  isNullOrUndefined,
  normaliseHttpProtocol,
  removeNullAndUndefinedFromObject,
} from "./general";
import {
  AnyResearchOutputTableColumnAnswerType,
  CheckboxesAnswerType,
  DateAnswerType,
  DMPToolDMPType,
  DMPToolExtensionSchema,
  DMPToolExtensionType,
  LicenseSearchAnswerType,
  MetadataStandardSearchAnswerType,
  NumberWithContextAnswerType,
  RadioButtonsAnswerType,
  RDA_COMMON_STANDARD_VERSION,
  RDACommonStandardDMPJSONSchema,
  RepositorySearchAnswerType,
  ResearchOutputTableAnswerType,
  ResearchOutputTableRowAnswerType,
  SelectBoxAnswerType,
  TextAnswerType,
  TextAreaAnswerType
} from "@dmptool/types";
import {
  DMPExtensionFunderOpportunityType,
  DMPExtensionFunderProjectType,
  DMPExtensionNarrative,
  LoadFundingInfo,
  LoadMemberInfo,
  LoadNarrativeQuestionInfo,
  LoadNarrativeSectionInfo,
  LoadPlanInfo,
  LoadProjectInfo,
  LoadRelatedWorkInfo,
  RDACommonStandardAffiliation,
  RDACommonStandardContact,
  RDACommonStandardContributor,
  RDACommonStandardDataset, RDACommonStandardIdentifierType,
  RDACommonStandardProject,
  RDACommonStandardRelatedWork,
  StandardIdentifierType,
} from "./maDMPTypes";

const ROR_REGEX = /^https?:\/\/ror\.org\/[0-9a-zA-Z]+$/;

const DOI_REGEX = /^(https?:\/\/)?(doi\.org\/)?(doi:)?(10\.\d{4,9}\/[-._;()/:\w]+)$/;

const ORCID_REGEX = /^(https?:\/\/)?(www\.|pub\.)?(sandbox\.)?(orcid\.org\/)?([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])$/;

class DMPValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DMPValidationError';
  }
}

/**
 * Ensure that the ORCID is in the correct format (https://orcid.org/0000-0000-0000-0000)
 *
 * @param orcidIn the ORCID to check
 * @returns the ORCID in the correct format or null if it is not in the correct format
 */
function formatORCID(env: EnvironmentEnum, orcidIn: string): string | null {
  // If it is blank or already in the correct format, return it
  if (orcidIn && (orcidIn.match(ORCID_REGEX) && orcidIn.startsWith('http'))) {
    return normaliseHttpProtocol(orcidIn);
  }

  const baseURL: string = env && ['stg', 'prd'].includes(env)
    ? 'https://orcid.org/'
    : 'https://sandbox.orcid.org/';

  // If it matches the ORCID format but didn't start with http then its just the id
  if (orcidIn && orcidIn.match(ORCID_REGEX)) {
    return normaliseHttpProtocol(
      `${baseURL}${orcidIn.split('/').pop()}`
    );
  }

  // Otherwise it's not an ORCID
  return null;
}

/**
 * Determine the identifier type for the given URI string
 *
 * @param uri the URI string to check
 * @returns the RDA Common Standard identifier type for the given URI string
 */
function determineIdentifierType(uri: string): string {
  if (isNullOrUndefined(uri)) {
    return 'other';
  }
  if (uri.match(ORCID_REGEX)) {
    return 'orcid';
  } else if (uri.match(DOI_REGEX)) {
    return 'doi';
  } else if (uri.match(ROR_REGEX)) {
    return 'ror';
  } else if (uri.startsWith('http')) {
    return 'url';
  } else {
    return 'other';
  }
}

/**
 * Function to convert a PlanFunding status to an RDA Common Standard funding_status
 *
 * @param status the PlanFunding status to convert
 * @returns the RDA Common Standard funding_status
 */
// Helper function to convert a ProjectFundingStatus to a DMPFundingStatus
function planFundingStatusToDMPFundingStatus(status: string): string {
  switch (status) {
    case 'DENIED':
      return 'rejected';
    case 'GRANTED':
      return 'granted';
    default:
      return 'planned';
  }
}

/**
 * Function to convert a 5 character language code (e.g. en-US) to a 3 character code (e.g. eng)
 *
 * @param language the 5 character language code to convert
 * @returns the 3 character language code
 */
function convertFiveCharToThreeChar(language: string): string {
  switch (language) {
    case 'pt-BR':
      return 'por';
    default:
      return 'eng';
  }
}

/**
 * Function to generate the base of an internal ID namespace
 *
 * @param applicationName the name of the application/service
 * @param projectId the Project ID to use for the internal ID namespace
 * @param planId the Plan ID to use for the internal ID namespace
 * @returns the base of the internal ID namespace
 */
function internalIdBase(
  applicationName: string,
  projectId: number,
  planId: number
): string {
  return `${applicationName}.projects.${projectId}.dmp.${planId}`
}

/**
 * Fetch the default MemberRole from the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @returns the default MemberRole as a string (or undefined if there is no default)
 */
const loadDefaultMemberRole = async (
  rdsConnectionParams: ConnectionParams
): Promise<string | undefined> => {
  const sql = 'SELECT * FROM memberRoles WHERE isDefault = 1';
  rdsConnectionParams.logger.debug({ sql }, 'Fetching default role');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    []
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    return resp.results[0].id;
  }
  return undefined;
}

/**
 * Fetches the Plan information needed to construct the DMP Common Standard from
 * the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param planId the Plan ID to fetch the Plan information for
 * @returns the Plan information needed to construct the DMP Common Standard
 */
// Fetch the Plan info needed from the MySQL database
const loadPlanInfo = async (
  rdsConnectionParams: ConnectionParams,
  planId: number
): Promise<LoadPlanInfo | undefined> => {
  const sql = `
    SELECT id, dmpId, projectId, versionedTemplateId,
           createdById, created, modifiedById, modified, title,
           status, visibility, featured, registeredById, registered,
           languageId
    FROM plans
    WHERE id = ?
  `;
  rdsConnectionParams.logger.debug({ planId, sql }, 'Fetching plan information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [planId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    return resp.results[0];
  }
  return undefined;
}

/**
 * Fetches the Project information needed to construct the DMP Common Standard
 * from the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param projectId the Project ID to fetch the Project information for
 * @returns the Project information needed to construct the DMP Common Standard
 */
const loadProjectInfo = async (
  rdsConnectionParams: ConnectionParams,
  projectId: number
): Promise<LoadProjectInfo | undefined> => {
  const sql = `
    SELECT id, title, abstractText, startDate, endDate
    FROM projects
    WHERE id = ?
  `;
  rdsConnectionParams.logger.debug({ projectId, sql }, 'Fetching project information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [projectId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    return resp.results[0];
  }
  return undefined;
}

/**
 * Fetches the PlanFunding information needed to construct the DMP Common Standard
 * from the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param planId the Plan ID to fetch the PlanFunding information for
 * @returns the Funding information needed to construct the DMP Common Standard
 */
const loadFundingInfo = async (
  rdsConnectionParams: ConnectionParams,
  planId: number
): Promise<LoadFundingInfo[]> => {
  const sql = `
    SELECT pf.id, a.uri, a.name, prf.status, prf.grantId,
           prf.funderProjectNumber, prf.funderOpportunityNumber
    FROM planFundings pf
      LEFT JOIN projectFundings prf ON pf.projectFundingId = prf.id
      LEFT JOIN affiliations a ON prf.affiliationId = a.uri
    WHERE pf.planId = ?
  `;
  rdsConnectionParams.logger.debug({ planId, sql }, 'Fetching plan funding information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [planId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    const fundings = resp.results.filter((row) => !isNullOrUndefined(row));
    fundings.forEach((funding) =>
      funding.status = planFundingStatusToDMPFundingStatus(funding.status)
    )
    return resp.results;
  }
  return [];
}

/**
 * Fetches the Plan's owner information needed to construct the DMP Common Standard
 * from the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param ownerId the user id for the plan's owner
 * @returns the contact information needed to construct the DMP Common Standard
 */
async function loadContactFromPlanOwner(
  rdsConnectionParams: ConnectionParams,
  ownerId: number
): Promise<LoadMemberInfo | undefined> {
  const sql = `
    SELECT u.id, u.givenName, u.surName, u.orcid, a.uri, a.name,
           (SELECT ue.email
            FROM userEmails ue
            WHERE ue.userId = u.id AND ue.isPrimary = 1 LIMIT 1) as email
    FROM users u
      LEFT JOIN affiliations a ON u.affiliationId = a.id
    WHERE u.id = ?
  `;

  rdsConnectionParams.logger.debug({ ownerId, sql }, 'Fetching plan owner information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [ownerId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    return resp.results.filter((row) => !isNullOrUndefined(row))[0];
  }
  return undefined;
}

/**
 * Fetches the PlanMember information needed to construct the DMP Common Standard
 * from the MySQL database
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param planId the Plan ID to fetch the PlanMember information for
 * @returns the contributor information needed to construct the DMP Common Standard
 */
const loadMemberInfo = async (
  rdsConnectionParams: ConnectionParams,
  planId: number
): Promise<LoadMemberInfo[] | []> => {
  const sql = `
    SELECT pc.id, a.uri, a.name, pctr.email, pctr.givenName, pctr.surName,
           pctr.orcid, pc.isPrimaryContact, GROUP_CONCAT(r.uri) as roles
    FROM planMembers pc
      LEFT JOIN planMemberRoles pcr ON pc.id = pcr.planMemberId
        LEFT JOIN memberRoles r ON pcr.memberRoleId = r.id
      LEFT JOIN projectMembers pctr ON pc.projectMemberId = pctr.id
        LEFT JOIN affiliations a ON pctr.affiliationId = a.uri
    WHERE pc.planId = ?
    GROUP BY a.uri, a.name, pctr.email, pctr.givenName, pctr.surName,
      pctr.orcid, pc.isPrimaryContact;
  `;

  rdsConnectionParams.logger.debug({ planId, sql }, 'Fetching plan member information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [planId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    return resp.results.filter((row) => !isNullOrUndefined(row));
  }
  return [];
}

/**
 * Returns a default RDA Common Standard Dataset entry for the DMP.
 * This is used when the Plan has no Answers to a Research Outputs question.
 *
 * @param applicationName the name of the application/service
 * @param projectId the Project ID to use for the Dataset entry
 * @param planId the Plan ID to use for the Dataset entry
 * @returns a generic default Dataset entry
 */
const defaultDataset = (
  applicationName: string,
  projectId: number,
  planId: number
): RDACommonStandardDataset => {
  return {
    dataset_id: {
      identifier: `${internalIdBase(applicationName, projectId, planId)}.outputs.1`,
      type: 'other'
    },
    personal_data: 'unknown',
    sensitive_data: 'unknown',
    title: 'Generic Dataset',
    type: 'dataset'
  }
}

/**
 * Fetches the Dataset information needed to construct the DMP Common Standard
 * from the MySQL database this information is extracted from the Answers table
 * for Research Output Questions
 *
 * @param rdssConnectionParams the connection parameters for the MySQL database
 * @param applicationName the name of the application/service
 * @param projectId the Project ID to fetch the Dataset information for
 * @param planId the Plan ID to fetch the Dataset information for
 * @param language the language to use for the Dataset information
 * @returns the dataset information needed to construct the DMP Common Standard
 */
const loadDatasetInfo = async (
  rdsConnectionParams: ConnectionParams,
  applicationName: string,
  projectId: number,
  planId: number,
  language = 'eng'
): Promise<RDACommonStandardDataset[] | []> => {
  const datasets: RDACommonStandardDataset[] = [];
  const sql = `
    SELECT a.json
    FROM answers a
    WHERE a.planId = ?
      AND a.json LIKE '%"researchOutputsTable"%';
  `;

  rdsConnectionParams.logger.debug({ projectId, planId, sql }, 'Fetching research output information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [planId.toString()]
  );
  // There would typically only be one research outputs question per plan but
  // we need to allow for multiples just in case.
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    for (const result of resp.results) {
      // Extract the column headings and the rows
      const json: ResearchOutputTableAnswerType = result.json ? JSON.parse(result.json) : {};
      const lang = language === 'eng' ? 'eng' : convertFiveCharToThreeChar(language);

      // Loop through the rows and construct the RDA Common Standard Dataset object
      for (let idx = 0; idx < json.answer.length; idx++) {
        const row = json.answer[idx];
        datasets.push(buildDataset(applicationName, idx, row, projectId, planId, lang));
      }
    }
  } else {
    rdsConnectionParams.logger.debug({ projectId, planId }, 'Using the default dataset');
    return [defaultDataset(applicationName, projectId, planId)];
  }

  return datasets;
}

/**
 * Builds the RDA Common Standard Related Identifier entries for the DMP
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param projectId the Project ID to fetch the Related Works information for
 * @returns the RDA Common Standard Related Identifier entries for the DMP
 */
const loadRelatedWorksInfo = async (
  rdsConnectionParams: ConnectionParams,
  projectId: number
): Promise<RDACommonStandardRelatedWork[] | []> => {
  const sql = `
    SELECT w.doi AS identifier, LOWER(wv.workType) AS workType
    FROM relatedWorks rw
      JOIN workVersions wv ON rw.workVersionId = wv.id
        JOIN works w ON wv.workId = w.id
    WHERE rw.projectId = ?;
  `;

  rdsConnectionParams.logger.debug({ projectId, sql }, 'Fetching related works information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [projectId.toString()]
  );
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    const works = resp.results.filter((row) => !isNullOrUndefined(row));
    // Determine the identifier types
    return works.map((work: LoadRelatedWorkInfo) => {
      return {
        relation_type: 'cites',
        identifier: work.identifier,
        type: determineIdentifierType(work.identifier),
        resource_type: work.workType?.toLowerCase(),
      };
    });
  }
  return [];
}

/**
 * Builds the DMP Tool Narrative extension for the DMP
 *
 * @param rdssConnectionParams the connection parameters for the MySQL database
 * @param planId the Plan ID to fetch the narrative information for
 * @returns the DMP Tool Narrative extension for the DMP
 */
const loadNarrativeTemplateInfo = async (
  rdsConnectionParams: ConnectionParams,
  planId: number
): Promise<DMPExtensionNarrative | undefined> => {
  // Fetch the template, sections, questions and answers all at once
  const sql = `
    SELECT t.id templateId, t.name templateTitle, t.description templateDescription,
           t.version templateVersion,
           s.id sectionId, s.name sectionTitle, s.introduction sectionDescription,
           s.displayOrder sectionOrder, q.id questionId, q.questionText questionText,
           q.displayOrder questionOrder, a.id answerId, a.json answerJSON
    FROM plans p
      INNER JOIN versionedTemplates t ON p.versionedTemplateId = t.id
        LEFT JOIN versionedSections s ON s.versionedTemplateId = t.id
          LEFT JOIN versionedQuestions q ON q.versionedSectionId = s.id
            LEFT JOIN answers a ON a.versionedQuestionId = q.id AND p.id = a.planId
    WHERE p.id = ?
    ORDER BY s.displayOrder, q.displayOrder;
  `;

  rdsConnectionParams.logger.debug({ planId, sql }, 'Fetching narrative information');
  const resp = await queryTable(
    rdsConnectionParams,
    sql,
    [planId.toString()]
  );

  let results = [];
  // Filter out any null or undefined results
  if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
    results = resp.results.filter((row) => !isNullOrUndefined(row));
  }

  if (!Array.isArray(results) || results.length === 0
    || !Array.isArray(results[0].section) || results[0].section.length === 0) {
    return undefined;
  }

  rdsConnectionParams.logger.debug(
    {
      planId,
      nbrRsults: results.length,
      sectionCount: results?.[0]?.section?.length,
      questionCount: results?.[0]?.section?.[0]?.question?.length
    },
    'Loaded narrative information'
  )

  // Sort the questions by display order
  results[0].section.forEach((section: LoadNarrativeSectionInfo) => {
    section.question.sort((a, b) => a.questionOrder - b.questionOrder);
  });
  // Sort the sections by display order
  results[0].section.sort((a: LoadNarrativeSectionInfo, b: LoadNarrativeSectionInfo) => {
    return a.sectionOrder - b.sectionOrder;
  });

  return {
    id: results[0].templateId,
    title: results[0].templateTitle,
    description: results[0].templateDescription,
    version: results[0].templateVersion,
    section: results[0].section.map((section: LoadNarrativeSectionInfo) => {

      rdsConnectionParams.logger.debug(
        {
          sectionId: section.sectionId,
          questionCount: section.question.length
        },
        'Loaded narrative section information'
      )

      return {
        id: section.sectionId,
        title: section.sectionTitle,
        description: section.sectionDescription,
        order: section.sectionOrder,
        question: section.question.map((question: LoadNarrativeQuestionInfo) => {

          rdsConnectionParams.logger.debug(
            {
              questionId: question.questionId,
              answerId: question.answerId
            },
            'Loaded narrative question information'
          )

          return {
            id: question.questionId,
            order: question.questionOrder,
            text: question.questionText,
            answer: {
              id: question.answerId,
              json: question.answerJSON
            }
          };
        })
      };
    })
  };
}

/**
 * Builds the RDA Common Standard Contact entry for the DMP
 *
 * @param rdssConnectionParams the connection parameters for the MySQL database
 * @param plan the Plan information retrieve from the MySQL database
 * @param members the PlanMembers information retrieve from the MySQL database
 * @returns the RDA Common Standard Contact entry for the DMP
 * @throws DMPValidationError if no primary contact is found for the DMP
 */
const buildContact = async (
  rdsConnectionParams: ConnectionParams,
  env: EnvironmentEnum,
  plan: LoadPlanInfo,
  members: LoadMemberInfo[]
): Promise<RDACommonStandardContact> => {
  // Extract the primary contact from the members
  const memberContact: LoadMemberInfo | undefined = members.find((c: LoadMemberInfo) => {
    return c?.isPrimaryContact
  });

  // If no primary contact is available, use the plan owner
  const primary: LoadMemberInfo | undefined = memberContact && memberContact.email
    ? memberContact
    : await loadContactFromPlanOwner(rdsConnectionParams, Number(plan.createdById));

  if (primary && primary.email) {
    const orcid: string | null = primary.orcid ? formatORCID(env, primary.orcid) : null;

    // Build the contact entry for the DMP
    const contactEntry: RDACommonStandardContact = {
      contact_id: [{
        identifier: orcid !== null ? orcid : primary.email,
        type: orcid !== null ? determineIdentifierType(orcid) : 'other',
      }],
      mbox: primary.email,
      name: [primary.givenName, primary.surName].filter((n) => n).join(' ').trim(),
    }

    // Add the affiliation to the Contact if it exists
    if (primary.name) {
      const contactAffiliation: RDACommonStandardAffiliation = {
        name: primary.name
      };
      // Add the URI if it exists
      if (primary.uri) {
        contactAffiliation.affiliation_id = {
          identifier: primary.uri,
          type: determineIdentifierType(primary.uri),
        };
      }
      contactEntry.affiliation = [contactAffiliation];
    }
    return contactEntry;
  } else {
    throw new DMPValidationError('Unable to find a primary contact for the DMP');
  }
}

/**
 * Builds the RDA Common Standard Contributor array for the DMP from the PlanMembers
 *
 * @param applicationName the name of the application/service
 * @param planId the Plan ID
 * @param projectId the Project ID
 * @param members the PlanMembers information retrieve from the MySQL database
 * @param defaultRole the default role to use if the member doesn't have a role'
 * @returns the RDA Common Standard Contributor array for the DMP
 */
const buildContributors = (
  applicationName: string,
  env: EnvironmentEnum,
  planId: number,
  projectId: number,
  members: LoadMemberInfo[],
  defaultRole: string
): RDACommonStandardContributor[] | [] => {
  if (!Array.isArray(members) || members.length <= 0) {
    return [];
  }

  return members.map((member: LoadMemberInfo): RDACommonStandardContributor => {
    // Make sure that we always have roles as an array
    const roles = member.roles && member.roles.includes('[')
      ? JSON.parse(member.roles)
      : [defaultRole];

    // Combine the member's given name and surname into a single name'
    const contrib = {
      name: [member.givenName, member.surName]
        .filter((n) => n).join(' ').trim(),
      role: roles,
    } as RDACommonStandardContributor;

    // Use the member's ORCID if it exists, otherwise generate a new one'
    const formatted: string | null = member.orcid ? formatORCID(env, member.orcid) : null;
    if (formatted !== null) {
      contrib.contributor_id = [{
        identifier: formatted,
        type: determineIdentifierType(formatted)
      }];
    } else {
      // RDA Common Standard requires an id so generate one
      contrib.contributor_id = [{
        identifier: `${internalIdBase(applicationName, projectId, planId)}.members.${member.id}`,
        type: 'other'
      }]
    }

    // Add the affiliation to the Contributor if it exists
    if (member.name && member.uri) {
      contrib.affiliation = [{
        name: member.name,
        affiliation_id: {
          identifier: member.uri,
          type: determineIdentifierType(member.uri),
        }
      }];
    }

    return contrib;
  });
}

/**
 * Builds the DMP Tool extensions to the RDA Common Standard
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param applicationName the name of the application/service
 * @param domainName the domain name of the DMP Tool
 * @param plan the Plan information retrieve from the MySQL database
 * @param project the Project information retrieve from the MySQL database
 * @param funding the Funding information retrieve from the MySQL database
 * @returns the DMP metadata with extensions from the DMP Tool
 */
const buildDMPToolExtensions = async (
  rdsConnectionParams: ConnectionParams,
  applicationName: string,
  domainName: string,
  plan: LoadPlanInfo,
  project: LoadProjectInfo,
  funding: LoadFundingInfo | undefined,
): Promise<DMPToolExtensionType> => {
  const extensions: DMPToolExtensionType = {
    rda_schema_version: RDA_COMMON_STANDARD_VERSION,
    // Ignoring the `!` assertion here because we know we check the env variable
    // when the entrypoint function is called.
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    provenance: applicationName!,
    featured: plan.featured ? 'yes' : 'no',
    privacy: plan.visibility?.toLowerCase() ?? 'private',
    status: plan.status?.toLowerCase() ?? 'draft',
  }

  // Generate the DMP Narrative
  const narrative = await loadNarrativeTemplateInfo(
    rdsConnectionParams,
    plan.id
  );

  // Fetch the research domain if one was specified
  const research_domain = project.dmptool_research_domain
    ? { name: project.dmptool_research_domain }
    : undefined;

  let funderProject: DMPExtensionFunderProjectType | undefined = undefined;
  let funderOpportunity: DMPExtensionFunderOpportunityType | undefined = undefined;

  if (funding) {
    const projectId: RDACommonStandardIdentifierType = {
      identifier: internalIdBase(applicationName, project.id, plan.id),
      type: StandardIdentifierType.OTHER
    };

    const funderId: RDACommonStandardIdentifierType = funding.uri === undefined
      ? {
        identifier: `${internalIdBase(applicationName, project.id, plan.id)}.fundings.${funding.id}`,
        type: StandardIdentifierType.OTHER
      }
      : {
        identifier: funding.uri,
        type: funding.uri.match(ROR_REGEX)
          ? StandardIdentifierType.ROR
          : StandardIdentifierType.URL
      };

    // Define the Funder's project number if applicable. project_id and funder_id
    // are used to help tie the project_identifier to the correct
    // project[?].funding[?] in the RDA Common Standard.
    if (funding.funderProjectNumber !== undefined) {
      funderProject = {
        project_id: projectId,
        funder_id: funderId,
        project_identifier: {
          identifier: funding.funderProjectNumber,
          type: funding.funderProjectNumber?.startsWith('http')
            ? StandardIdentifierType.URL
            : StandardIdentifierType.OTHER
        }
      };
    }

    // Define the Funder's opportunity number if applicable. project_id and
    // funder_id are used to help tie the opportunity_identifier to the correct
    // project[?].funding[?] in the RDA Common Standard.
    if (funding.funderOpportunityNumber !== undefined) {
      funderOpportunity = {
        project_id: projectId,
        funder_id: funderId,
        opportunity_identifier: {
          identifier: funding.funderOpportunityNumber,
          type: funding.funderOpportunityNumber?.startsWith('http')
            ? StandardIdentifierType.URL
            : StandardIdentifierType.OTHER
        }
      };
    }
  }

  // Only add these properties if they have values we don't want 'undefined' or
  // 'null' in the JSON
  const regDate: string | null = convertMySQLDateTimeToRFC3339(plan.registered);
  if (!isNullOrUndefined(plan.registered) && regDate !== null) {
    extensions.registered = regDate;
  }
  if (!isNullOrUndefined(project.dmptool_research_domain) && research_domain !== undefined) {
    extensions.research_domain = research_domain;
  }
  if (funderProject !== undefined) {
    extensions.funding_project = [funderProject];
  }
  if (funderOpportunity !== undefined) {
    extensions.funding_opportunity = [funderOpportunity];
  }

  if (!isNullOrUndefined(narrative)) {
    extensions.narrative = {
      download_url: `https://${domainName}/dmps/${plan.dmpId}/narrative`,
      template: narrative
    };
  }

  return extensions;
}

/**
 * Builds the Project and Funding info for the RDA Common Standard
 *
 * @param applicationName the name of the application/service
 * @param planId the Plan ID
 * @param project the Project information retrieve from the MySQL database
 * @param funding the Funding information retrieve from the MySQL database
 * @returns the Project and Funding info for the RDA Common Standard
 */
const buildProject = (
  applicationName: string,
  planId: number,
  project: LoadProjectInfo,
  funding: LoadFundingInfo | undefined
): RDACommonStandardProject | undefined => {
  if (isNullOrUndefined(project)) {
    return undefined;
  }

  let fundingObject = undefined;

  if (funding && funding.name && funding.status) {
    const grantIdObject = funding.grantId
      ? {
        identifier: funding.grantId,
        type: funding.grantId?.startsWith('http') ? 'url' : 'other'
      }
      : undefined;

    // RDA Common Standard requires the funder id to be present
    const funderIdObject = funding.uri
      ? {
        identifier: funding.uri,
        type: (funding).uri?.match(ROR_REGEX) ? 'ror' : 'url'
      }
      : {
        identifier: `${internalIdBase(applicationName, project.id, planId)}.fundings.${funding.id}`,
        type: 'other'
      };

    // The RDA Common Standard requires the funder name and status to be present
    fundingObject = !isNullOrUndefined(funding)
      ? [{
        name: (funding).name,
        funding_status: planFundingStatusToDMPFundingStatus((funding).status),
        grant_id: grantIdObject,
        funder_id: funderIdObject
      }]
      : undefined;

    if (grantIdObject !== undefined && fundingObject !== undefined) {
      fundingObject[0].grant_id = grantIdObject;
    }
  }

  return {
    title: project.title,
    description: project.abstractText ?? undefined,
    start: project.startDate ?? undefined,
    end: project.endDate ?? undefined,
    project_id: [{
      identifier: internalIdBase(applicationName, project.id, planId),
      type: 'other'
    }],
    funding: fundingObject
  }
}

/**
 * Extracts the specified column from the columns of a ResearchOutputTable answer row.
 * @param id the ID of the column to extract
 * @param columns the columns of the answer row
 * @returns the specified column if it exists, otherwise undefined
 */
const findColumnById = (
  id: string,
  columns: AnyResearchOutputTableColumnAnswerType[]
): AnyResearchOutputTableColumnAnswerType | undefined => {
  return columns.find((col: AnyResearchOutputTableColumnAnswerType) => {
    return col?.commonStandardId === id
  });
}

/**
 * Converts the size + context into a byte size.
 *
 * @param size the NumberWithContextAnswerType
 * (e.g. `{ answer: { context: 'GB', value: 5 } }`) to convert
 * @returns the byte size if it could be converted, otherwise undefined
 */
const byteSizeToBytes = (size: NumberWithContextAnswerType): number | undefined => {
  if (isNullOrUndefined(size) || isNullOrUndefined(size.answer.value)) {
    return undefined;
  }

  const multipliers: Record<string, number> = {
    'TB': 1e12,
    'GB': 1e9,
    'MB': 1e6,
    'KB': 1e3,
  };

  const context = size.answer.context.toUpperCase();
  // If the context has a match in our multipliers, use it, otherwise use 1 as a fallback
  const multiplier = multipliers[context] ?? 1;

  return size.answer.value * multiplier;
}

/**
 * Convert a @dmptool/types researchOutputTable answer row into an RDA Common
 * Standard Dataset object.
 *
 * @param applicationName the name of the application/service
 * @param rowIdx the index of the answer row
 * @param row the answer row
 * @param projectId the ID of the project that the dataset belongs to
 * @param planId the ID of the plan that the dataset belongs to
 * @param language the language of the dataset (defaults to 'eng')
 * @returns a RDA Common Standard Dataset object
 */
const buildDataset = (
  applicationName: string,
  rowIdx: number,
  row: ResearchOutputTableRowAnswerType,
  projectId: number,
  planId: number,
  language = 'eng'
): RDACommonStandardDataset => {
  const title = findColumnById('title', row.columns) as TextAnswerType;
  const desc = findColumnById('description', row.columns) as TextAreaAnswerType;
  const typ = findColumnById('type', row.columns) as SelectBoxAnswerType;
  const flags = findColumnById('data_flags', row.columns) as CheckboxesAnswerType;
  const access_date = findColumnById('issued', row.columns) as DateAnswerType;
  const access = findColumnById('data_access', row.columns) as RadioButtonsAnswerType;
  const byte_size = findColumnById('byte_size', row.columns) as NumberWithContextAnswerType;
  const host = findColumnById('host', row.columns) as RepositorySearchAnswerType;
  const meta = findColumnById('metadata', row.columns) as MetadataStandardSearchAnswerType;
  const license = findColumnById('license_ref', row.columns) as LicenseSearchAnswerType;

  // The large RDA Common Standard Research Output representation.
  // Any properties that are commented out are ones that we do not currently support.

  // Build the Metadata object from the Metadata Standards defined on the Research Output
  const metadata = isNullOrUndefined(meta)
    ? undefined
    : meta.answer.map((m) => {
      return {
        description: m.metadataStandardName,
        // RDA Common Standard requires the language, but we can't get it from
        // the metadata standard repository we use, so just default it
        language: 'eng',
        metadata_standard_id: [{
          identifier: m.metadataStandardId,
          type: determineIdentifierType(m.metadataStandardId)
        }]
      }
    });

  // Get the Anticipated Release Date for the dataset
  const issued = isNullOrUndefined(access_date) ? undefined : access_date.answer

  // Build the License object from the Licenses defined on the Research Output
  const licenses = isNullOrUndefined(license)
    ? undefined
    : license.answer.map((l) => {
      return {
        license_ref: l.licenseId,
        start_date: issued
      }
    });

  // Build the Distribution object from the Repositories defined on the Research Output
  const distribution = isNullOrUndefined(host)
    ? undefined
    : host.answer.map((h) => {
      return {
        title: isNullOrUndefined(title) ? `Dataset ${rowIdx + 1}` : title.answer,
        // description: 'This is a test distribution',
        // access_url: 'https://example.com/dataset/123/distribution/123456789',
        // download_url: 'https://example.com/dataset/123/distribution/123456789/download',
        byte_size: isNullOrUndefined(byte_size) ? undefined : byteSizeToBytes(byte_size),
        // format: ['application/zip'],
        data_access: isNullOrUndefined(access) ? 'restricted' : access.answer,
        issued,
        license: licenses,
        host: {
          title: h.repositoryName,
          // description: 'This is a test host',
          url: h.repositoryId,
          host_id: [{
            identifier: h.repositoryId,
            type: determineIdentifierType(h.repositoryId)
          }],
          // availability: '99.99',
          // backup_frequency: 'weekly',
          // backup_type: 'tapes',
          // certified_with: 'coretrustseal',
          // geo_location: 'US',
          // pid_system: ['doi', 'ark'],
          // storage_type: 'LTO-8 tape',
          // support_versioning: 'yes'
        }
      }
    });

  return {
    title: isNullOrUndefined(title) ? `Dataset ${rowIdx + 1}` : title.answer,
    type: isNullOrUndefined(typ) ? 'dataset' : typ.answer,
    description: isNullOrUndefined(desc) ? undefined : desc.answer,
    dataset_id: {
      identifier: `${internalIdBase(applicationName, projectId, planId)}.outputs.${rowIdx + 1}`,
      type: 'other'
    },
    personal_data: isNullOrUndefined(flags) ?
      'unknown'
      : (flags.answer.includes('personal') ? 'yes' : 'no'),
    sensitive_data: isNullOrUndefined(flags)
      ? 'unknown'
      : (flags.answer.includes('sensitive') ? 'yes' : 'no'),
    issued,
    language,
    metadata,
    distribution,
    // data_quality_assurance: [''],
    // is_reused: false,
    // keyword: ['test', 'dataset'],
    // preservation_statement: 'Statement about preservation',
    // security_and_privacy: [{
    //   title: 'Security and Privacy Statement',
    //   description: 'Description of security and privacy statement'
    // }],
    // alternate_identifier: [{
    //   identifier: 'https://example.com/dataset/123',
    //   type: 'url'
    // }],
    // technical_resource: [{
    //   name: 'Test Server',
    //   description: 'This is a test server',
    //   technical_resource_id: [{
    //     identifier: 'https://example.com/server/123',
    //     type: 'url'
    //  }],
    // }],
  }
}

/**
 * Validate the specified DMP metadata record against the RDA Common Standard
 * and DMP Tool extensions schema
 *
 * @param logger the logger to use for logging
 * @param dmp The DMP metadata record to validate
 * @returns the DMP metadata record if it is valid
 * @throws DMPValidationError if the record is invalid with the error message(s)
 */
export const validateRDACommonStandard = (
  logger: Logger,
  dmp: DMPToolDMPType
): DMPToolDMPType => {
  const validationErrors: string[] = [];
  const validator = new Validator();

  // Validate against the RDA Common Standard schema
  const rdaResult = validator.validate(dmp, RDACommonStandardDMPJSONSchema);
  if (rdaResult && rdaResult.errors.length > 0) {
    validationErrors.push(
      ...rdaResult.errors.map(e => `${e.path.join('.')} - ${e.message}`)
    )
  }

  if (validationErrors.length > 0) {
    const msg = `Invalid RDA Common Standard: ${validationErrors.join('; ')}`;
    logger.warn({ dmpId: dmp?.dmp?.dmp_id?.identifier }, msg);
    logger.warn({ dmp: dmp?.dmp }, 'Full DMP');
    throw new DMPValidationError(msg);
  }

  return dmp;
}

/**
 * Validate the specified DMP metadata record against the RDA Common Standard
 * and DMP Tool extensions schema
 *
 * @param logger the logger to use for logging
 * @param dmp The DMP metadata record to validate
 * @returns the DMP metadata record if it is valid
 * @throws DMPValidationError if the record is invalid with the error message(s)
 */
export const validateDMPToolExtensions = (
  logger: Logger,
  dmpId: string,
  dmp: DMPToolExtensionType
): DMPToolExtensionType => {
  const validationErrors: string[] = [];

  // Next validate against the DMP Tool extension schema
  const extResult = DMPToolExtensionSchema.safeParse(dmp);
  if (extResult && !extResult.success && extResult.error.issues?.length > 0) {
    validationErrors.push(
      ...extResult.error.issues.map(e => `${e.path.join('.')} - ${e.message}`)
    );
  }

  if (validationErrors.length > 0) {
    const msg = `Invalid DMP Tool extensions: ${validationErrors.join('; ')}`;
    logger.warn({ dmpId }, msg);
    logger.warn({ dmp }, 'Full DMP Tool extensions');
    throw new DMPValidationError(msg);
  }

  return dmp;
}

/**
 * Clean the RDA Common Standard portion of the DMP metadata record
 *
 * @param plan The plan information
 * @param dmp
 * @returns The cleaned DMP metadata record
 */
const cleanRDACommonStandard = (
  plan: LoadPlanInfo,
  dmp: DMPToolDMPType
): DMPToolDMPType => {
  // Make sure some of the required properties have a default value
  if (!dmp.dmp.created) dmp.dmp.created = plan.created;
  if (!dmp.dmp.modified) dmp.dmp.modified = plan.modified;
  if (!dmp.dmp.language) dmp.dmp.language = 'eng';
  if (!dmp.dmp.ethical_issues_exist) dmp.dmp.ethical_issues_exist = 'unknown';

  return dmp;
}

/**
 *
 * Generate a JSON representation for a DMP that confirms to the RDA Common Metadata
 * standard and includes DMP Tool specific extensions to that standard.
 *
 * Some things of note about the JSON representation:
 *   - There are no primitive booleans, booleans are represented as: 'yes',
 *   'no', 'unknown'
 *   - There are no primitive dates, strings formatted as 'YYYY-MM-DD hh:mm:ss:msZ'
 *   are used
 *   - The `provenance` is used to store the ID of the system that created the DMP
 *   - The `privacy` should be used to determine authorization for viewing the
 *   DMP narrative
 *   - The `featured` indicates whether the DMP is featured on the DMP Tool
 *   website's public plans page
 *   - The `registered` indicates whether the DMP is published/registered with DataCite/EZID
 *   - The `tombstoned` indicates that it was published/registered but is now removed
 *
 * @param rdsConnectionParams the connection parameters for the RDS instance
 * @param applicationName the name of the application/service
 * @param domainName the domain name of the application/service website
 * @param planId the ID of the plan to generate the DMP for
 * @param env The environment from EnvironmentEnum (defaults to EnvironmentEnum.DEV)
 * @returns a JSON representation of the DMP
 */
export async function planToDMPCommonStandard(
  rdsConnectionParams: ConnectionParams,
  applicationName: string,
  domainName: string,
  env: EnvironmentEnum = EnvironmentEnum.DEV,
  planId: number
): Promise<DMPToolDMPType | undefined> {
  if (!rdsConnectionParams || !applicationName || !domainName || !planId) {
    throw new Error('Invalid arguments provided to planToDMPCommonStandard');
  }

  // Fetch the Plan data
  const plan: LoadPlanInfo | undefined = await loadPlanInfo(
    rdsConnectionParams,
    planId
  );
  if (plan === undefined) {
    rdsConnectionParams.logger.error({ planId, applicationName, env }, 'Plan not found');
    throw new DMPValidationError(`Plan not found: ${planId}`);
  }

  if (isNullOrUndefined(plan.title)) {
    rdsConnectionParams.logger.error({ planId, applicationName, env }, 'Plan title not found');
    throw new DMPValidationError(`Plan title not found for plan: ${planId}`);
  }

  if (isNullOrUndefined(plan.dmpId)) {
    rdsConnectionParams.logger.error({ planId, applicationName, env }, 'Plan dmpId not found');
    throw new DMPValidationError(`DMP ID not found for plan: ${planId}`);
  }

  // Get the Project data
  const project: LoadProjectInfo | undefined = await loadProjectInfo(
    rdsConnectionParams,
    plan.projectId
  );
  if (project === undefined || !project.title) {
    rdsConnectionParams.logger.error({ planId, applicationName, env }, 'Project not found');
    throw new DMPValidationError(`Project not found: ${plan.projectId}`);
  }

  // Get all the plan members and determine the primary contact
  const members: LoadMemberInfo[] = plan.id ? await loadMemberInfo(
    rdsConnectionParams,
    plan.id
  ) : [];
  const contact: RDACommonStandardContact = await buildContact(
    rdsConnectionParams,
    env,
    plan,
    members
  );
  if (!contact) {
    rdsConnectionParams.logger.error({ planId, applicationName, env }, 'Could not build primary contact');
    throw new DMPValidationError(
      `Could not establish a primary contact for plan: ${planId}`
    );
  }

  // Get all the funding and narrative info
  const datasets: RDACommonStandardDataset[] | [] = await loadDatasetInfo(
    rdsConnectionParams,
    applicationName,
    project.id,
    plan.id,
    plan.languageId
  );
  // We only allow one funding per plan at this time
  const fundings: LoadFundingInfo[] | [] = await loadFundingInfo(
    rdsConnectionParams,
    plan.id
  );
  const funding: LoadFundingInfo | undefined = fundings.length > 0 ? fundings[0] : undefined;
  const works: RDACommonStandardRelatedWork[] | [] = await loadRelatedWorksInfo(
    rdsConnectionParams,
    plan.projectId
  );
  const defaultRole: string | undefined = await loadDefaultMemberRole(rdsConnectionParams);

  // If the plan is registered, use the DOI as the identifier, otherwise convert to a URL
  const dmpId = plan.registered
    ? {
      identifier: plan.dmpId,
      type: 'doi'
    }
    : {
      identifier: `https://${domainName}/projects/${project.id}/dmp/${plan.id}`,
      type: 'url'
    };

  // Examine the datasets to determine the status of ethical issues
  // If the datasets array only contains the default/generic dataset (meaning there
  // were no research outputs defined) then the status is `unknown`
  const isDefaultDataset: boolean = datasets.length === 1 && datasets[0].title === 'Generic Dataset';
  const yesFlag: RDACommonStandardDataset | undefined = datasets.find((d: RDACommonStandardDataset) => {
    return d.sensitive_data === 'yes' || d.personal_data === 'yes';
  });
  const noFlag: RDACommonStandardDataset | undefined = datasets.find((d: RDACommonStandardDataset) => {
    return d.sensitive_data === 'no' || d.personal_data === 'no';
  });

  const unknownEthicalState = isDefaultDataset
    || (isNullOrUndefined(yesFlag) && isNullOrUndefined(noFlag));

  // If we just
  const hasEthicalIssues:string = unknownEthicalState
    ? 'unknown'
    : (isNullOrUndefined(yesFlag) ? 'no' : 'yes');

  // Generate the RDA Common Standard DMP metadata record
  const dmp: DMPToolDMPType = {
    dmp: {
      title: plan.title,
      ethical_issues_exist: hasEthicalIssues,
      language: convertFiveCharToThreeChar(plan.languageId),
      created: convertMySQLDateTimeToRFC3339(plan.created),
      modified: convertMySQLDateTimeToRFC3339(plan.modified),
      dmp_id: dmpId,
      contact: contact,
      dataset: datasets,
    },
  };
  const dmpProject: RDACommonStandardProject | undefined = buildProject(
    applicationName,
    plan.id,
    project,
    funding
  );
  const dmpContributor: RDACommonStandardContributor[] | [] = buildContributors(
    applicationName,
    env,
    plan.id,
    project.id,
    members,
    defaultRole ?? 'other'
  );

  // Add the contributor, project and related identifier properties if they have values
  if (!isNullOrUndefined(dmpProject)) {
    dmp.dmp.project = [removeNullAndUndefinedFromObject(dmpProject)];
  }
  if (!isNullOrUndefined(dmpContributor)
    && Array.isArray(dmpContributor)
    && dmpContributor.length > 0) {

    dmp.dmp.contributor = removeNullAndUndefinedFromObject(dmpContributor);
  }
  if (!isNullOrUndefined(works) && Array.isArray(works) && works.length > 0) {
    dmp.dmp.related_identifier = removeNullAndUndefinedFromObject(works);
  }
  const cleaned: DMPToolDMPType = cleanRDACommonStandard(plan, dmp);

  // Generate the DMP Tool extensions to the RDA Common Standard
  const extensions: DMPToolExtensionType = await buildDMPToolExtensions(
    rdsConnectionParams,
    applicationName,
    domainName,
    plan,
    project,
    funding
  );

  rdsConnectionParams.logger.debug(
    { applicationName, domainName, planId, env, dmpId: plan.dmpId },
    'Generated maDMP metadata record'
  );

  // Return the combined DMP metadata record
  return {
    dmp: {
      ...validateRDACommonStandard(rdsConnectionParams.logger, cleaned).dmp,
      ...validateDMPToolExtensions(rdsConnectionParams.logger, plan.dmpId, extensions),
    }
  };
}
