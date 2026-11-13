import { AnyQuestionType } from '@dmptool/types';
import { ConnectionParams, queryTable } from './rds';

/**
 * A question on a published (versioned) Template, without any answer.
 */
export interface TemplateNarrativeQuestion {
  // The versionedQuestions.id
  id: number;
  order: number;
  text: string;
  // The question type (e.g. textArea, checkBoxes, radioButtons) from the question JSON
  type?: string;
  // The question JSON (contains things like the options for option based questions)
  json?: AnyQuestionType;
  requirementText?: string;
  guidanceText?: string;
  sampleText?: string;
  useSampleTextAsDefault: boolean;
  required: boolean;
}

/**
 * A section on a published (versioned) Template.
 */
export interface TemplateNarrativeSection {
  // The versionedSections.id
  id: number;
  order: number;
  title: string;
  introduction?: string;
  requirements?: string;
  guidance?: string;
  question: TemplateNarrativeQuestion[];
}

/**
 * The affiliation that owns the Template (e.g. the funder)
 */
export interface TemplateNarrativeOwner {
  uri: string;
  name?: string;
  displayName?: string;
}

/**
 * A published (versioned) Template with all of its sections and questions.
 */
export interface TemplateNarrative {
  // The versionedTemplates.id
  id: number;
  // The versionedTemplates.templateId (the templates.id this is a version of)
  templateId: number;
  title: string;
  description?: string;
  version: string;
  // The versionedTemplates.visibility (PUBLIC or ORGANIZATION) at the time this version was published
  visibility: string;
  // DRAFT or PUBLISHED
  versionType: string;
  // Whether this is the current version of the Template
  active: boolean;
  isDefault: boolean;
  languageId?: string;
  modified?: string;
  owner: TemplateNarrativeOwner;
  section: TemplateNarrativeSection[];
}

// Fetches the Template, its owner and all of its sections and questions.
// Note that everything comes from the versioned tables (the published snapshot of the
// Template), not the `templates` table (which has `latestPublishVisibility` instead
// of `visibility`).
const SQL_TEMPLATE_NARRATIVE = `
  SELECT vt.id versionedTemplateId, vt.templateId, vt.name templateTitle,
         vt.description templateDescription, vt.version templateVersion, vt.visibility,
         vt.versionType, vt.active, vt.isDefault, vt.languageId, vt.modified,
         vt.ownerId ownerURI, a.name ownerName, a.displayName ownerDisplayName,
         vs.id sectionId, vs.name sectionTitle, vs.introduction sectionIntroduction,
         vs.requirements sectionRequirements, vs.guidance sectionGuidance,
         vs.displayOrder sectionOrder,
         vq.id questionId, vq.questionText, vq.json questionJSON, vq.requirementText,
         vq.guidanceText, vq.sampleText, vq.useSampleTextAsDefault, vq.required,
         vq.displayOrder questionOrder
  FROM versionedTemplates vt
    LEFT JOIN affiliations a ON a.uri = vt.ownerId
    LEFT JOIN versionedSections vs ON vs.versionedTemplateId = vt.id
      LEFT JOIN versionedQuestions vq ON vq.versionedSectionId = vs.id
  WHERE vt.id = ?
  ORDER BY vs.displayOrder, vq.displayOrder;
`;

/**
 * Convert a MySQL TINYINT (or boolean/string) value into a boolean
 */
const toBoolean = (val: unknown): boolean => {
  return val === true || val === 1 || val === '1' || val === 'true';
};

/**
 * Return undefined for text that is empty once HTML tags, non-breaking spaces and
 * whitespace are removed (e.g. "<p></p>" or "<p>&nbsp;</p>"). Otherwise, return the
 * trimmed text.
 */
const blankToUndefined = (val: unknown): string | undefined => {
  if (val === null || val === undefined) return undefined;

  const text = String(val).trim();
  const stripped = text.replace(/<[^>]*>/g, '').replace(/&nbsp;/gi, '').trim();
  return stripped.length > 0 ? text : undefined;
};

/**
 * mysql2 returns JSON columns as objects, but some drivers return them as strings.
 */
const parseQuestionJSON = (val: unknown): AnyQuestionType | undefined => {
  if (val === null || val === undefined) return undefined;
  if (typeof val === 'object') return val as AnyQuestionType;

  try {
    return JSON.parse(String(val)) as AnyQuestionType;
  } catch {
    return undefined;
  }
};

/**
 * Builds the TemplateNarrative structure from SQL_TEMPLATE_NARRATIVE rows.
 */
const buildTemplateNarrative = (rows: any[]): TemplateNarrative => {
  const first = rows[0];
  const narrative: TemplateNarrative = {
    id: first.versionedTemplateId,
    templateId: first.templateId,
    title: first.templateTitle,
    description: blankToUndefined(first.templateDescription),
    version: first.templateVersion,
    visibility: first.visibility,
    versionType: first.versionType,
    active: toBoolean(first.active),
    isDefault: toBoolean(first.isDefault),
    languageId: first.languageId ?? undefined,
    modified: first.modified ?? undefined,
    owner: {
      uri: first.ownerURI,
      name: first.ownerName ?? undefined,
      displayName: first.ownerDisplayName ?? undefined,
    },
    section: [],
  };

  const sectionMap = new Map<number, TemplateNarrativeSection>();
  for (const row of rows) {
    if (row.sectionId === null || row.sectionId === undefined) continue;

    let section = sectionMap.get(row.sectionId);
    if (!section) {
      section = {
        id: row.sectionId,
        order: row.sectionOrder ?? 0,
        title: row.sectionTitle,
        introduction: blankToUndefined(row.sectionIntroduction),
        requirements: blankToUndefined(row.sectionRequirements),
        guidance: blankToUndefined(row.sectionGuidance),
        question: [],
      };
      sectionMap.set(row.sectionId, section);
      narrative.section.push(section);
    }

    if (row.questionId !== null && row.questionId !== undefined) {
      const json = parseQuestionJSON(row.questionJSON);
      section.question.push({
        id: row.questionId,
        order: row.questionOrder ?? 0,
        text: row.questionText,
        type: json?.type,
        json,
        requirementText: blankToUndefined(row.requirementText),
        guidanceText: blankToUndefined(row.guidanceText),
        sampleText: blankToUndefined(row.sampleText),
        useSampleTextAsDefault: toBoolean(row.useSampleTextAsDefault),
        required: toBoolean(row.required),
      });
    }
  }
  return narrative;
};

/**
 * Loads a versioned Template along with all of its sections and questions. This is
 * the blank Template (no Plan or answers) and it does not include any customizations.
 *
 * The caller is responsible for deciding whether the Template can be shown to the
 * current user (e.g. check `active`, `versionType` and `visibility`).
 *
 * @param rdsConnectionParams the connection parameters for the MySQL database
 * @param versionedTemplateId the versionedTemplates.id to load
 * @returns the Template narrative, or undefined if the Template does not exist
 */
export const loadTemplateNarrative = async (
  rdsConnectionParams: ConnectionParams,
  versionedTemplateId: number
): Promise<TemplateNarrative | undefined> => {
  rdsConnectionParams.logger.debug({ versionedTemplateId }, 'Fetching template narrative');

  const rows = await queryTable(
    rdsConnectionParams,
    SQL_TEMPLATE_NARRATIVE,
    [versionedTemplateId.toString()]
  );
  if (!Array.isArray(rows?.results) || rows.results.length === 0) return undefined;

  return buildTemplateNarrative(rows.results);
};
