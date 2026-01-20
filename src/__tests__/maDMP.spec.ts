


import { planToDMPCommonStandard } from '../maDMP';
import {
  DMPToolDMPType,
  ResearchOutputTableAnswerType
} from "@dmptool/types";
import {
  LoadFundingInfo,
  LoadMemberInfo,
  LoadNarrativeInfo,
  LoadPlanInfo,
  LoadProjectInfo,
  LoadRelatedWorkInfo
} from "../maDMPTypes";

// Mock external dependencies
jest.mock('../rds');

import pino, { Logger } from 'pino';
import { queryTable } from "../rds";
import { convertMySQLDateTimeToRFC3339, EnvironmentEnum } from "../general";

const mockLogger: Logger = pino({ level: 'silent' });

const mockConfig = {
  logger: mockLogger,
  host: 'localhost',
  port: 3306,
  user: 'test',
  password: 'test',
  database: 'testdb'
}
const mockEnv = EnvironmentEnum.DEV;
const mockApplication = 'test-app';
const mockDomain = 'example.com';

describe('planToDMPCommonStandard', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const defaultMemberRole = { id: 'tester' };

  const mockRegisteredPlanInfo: LoadPlanInfo = {
    id: 123,
    dmpId: `https://doi.org/11.22222/12345`,
    projectId: 12,
    versionedTemplateId: 1,
    createdById: 4,
    created: '2025-12-31 10:52:00',
    modifiedById: 5,
    modified: '2026-01-08 10:52:00',
    title: 'Test DMP',
    status: 'DRAFT',
    visibility: 'PUBLIC',
    featured: false,
    registeredBy: 5,
    registered: '2026-01-08 10:52:00',
    languageId: 'pt-BR'
  }

  const mockUnregisteredPlanInfo: LoadPlanInfo = {
    id: 123,
    dmpId: `https://doi.org/11.22222/67890`,
    projectId: 12,
    versionedTemplateId: 1,
    createdById: 4,
    created: '2025-12-31 10:52:00',
    modifiedById: 5,
    modified: '2026-01-08 10:52:00',
    title: 'Test DMP',
    status: 'COMPLETE',
    visibility: 'PUBLIC',
    featured: false,
    languageId: 'pt-BR'
  }

  const mockProjectMinimumInfo: LoadProjectInfo = {
    id: 12,
    title: 'Test Research Project',
  }

  const mockProjectCompleteInfo: LoadProjectInfo = {
    id: 12,
    title: 'Test Research Project',
    abstractText: 'This is a test research project',
    startDate: '2025-01-01',
    endDate: '2028-01-31',
    dmptool_research_domain: 'biology'
  }

  const mockMembersWithoutPrimaryContact: LoadMemberInfo[] = [
    {
      id: 1,
      uri: 'https://ror.org/000000000',
      name: 'Example University',
      email: 'pi@example.com',
      givenName: 'Test',
      surName: 'PI',
      orcid: 'https://orcid.org/0000-0000-0000-000X',
      isPrimaryContact: false,
      roles: '["pi","data_curation"]'
    },
  ]

  const mockPlanOwner: LoadMemberInfo = {
    id: 875,
    uri: 'https://ror.org/000000000',
    name: 'Example University',
    email: 'owner@example.com',
    givenName: 'Example',
    surName: 'Owner',
    orcid: 'https://orcid.org/0000-0000-0000-0000',
    isPrimaryContact: true,
    roles: '["other"]'
  };

  const mockMembersWithPrimaryContact: LoadMemberInfo[] = [
    {
      id: 2,
      uri: 'https://ror.org/000000000',
      name: 'Example University',
      email: 'contact@example.com',
      givenName: 'Example',
      surName: 'Contact',
      isPrimaryContact: true,
      roles: '["other"]'
    },
    {
      id: 1,
      uri: 'https://ror.org/000000000',
      name: 'Example University',
      givenName: 'Test',
      surName: 'PI',
      orcid: 'https://orcid.org/0000-0000-0000-000X',
      isPrimaryContact: false,
      roles: '["pi","data_curation"]'
    },
  ]

  const mockMinimalPlanFunding: LoadFundingInfo = {
    id: 1,
    name: 'Federal Agency',
    status: 'PLANNED',
  }

  const mockCompletePlanFunding: LoadFundingInfo = {
    id: 2,
    uri: 'https://ror.org/000000009',
    name: 'Federal Agency',
    status: 'PLANNED',
    grantId: 'https://example.com/grant/123456789',
    funderProjectNumber: 'PRJ-EU-3575674574567556',
    funderOpportunityNumber: 'DEPTA-GJ3245TH-564T'
  }

  const mockRelatedWorks: LoadRelatedWorkInfo[] = [
    { identifier: 'https://example.org/works/29485674952', workType: 'dataset' },
    { identifier: 'https://example.org/works/07070877856', workType: 'SOFTWARE' },
  ]

  const mockResearchOutputTableAnswer: ResearchOutputTableAnswerType = {
    answer: [
      {
        columns: [
          {
            type: 'text',
            commonStandardId: 'title',
            answer: 'My dataset',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'textArea',
            commonStandardId: 'description',
            answer: 'A description of the dataset',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'selectBox',
            commonStandardId: 'type',
            answer: 'dataset',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'checkBoxes',
            commonStandardId: 'data_flags',
            answer: ['sensitive'],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'radioButtons',
            commonStandardId: 'data_access',
            answer: 'open',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'date',
            commonStandardId: 'issued',
            answer: '2028-01-01',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'numberWithContext',
            commonStandardId: 'byte_size',
            answer: { value: 123, context: 'MB' },
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'repositorySearch',
            commonStandardId: 'host',
            answer: [
              {
                repositoryId: 'https://example.org/repositories/123456789',
                repositoryName: 'Example Repository'
              },
              {
                repositoryId: 'https://example.org/repositories/987654321',
                repositoryName: 'Example Repository 2'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'metadataStandardSearch',
            commonStandardId: 'metadata',
            answer: [
              {
                metadataStandardId: 'https://example.org/standards/123456789',
                metadataStandardName: 'Example Standard'
              },
              {
                metadataStandardId: 'https://example.org/standards/987654321',
                metadataStandardName: 'Example Standard 2'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'licenseSearch',
            commonStandardId: 'license_ref',
            answer: [
              {
                licenseId: 'https://example.org/licenses/123456789',
                licenseName: 'Example License'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          }
        ],
      },
      {
        columns: [
          {
            type: 'text',
            commonStandardId: 'title',
            answer: 'My software',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'textArea',
            commonStandardId: 'description',
            answer: 'Software to process the dataset',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'selectBox',
            commonStandardId: 'type',
            answer: 'software',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'checkBoxes',
            commonStandardId: 'data_flags',
            answer: [],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'radioButtons',
            commonStandardId: 'data_access',
            answer: 'open',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'date',
            commonStandardId: 'issued',
            answer: '2028-03-01',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'numberWithContext',
            commonStandardId: 'byte_size',
            answer: { value: 123, context: 'KB' },
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'repositorySearch',
            commonStandardId: 'host',
            answer: [
              {
                repositoryId: 'https://example.org/repositories/35353545',
                repositoryName: 'Example Repository'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'metadataStandardSearch',
            commonStandardId: 'metadata',
            answer: [
              {
                metadataStandardId: 'https://example.org/standards/35353545',
                metadataStandardName: 'Example Standard'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'licenseSearch',
            commonStandardId: 'license_ref',
            answer: [
              {
                licenseId: 'https://example.org/licenses/35353545',
                licenseName: 'Example License'
              }
            ],
            meta: { schemaVersion: 'v1.0' }
          }
        ],
      }
    ],
    columnHeadings: [
      "Title",
      "Description",
      "Type",
      "Data Flags",
      "Access Level",
      "Anticipated Release Date",
      "Byte Size",
      "Repository(ies)",
      "Metadata Standard(s)",
      "License"
    ],
    meta: { schemaVersion:"1.0" },
    type: "researchOutputTable"
  };

  const mockMinimalResearchOutputs = {
    json: JSON.stringify({
      answer: [{
        columns: [
          {
            type: 'text',
            commonStandardId: 'title',
            answer: 'My software',
            meta: { schemaVersion: 'v1.0' }
          },
          {
            type: 'selectBox',
            commonStandardId: 'type',
            answer: 'software',
            meta: { schemaVersion: 'v1.0' }
          }
        ],
      }],
      columnHeadings: ["Title", "Type"],
      meta: { schemaVersion: "1.0" },
      type: "researchOutputTable"
    })
  }

  const mockResearchOutputs = {
    json: JSON.stringify(mockResearchOutputTableAnswer)
  }

  const mockNarrativeResults = [
    {
      templateId: 999,
      templateTitle: 'Example DMP Tool Template',
      templateDescription: 'This template is for testing only!',
      templateVersion: 'v12',
      sectionId: 1,
      sectionTitle: 'First section',
      sectionDescription: '<p>The first section of this template<p>',
      sectionOrder: 1,
      questionId: 1,
      questionText: 'What is the secret password?',
      questionOrder: 1,
      answerId: 1,
      answerJSON: {
        type: "text",
        answer: "open sesame",
        meta: { schemaVersion: "1.0" }
      }
    },
    {
      templateId: 999,
      templateTitle: 'Example DMP Tool Template',
      templateDescription: 'This template is for testing only!',
      templateVersion: 'v12',
      sectionId: 1,
      sectionTitle: 'First section',
      sectionDescription: '<p>The first section of this template<p>',
      sectionOrder: 1,
      questionId: 2,
      questionText: 'What is your favorite color?',
      questionOrder: 2,
      answerId: 2,
      answerJSON: {
        type: "selectBox",
        answer: "blue",
        meta: { schemaVersion: "1.0" }
      }
    },
    {
      templateId: 999,
      templateTitle: 'Example DMP Tool Template',
      templateDescription: 'This template is for testing only!',
      templateVersion: 'v12',
      sectionId: 2,
      sectionTitle: 'Second section',
      sectionDescription: '<p>The second section of this template<p>',
      sectionOrder: 1,
      questionId: 3,
      questionText: 'Do you agree to the terms and conditions?',
      questionOrder: 1,
      answerId: 3,
      answerJSON: {
        type: "checkBoxes",
        answer: ["yes", "maybe"],
        meta: { schemaVersion: "1.0" }
      }
    },
    {
      templateId: 999,
      templateTitle: 'Example DMP Tool Template',
      templateDescription: 'This template is for testing only!',
      templateVersion: 'v12',
      sectionId: 2,
      sectionTitle: 'Second section',
      sectionDescription: '<p>The second section of this template<p>',
      sectionOrder: 1,
      questionId: 4,
      questionText: 'What outputs will your project produce?',
      questionOrder: 2,
      answerId: 4,
      answerJSON: mockResearchOutputTableAnswer
    }
  ]

  const mockCompleteNarrative: LoadNarrativeInfo = {
    id: 999,
    title: 'Example DMP Tool Template',
    description: 'This template is for testing only!',
    version: 'v12',
    section: [
      {
        id: 1,
        title: 'First section',
        description: '<p>The first section of this template<p>',
        order: 1,
        question: [
          {
            id: 1,
            order: 1,
            text: 'What is the secret password?',
            answer: {
              id: 1,
              json: {
                answer: "open sesame",
                meta: { schemaVersion: "1.0" },
                type: "text"
              }
            }
          },
          {
            id: 2,
            order: 2,
            text: 'What is your favorite color?',
            answer: {
              id: 2,
              json: {
                answer: "blue",
                meta: { schemaVersion: "1.0" },
                type: "selectBox"
              }
            }
          }
        ]
      },
      {
        id: 2,
        title: 'Second section',
        description: '<p>The second section of this template<p>',
        order: 1,
        question: [
          {
            id: 3,
            order: 1,
            text: 'Do you agree to the terms and conditions?',
            answer: {
              id: 3,
              json: {
                answer: ["yes", "maybe"],
                meta: { schemaVersion: "1.0" },
                type: "checkBoxes"
              }
            }
          },
          {
            id: 4,
            order: 2,
            text: 'What outputs will your project produce?',
            answer: {
              id: 4,
              json: mockResearchOutputTableAnswer
            }
          }
        ]
      }
    ]
  }

  describe('planToCommonStandardJSON', () => {
    it('registered plans use the correct DMP id format', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({ results: [mockRegisteredPlanInfo] })
        .mockResolvedValueOnce({ results: [mockProjectMinimumInfo] })
        .mockResolvedValueOnce({ results: [] })  // No Plan members
        .mockResolvedValueOnce({ results: [mockPlanOwner] })  // Will use the plan owner
        .mockResolvedValueOnce({ results: [] })  // No Research Outputs
        .mockResolvedValueOnce({ results: [] })  // No Funding Info
        .mockResolvedValueOnce({ results: [] })  // No Related Works Info
        .mockResolvedValueOnce({ results: [defaultMemberRole] });

      const expected: DMPToolDMPType = {
        dmp: {
          title: mockRegisteredPlanInfo.title,
          dmp_id: {
            identifier: mockRegisteredPlanInfo.dmpId,
            type: 'doi'
          },
          created: convertMySQLDateTimeToRFC3339(mockRegisteredPlanInfo.created),
          modified: convertMySQLDateTimeToRFC3339(mockRegisteredPlanInfo.modified),
          ethical_issues_exist: 'unknown',
          language: 'por',
          contact: {
            name: [mockPlanOwner.givenName, mockPlanOwner.surName].join(' '),
            mbox: mockPlanOwner.email,
            contact_id: [{
              identifier: mockPlanOwner.orcid,
              type: 'orcid'
            }],
            affiliation: [{
              name: mockPlanOwner.name,
              affiliation_id: {
                identifier: mockPlanOwner.uri,
                type: 'ror'
              }
            }]
          },
          project: [{
            title: mockProjectMinimumInfo.title,
            project_id: [{
              identifier: `${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockRegisteredPlanInfo.id}`,
              type: 'other'
            }]
          }],
          dataset: [{
            dataset_id: {
              identifier: `${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockRegisteredPlanInfo.id}.outputs.1`,
              type: 'other'
            },
            personal_data: 'unknown',
            sensitive_data: 'unknown',
            title: 'Generic Dataset',
            type: 'dataset'
          }],
          rda_schema_version: "1.2",
          provenance: mockApplication,
          privacy: mockRegisteredPlanInfo.visibility.toLowerCase(),
          status: mockRegisteredPlanInfo.status.toLowerCase(),
          featured: mockRegisteredPlanInfo.featured ? 'yes' : 'no',
          registered: convertMySQLDateTimeToRFC3339(mockRegisteredPlanInfo.registered),
        }
      };

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );
      expect(result).toEqual(expected);
    });

    it('unregistered plans have correct DMP ID structure and no registered date', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result: DMPToolDMPType | undefined = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );
      expect(result).toBeDefined();
      expect(result?.dmp?.dmp_id).toEqual({
        identifier: `https://${mockDomain}/projects/${mockProjectMinimumInfo.id}/dmp/${mockUnregisteredPlanInfo.id}`,
        type: 'url'
      });
      expect(result?.dmp?.registered).toBeUndefined();
    });

    it('includes narrative in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]})
        .mockResolvedValueOnce({results: mockNarrativeResults});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );
      expect(result).toBeDefined();
      expect(result?.dmp?.narrative).toBeDefined();

      // Verify the Template details
      expect(result?.dmp?.narrative?.template?.title).toEqual(mockCompleteNarrative.title);
      expect(result?.dmp?.narrative?.template?.description).toEqual(mockCompleteNarrative.description);
      expect(result?.dmp?.narrative?.template?.version).toEqual(mockCompleteNarrative.version);
      expect(result?.dmp?.narrative?.template?.section).toHaveLength(2);

      // Verify the first section
      expect(result?.dmp?.narrative?.template?.section[0].id).toEqual(mockCompleteNarrative.section[0].id);
      expect(result?.dmp?.narrative?.template?.section[0].title).toEqual(mockCompleteNarrative.section[0].title);
      expect(result?.dmp?.narrative?.template?.section[0].description).toEqual(mockCompleteNarrative.section[0].description);
      expect(result?.dmp?.narrative?.template?.section[0].order).toEqual(mockCompleteNarrative.section[0].order);
      expect(result?.dmp?.narrative?.template?.section[0].question).toHaveLength(2);

      // Verify the first question
      expect(result?.dmp?.narrative?.template?.section[0].question[0].id).toEqual(mockCompleteNarrative.section[0].question[0].id);
      expect(result?.dmp?.narrative?.template?.section[0].question[0].order).toEqual(mockCompleteNarrative.section[0].question[0].order);
      expect(result?.dmp?.narrative?.template?.section[0].question[0].text).toEqual(mockCompleteNarrative.section[0].question[0].text);
      expect(result?.dmp?.narrative?.template?.section[0].question[0].answer.id).toEqual(mockCompleteNarrative.section[0].question[0].answer.id);
      expect(result?.dmp?.narrative?.template?.section[0].question[0].answer.json).toEqual(mockCompleteNarrative.section[0].question[0].answer.json);
    });

    it('includes members in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: mockMembersWithPrimaryContact})
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      expect(result?.dmp?.contributor).toBeDefined();
      expect(result?.dmp?.contributor).toHaveLength(2);
    });

    it('includes complete project information in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectCompleteInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      expect(result?.dmp.project).toBeDefined();
      expect(result?.dmp.project?.[0].title).toEqual(mockProjectCompleteInfo.title);
      expect(result?.dmp.project?.[0].description).toEqual(mockProjectCompleteInfo.abstractText);
      expect(result?.dmp.project?.[0].start).toEqual(mockProjectCompleteInfo.startDate);
      expect(result?.dmp.project?.[0].end).toEqual(mockProjectCompleteInfo.endDate);
      expect(result?.dmp?.research_domain.name).toEqual(mockProjectCompleteInfo.dmptool_research_domain);
    });

    it('includes minimal funding in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: [mockMinimalPlanFunding]})
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      expect(result?.dmp?.project?.[0].funding).toBeDefined();
      expect(result?.dmp?.project?.[0].funding).toHaveLength(1);
      expect(result?.dmp?.project?.[0].funding?.[0].funder_id.identifier).toEqual(`${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}.fundings.${mockMinimalPlanFunding.id}`);
      expect(result?.dmp?.project?.[0].funding?.[0].funder_id.type).toEqual('other');
      expect(result?.dmp?.project?.[0].funding?.[0].funding_status).toEqual('planned');
    });

    it('includes complete funding in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: [mockCompletePlanFunding]})
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      expect(result?.dmp?.project?.[0].funding).toBeDefined();
      expect(result?.dmp?.project?.[0].funding).toHaveLength(1);
      expect(result?.dmp?.project?.[0].funding?.[0].funder_id).toEqual({
        identifier: mockCompletePlanFunding.uri,
        type: 'ror'
      });
      expect(result?.dmp?.project?.[0].funding?.[0].grant_id).toEqual({
        identifier: mockCompletePlanFunding.grantId,
        type: 'url'
      });
      expect(result?.dmp?.funding_project).toEqual([{
        project_id: {
          identifier: `${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}`,
          type: 'other'
        },
        funder_id: {
          identifier: mockCompletePlanFunding.uri,
          type: 'ror'
        },
        project_identifier: {
          identifier: mockCompletePlanFunding.funderProjectNumber,
          type: 'other'
        }
      }]);
      expect(result?.dmp?.funding_opportunity).toEqual([{
        project_id: {
          identifier: `${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}`,
          type: 'other'
        },
        funder_id: {
          identifier: mockCompletePlanFunding.uri,
          type: 'ror'
        },
        opportunity_identifier: {
          identifier: mockCompletePlanFunding.funderOpportunityNumber,
          type: 'other'
        }
      }]);
    });

    it('uses plan owner as contact when no members have isPrimaryContact set', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: mockMembersWithoutPrimaryContact})
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      // Verify their info as the Contact
      expect(result?.dmp?.contact.name).toEqual([mockPlanOwner.givenName, mockPlanOwner.surName].join(' '));
      expect(result?.dmp?.contact.mbox).toEqual(mockPlanOwner.email);
      expect(result?.dmp?.contact?.contact_id[0].identifier).toEqual(mockPlanOwner.orcid);
      expect(result?.dmp?.contact?.contact_id[0].type).toEqual('orcid');
      expect(result?.dmp?.contact?.affiliation[0].name).toEqual(mockPlanOwner.name);
      expect(result?.dmp?.contact?.affiliation[0]?.affiliation_id.identifier).toEqual(mockPlanOwner.uri);
      expect(result?.dmp?.contact?.affiliation[0]?.affiliation_id.type).toEqual('ror');
    });

    it('uses first member with isPrimaryContact as contact', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: mockMembersWithPrimaryContact})
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})  // No Related Works Info
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      // Verify their info as the Contact
      const primaryContact = mockMembersWithPrimaryContact[0];
      expect(result?.dmp?.contact.name).toEqual([primaryContact.givenName, primaryContact.surName].join(' '));
      expect(result?.dmp?.contact.mbox).toEqual(primaryContact.email);
      expect(result?.dmp?.contact?.contact_id[0].identifier).toEqual(primaryContact.email);
      expect(result?.dmp?.contact?.contact_id[0].type).toEqual('other');
      expect(result?.dmp?.contact?.affiliation[0].name).toEqual(primaryContact.name);
      expect(result?.dmp?.contact?.affiliation[0]?.affiliation_id.identifier).toEqual(primaryContact.uri);
      expect(result?.dmp?.contact?.affiliation[0]?.affiliation_id.type).toEqual('ror');

      // Verify their info as a Contributor
      const contributor = result?.dmp?.contributor?.find((c: any) => c?.name === 'Example Contact');
      expect(contributor).toBeDefined();
      expect(contributor?.name).toEqual([primaryContact.givenName, primaryContact.surName].join(' '));
      expect(contributor?.role).toEqual(JSON.parse(primaryContact.roles));
      expect(contributor?.contributor_id[0].identifier).toEqual(`${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}.members.${primaryContact.id}`);
      expect(contributor?.contributor_id[0].type).toEqual('other');
      expect(contributor?.affiliation[0].name).toEqual(primaryContact.name);
      expect(contributor?.affiliation[0].affiliation_id.identifier).toEqual(primaryContact.uri);
      expect(contributor?.affiliation[0].affiliation_id.type).toEqual('ror');
    });

    it('includes related works in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: []})  // No Research Outputs
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: mockRelatedWorks})
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      expect(result?.dmp?.related_identifier).toBeDefined();
      expect(result?.dmp?.related_identifier).toHaveLength(2);
      expect(result?.dmp?.related_identifier[0].relation_type).toEqual('cites');
      expect(result?.dmp?.related_identifier[0].type).toEqual('url');
      expect(result?.dmp?.related_identifier[0].identifier).toEqual(mockRelatedWorks[0].identifier);
      expect(result?.dmp?.related_identifier[0].resource_type).toEqual(mockRelatedWorks[0]?.workType?.toLowerCase());
      expect(result?.dmp?.related_identifier[1].relation_type).toEqual('cites');
      expect(result?.dmp?.related_identifier[1].type).toEqual('url');
      expect(result?.dmp?.related_identifier[1].identifier).toEqual(mockRelatedWorks[1].identifier);
      expect(result?.dmp?.related_identifier[1].resource_type).toEqual(mockRelatedWorks[1]?.workType?.toLowerCase());
    });

    it('includes a minimal set of datasets in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: [mockMinimalResearchOutputs]})
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        456
      );

      expect(result).toBeDefined();
      // Since the research outputs table has no data flags the status of
      // ethical issues will be unknown
      expect(result?.dmp.ethical_issues_exist).toEqual('unknown');

      expect(result?.dmp.dataset).toBeDefined();
      expect(result?.dmp.dataset).toHaveLength(1);
      expect(result?.dmp?.dataset[0].title).toEqual('My software');
      expect(result?.dmp?.dataset[0].type).toEqual('software');
      expect(result?.dmp?.dataset[0].dataset_id.identifier).toEqual(`${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}.outputs.1`);
      expect(result?.dmp?.dataset[0].personal_data).toEqual('unknown');
      expect(result?.dmp?.dataset[0].sensitive_data).toEqual('unknown');
      expect(result?.dmp?.dataset[0].language).toEqual('por');
      expect(result?.dmp?.dataset[0].distribution).toBeUndefined();
      expect(result?.dmp?.dataset[0].metadata).toBeUndefined();
    });

    it('includes a complete set of datasets in the DMP when present', async () => {
      // Mock all the calls to the RDS MySQL tables
      (queryTable as jest.Mock)
        .mockResolvedValueOnce({results: [mockUnregisteredPlanInfo]})
        .mockResolvedValueOnce({results: [mockProjectMinimumInfo]})
        .mockResolvedValueOnce({results: []})  // No Plan members
        .mockResolvedValueOnce({results: [mockPlanOwner]})  // Will use the plan owner
        .mockResolvedValueOnce({results: [mockResearchOutputs]})
        .mockResolvedValueOnce({results: []})  // No Funding Info
        .mockResolvedValueOnce({results: []})
        .mockResolvedValueOnce({results: [defaultMemberRole]});

      const result = await planToDMPCommonStandard(
        mockConfig,
        mockApplication,
        mockDomain,
        mockEnv,
        123
      );

      expect(result).toBeDefined();
      // Expect the ethical issues exist to be `yes` because the datasets flag
      // either `sensitive_data` or `personal_data` to be `yes`.`
      expect(result?.dmp.ethical_issues_exist).toEqual('yes');

      expect(result?.dmp.dataset).toBeDefined();
      expect(result?.dmp.dataset).toHaveLength(2);
      expect(result?.dmp.dataset[0].title).toEqual('My dataset');
      expect(result?.dmp.dataset[0].description).toEqual('A description of the dataset');
      expect(result?.dmp.dataset[0].type).toEqual('dataset');
      expect(result?.dmp.dataset[0].dataset_id.identifier).toEqual(`${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}.outputs.1`);
      expect(result?.dmp.dataset[0].dataset_id.type).toEqual('other');
      expect(result?.dmp.dataset[0].personal_data).toEqual('no');
      expect(result?.dmp.dataset[0].sensitive_data).toEqual('yes');
      expect(result?.dmp.dataset[0].issued).toEqual('2028-01-01');
      expect(result?.dmp.dataset[0].language).toEqual('por');
      expect(result?.dmp.dataset[0].distribution).toHaveLength(2);
      expect(result?.dmp.dataset[0].distribution[0].title).toEqual('My dataset');
      expect(result?.dmp.dataset[0].distribution[0].byte_size).toEqual(123000000);
      expect(result?.dmp.dataset[0].distribution[0].data_access).toEqual('open');
      expect(result?.dmp.dataset[0].distribution[0].issued).toEqual('2028-01-01');
      expect(result?.dmp.dataset[0].distribution[0].license).toHaveLength(1);
      expect(result?.dmp.dataset[0].distribution[0].license[0].license_ref).toEqual('https://example.org/licenses/123456789');
      expect(result?.dmp.dataset[0].distribution[0].license[0].start_date).toEqual('2028-01-01');
      expect(result?.dmp.dataset[0].distribution[0].host.title).toEqual('Example Repository');
      expect(result?.dmp.dataset[0].distribution[0].host.url).toEqual('https://example.org/repositories/123456789');
      expect(result?.dmp.dataset[0].distribution[0].host.host_id).toHaveLength(1)
      expect(result?.dmp.dataset[0].distribution[0].host.host_id[0].identifier).toEqual('https://example.org/repositories/123456789');
      expect(result?.dmp.dataset[0].distribution[0].host.host_id[0].type).toEqual('url');
      expect(result?.dmp.dataset[0].distribution[1].title).toEqual('My dataset');
      expect(result?.dmp.dataset[0].distribution[1].byte_size).toEqual(123000000);
      expect(result?.dmp.dataset[0].distribution[1].data_access).toEqual('open');
      expect(result?.dmp.dataset[0].distribution[1].issued).toEqual('2028-01-01');
      expect(result?.dmp.dataset[0].distribution[1].license).toHaveLength(1);
      expect(result?.dmp.dataset[0].distribution[1].license[0].license_ref).toEqual('https://example.org/licenses/123456789');
      expect(result?.dmp.dataset[0].distribution[1].license[0].start_date).toEqual('2028-01-01');
      expect(result?.dmp.dataset[0].distribution[1].host.title).toEqual('Example Repository 2');
      expect(result?.dmp.dataset[0].distribution[1].host.url).toEqual('https://example.org/repositories/987654321');
      expect(result?.dmp.dataset[0].distribution[1].host.host_id).toHaveLength(1)
      expect(result?.dmp.dataset[0].distribution[1].host.host_id[0].identifier).toEqual('https://example.org/repositories/987654321');
      expect(result?.dmp.dataset[0].distribution[1].host.host_id[0].type).toEqual('url');
      expect(result?.dmp?.dataset[0].metadata).toHaveLength(2)
      expect(result?.dmp?.dataset[0].metadata[0].description).toEqual('Example Standard');
      expect(result?.dmp?.dataset[0].metadata[0].language).toEqual('eng');
      expect(result?.dmp?.dataset[0].metadata[0].metadata_standard_id[0].identifier).toEqual('https://example.org/standards/123456789');
      expect(result?.dmp?.dataset[0].metadata[0].metadata_standard_id[0].type).toEqual('url');
      expect(result?.dmp?.dataset[0].metadata[1].description).toEqual('Example Standard 2');
      expect(result?.dmp?.dataset[0].metadata[1].language).toEqual('eng');
      expect(result?.dmp?.dataset[0].metadata[1].metadata_standard_id[0].identifier).toEqual('https://example.org/standards/987654321');
      expect(result?.dmp?.dataset[0].metadata[1].metadata_standard_id[0].type).toEqual('url');

      expect(result?.dmp.dataset[1].title).toEqual('My software');
      expect(result?.dmp.dataset[1].description).toEqual('Software to process the dataset');
      expect(result?.dmp.dataset[1].type).toEqual('software');
      expect(result?.dmp.dataset[1].dataset_id.identifier).toEqual(`${mockApplication}.projects.${mockProjectMinimumInfo.id}.dmp.${mockUnregisteredPlanInfo.id}.outputs.2`);
      expect(result?.dmp.dataset[1].dataset_id.type).toEqual('other');
      expect(result?.dmp.dataset[1].personal_data).toEqual('no');
      expect(result?.dmp.dataset[1].sensitive_data).toEqual('no');
      expect(result?.dmp.dataset[1].issued).toEqual('2028-03-01');
      expect(result?.dmp.dataset[1].language).toEqual('por');
      expect(result?.dmp.dataset[1].distribution).toHaveLength(1);
      expect(result?.dmp.dataset[1].distribution[0].title).toEqual('My software');
      expect(result?.dmp.dataset[1].distribution[0].byte_size).toEqual(123000);
      expect(result?.dmp.dataset[1].distribution[0].data_access).toEqual('open');
      expect(result?.dmp.dataset[1].distribution[0].issued).toEqual('2028-03-01');
      expect(result?.dmp.dataset[1].distribution[0].license).toHaveLength(1);
      expect(result?.dmp.dataset[1].distribution[0].license[0].license_ref).toEqual('https://example.org/licenses/35353545');
      expect(result?.dmp.dataset[1].distribution[0].license[0].start_date).toEqual('2028-03-01');
      expect(result?.dmp.dataset[1].distribution[0].host.title).toEqual('Example Repository');
      expect(result?.dmp.dataset[1].distribution[0].host.url).toEqual('https://example.org/repositories/35353545');
      expect(result?.dmp.dataset[1].distribution[0].host.host_id).toHaveLength(1)
      expect(result?.dmp.dataset[1].distribution[0].host.host_id[0].identifier).toEqual('https://example.org/repositories/35353545');
      expect(result?.dmp.dataset[1].distribution[0].host.host_id[0].type).toEqual('url');
      expect(result?.dmp.dataset[1].metadata).toHaveLength(1)
      expect(result?.dmp.dataset[1].metadata[0].description).toEqual('Example Standard');
      expect(result?.dmp.dataset[1].metadata[0].language).toEqual('eng');
      expect(result?.dmp.dataset[1].metadata[0].metadata_standard_id[0].identifier).toEqual('https://example.org/standards/35353545');
      expect(result?.dmp.dataset[1].metadata[0].metadata_standard_id[0].type).toEqual('url');
    });
  });
});
