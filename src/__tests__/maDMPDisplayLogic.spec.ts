import { DMPToolDMPType } from '@dmptool/types';
import { planToDMPCommonStandard } from '../maDMP';
import {
  DMPExtensionNarrativeQuestion,
  DMPExtensionNarrativeSection,
  LoadMemberInfo,
  LoadPlanInfo,
  LoadProjectInfo,
  RDACommonStandardDataset,
} from '../maDMPTypes';

// Mock external dependencies
jest.mock('../rds');

import pino, { Logger } from 'pino';
import { queryTable } from '../rds';
import { EnvironmentEnum } from '../general';

const mockLogger: Logger = pino({ level: 'silent' });

const mockConfig = {
  logger: mockLogger,
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: 'password',
  database: 'dmp',
};

const mockPlan: LoadPlanInfo = {
  id: 123,
  dmpId: 'https://doi.org/11.22222/67890',
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
  languageId: 'en-US',
};

const mockProject: LoadProjectInfo = { id: 12, title: 'Test Research Project' };

const mockPlanOwner: LoadMemberInfo = {
  id: 875,
  uri: 'https://ror.org/000000000',
  name: 'Example University',
  email: 'owner@example.com',
  givenName: 'Example',
  surName: 'Owner',
  orcid: 'https://orcid.org/0000-0000-0000-0000',
  isPrimaryContact: true,
  roles: '["other"]',
};

// A real answer to a research outputs question (as stored in the answers.json column)
const researchOutputsAnswer = {
  meta: { schemaVersion: '1.0' },
  type: 'researchOutputTable',
  answer: [{
    columns: [
      { meta: { schemaVersion: '1.0' }, type: 'text', answer: 'RO Question', commonStandardId: 'title' },
      { meta: { schemaVersion: '1.0' }, type: 'textArea', answer: '<p>This is my description</p>', commonStandardId: 'description' },
      { meta: { schemaVersion: '1.0' }, type: 'selectBox', answer: 'my-output-type', commonStandardId: 'type' },
      { meta: { schemaVersion: '1.0' }, type: 'checkBoxes', answer: ['sensitive', 'personal'], commonStandardId: 'data_flags' },
      {
        meta: { schemaVersion: '1.0' },
        type: 'repositorySearch',
        answer: [{
          repositoryId: 'https://www.re3data.org/repository/r3d100014251',
          repositoryName: 'Arias Montano',
          repositoryType: ['institutional'],
          repositoryWebsite: 'https://ariasmontano.uhu.es',
          repositoryKeywords: ['multidisciplinary'],
          repositoryDescription: 'Arias Montano, Institutional Repository of the University of Huelva',
        }],
        commonStandardId: 'host',
      },
      {
        meta: { schemaVersion: '1.0' },
        type: 'metadataStandardSearch',
        answer: [{ metadataStandardId: 'https://repositorio.unicamp.br/', metadataStandardName: 'Terminal RI Unicamp' }],
        commonStandardId: 'metadata',
      },
      {
        meta: { schemaVersion: '1.0' },
        type: 'licenseSearch',
        answer: [{ licenseId: 'https://spdx.org/licenses/CC0-1.0.json', licenseName: 'CC0-1.0' }],
        commonStandardId: 'license_ref',
      },
      { meta: { schemaVersion: '1.0' }, type: 'radioButtons', answer: 'open', commonStandardId: 'data_access' },
      { meta: { schemaVersion: '1.0' }, type: 'date', answer: '2026-10-30', commonStandardId: 'issued' },
      { meta: { schemaVersion: '1.0' }, type: 'numberWithContext', answer: { value: 2, context: 'kb' }, commonStandardId: 'byte_size' },
      { meta: { schemaVersion: '1.0' }, type: 'text', answer: 'Custom Field Default Value', commonStandardId: 'custom' },
    ],
  }],
  columnHeadings: [
    'Title', 'Description', 'Output Type', 'Data Flags', 'Repositories', 'Metadata Standards', 'Licenses',
    'Initial Access Levels', 'Anticipated Release Date', 'Anticipated File Size', 'Custom Field',
  ],
};

// The template has 3 BASE questions:
//   Q1: "Will this project produce data?" (radioButtons)
//   Q2: the research outputs table, displayed only when Q1 = "Yes"
//   Q3: a text question without display logic
const narrativeRow = (questionId: number, questionText: string, answerJSON: object | null) => ({
  templateId: 999,
  templateTitle: 'Example DMP Tool Template',
  templateDescription: 'This template is for testing only!',
  templateVersion: 'v1',
  sectionId: 1,
  sectionTitle: 'Data',
  sectionDescription: '<p>About the data</p>',
  sectionOrder: 1,
  questionId,
  questionText,
  questionJSON: '{"type":"text"}',
  questionOrder: questionId,
  answerId: answerJSON ? questionId * 10 : null,
  answerJSON,
});

const displayLogicRows = [{
  versionedQuestionId: 2,
  displayLogicAction: 'SHOW_QUESTION',
  displayLogicMatchType: 'ANY',
  groupId: 50,
  triggerVersionedQuestionId: 1,
  conditionType: 'EQUAL',
  conditionMatch: 'Yes',
}];

// Mock the database by matching the SQL statement rather than relying on the order of the calls.
// The answers.json column is a MySQL JSON column, which mysql2 returns as a parsed object
const mockDatabase = (q1Answer: string) => {
  const q1JSON = { type: 'radioButtons', answer: q1Answer, meta: { schemaVersion: '1.0' } };
  const results = (sql: string): unknown[] => {
    if (sql.includes('versionedQuestionConditionGroups')) return displayLogicRows;
    if (sql.includes('AS answerJSON') && !sql.includes('templateTitle')) {
      return [
        { versionedQuestionId: 1, answerJSON: q1JSON },
        { versionedQuestionId: 2, answerJSON: researchOutputsAnswer },
        { versionedQuestionId: 3, answerJSON: null },
      ];
    }
    if (sql.includes('templateTitle')) {
      return [
        narrativeRow(1, 'Will this project produce data?', q1JSON),
        narrativeRow(2, 'Describe your research outputs', researchOutputsAnswer),
        narrativeRow(3, 'Anything else?', null),
      ];
    }
    if (sql.includes('researchOutputTable')) return [{ versionedQuestionId: 2, json: researchOutputsAnswer }];
    if (sql.includes('FROM projects')) return [mockProject];
    if (sql.includes('FROM users u')) return [mockPlanOwner];
    if (sql.includes('FROM memberRoles')) return [{ id: 'tester' }];
    if (sql.includes('FROM plans') && sql.includes('dmpId')) return [mockPlan];
    return [];
  };
  (queryTable as jest.Mock).mockImplementation(async (_params, sql: string) => ({ results: results(sql), fields: [] }));
};

const generate = () => planToDMPCommonStandard(
  mockConfig,
  'test-app',
  'example.com',
  EnvironmentEnum.DEV,
  123,
  true
);

const narrativeSections = (dmp: DMPToolDMPType | undefined): DMPExtensionNarrativeSection[] =>
  dmp?.dmp?.narrative?.template?.section ?? [];

const narrativeQuestionIds = (dmp: DMPToolDMPType | undefined): number[] =>
  narrativeSections(dmp).flatMap((section: DMPExtensionNarrativeSection) =>
    section.question.map((question: DMPExtensionNarrativeQuestion) => question.id)
  );

const datasetTitles = (dmp: DMPToolDMPType | undefined): string[] =>
  (dmp?.dmp?.dataset ?? []).map((dataset: RDACommonStandardDataset) => dataset.title);

const sqlCalls = (): string[] => (queryTable as jest.Mock).mock.calls.map((call) => call[1] as string);

describe('planToDMPCommonStandard display logic', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('research outputs', () => {
    it('finds researchOutputTable answers and builds the datasets from the JSON column object', async () => {
      mockDatabase('Yes');
      const result = await generate();

      const researchSql = sqlCalls().find((sql) => sql.includes('FROM answers a'));
      expect(researchSql).toContain(`LIKE '%"researchOutputTable"%'`);

      const dataset = result?.dmp?.dataset?.[0];
      expect(result?.dmp?.dataset).toHaveLength(1);
      expect(dataset?.title).toEqual('RO Question');
      expect(dataset?.type).toEqual('my-output-type');
      expect(dataset?.sensitive_data).toEqual('yes');
      expect(dataset?.personal_data).toEqual('yes');
      expect(result?.dmp?.ethical_issues_exist).toEqual('yes');
    });
  });

  describe('display logic', () => {
    it('leaves out the hidden question and its research outputs', async () => {
      mockDatabase('No');
      const result = await generate();

      expect(narrativeQuestionIds(result)).toEqual([1, 3]);
      // The hidden research outputs are ignored, so the generic default dataset is used
      expect(datasetTitles(result)).toEqual(['Generic Dataset']);
    });

    it('renumbers the remaining questions sequentially', async () => {
      mockDatabase('No');
      const result = await generate();

      const orders = narrativeSections(result)[0].question.map((question: DMPExtensionNarrativeQuestion) => question.order);
      expect(orders).toEqual([1, 2]);
    });

    it('includes the question and its research outputs when the display logic matches', async () => {
      mockDatabase('Yes');
      const result = await generate();

      expect(narrativeQuestionIds(result)).toEqual([1, 2, 3]);
      expect(datasetTitles(result)).toEqual(['RO Question']);
    });

    it('includes the question when its trigger question is unanswered', async () => {
      mockDatabase('');
      const result = await generate();

      expect(narrativeQuestionIds(result)).toEqual([1, 2, 3]);
      expect(datasetTitles(result)).toEqual(['RO Question']);
    });

    it('does not load the answers when the template has no display logic', async () => {
      mockDatabase('No');
      const defaultImpl = (queryTable as jest.Mock).getMockImplementation();
      (queryTable as jest.Mock).mockImplementation(async (params, sql: string) =>
        sql.includes('versionedQuestionConditionGroups') ? { results: [], fields: [] } : defaultImpl?.(params, sql)
      );

      const result = await generate();

      expect(narrativeQuestionIds(result)).toEqual([1, 2, 3]);
      expect(sqlCalls().some((sql) => sql.includes('AS answerJSON') && !sql.includes('templateTitle'))).toBe(false);
    });
  });
});
