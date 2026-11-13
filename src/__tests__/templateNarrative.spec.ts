import pino, { Logger } from 'pino';
import { loadTemplateNarrative } from '../templateNarrative';
import { queryTable } from '../rds';

jest.mock('../rds');

const mockLogger: Logger = pino({ level: 'silent' });
const mockConfig = {
  logger: mockLogger,
  host: 'localhost',
  port: 3306,
  user: 'test',
  password: 'test',
  database: 'testdb'
};

const baseRow = {
  versionedTemplateId: 10,
  templateId: 3,
  templateTitle: 'NSF Polar Programs',
  templateDescription: '<p>A template for NSF Polar Programs</p>',
  templateVersion: 'v2',
  visibility: 'PUBLIC',
  versionType: 'PUBLISHED',
  active: 1,
  isDefault: 0,
  languageId: 'en-US',
  modified: '2026-10-01 12:00:00',
  ownerURI: 'https://ror.org/021nxhr62',
  ownerName: 'National Science Foundation (nsf.gov)',
  ownerDisplayName: 'National Science Foundation',
};

const textAreaJSON = {
  type: 'textArea',
  attributes: { cols: 20, rows: 4, asRichText: true },
  meta: { schemaVersion: '1.0' },
};

const radioJSON = {
  type: 'radioButtons',
  options: [
    { label: 'Yes', value: 'yes', selected: true },
    { label: 'No', value: 'no', selected: false },
  ],
  meta: { schemaVersion: '1.0' },
};

describe('loadTemplateNarrative', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('returns undefined when the template does not exist', async () => {
    (queryTable as jest.Mock).mockResolvedValueOnce({ results: [], fields: [] });

    const result = await loadTemplateNarrative(mockConfig, 999);
    expect(result).toBeUndefined();
    expect(queryTable).toHaveBeenCalledWith(mockConfig, expect.stringContaining('FROM versionedTemplates vt'), ['999']);
  });

  it('returns undefined when the query fails', async () => {
    (queryTable as jest.Mock).mockResolvedValueOnce(undefined);

    const result = await loadTemplateNarrative(mockConfig, 10);
    expect(result).toBeUndefined();
  });

  it('returns the template with no sections when it has none', async () => {
    (queryTable as jest.Mock).mockResolvedValueOnce({
      results: [{ ...baseRow, sectionId: null, questionId: null }],
      fields: [],
    });

    const result = await loadTemplateNarrative(mockConfig, 10);
    expect(result).toEqual({
      id: 10,
      templateId: 3,
      title: 'NSF Polar Programs',
      description: '<p>A template for NSF Polar Programs</p>',
      version: 'v2',
      visibility: 'PUBLIC',
      versionType: 'PUBLISHED',
      active: true,
      isDefault: false,
      languageId: 'en-US',
      modified: '2026-10-01 12:00:00',
      owner: {
        uri: 'https://ror.org/021nxhr62',
        name: 'National Science Foundation (nsf.gov)',
        displayName: 'National Science Foundation',
      },
      section: [],
    });
  });

  it('builds the sections and questions in order', async () => {
    (queryTable as jest.Mock).mockResolvedValueOnce({
      results: [
        {
          ...baseRow,
          sectionId: 1,
          sectionTitle: 'Roles and responsibilities',
          sectionIntroduction: '<p>Who is involved</p>',
          sectionRequirements: '<p></p>',
          sectionGuidance: null,
          sectionOrder: 1,
          questionId: 100,
          questionText: '<p>What parties will be involved?</p>',
          questionJSON: textAreaJSON,
          requirementText: '<p>Required by NSF</p>',
          guidanceText: '<p>Use ORCIDs</p>',
          sampleText: '<p>The PI, Jane Doe, ...</p>',
          useSampleTextAsDefault: 1,
          required: 1,
          questionOrder: 1,
        },
        {
          ...baseRow,
          sectionId: 1,
          sectionTitle: 'Roles and responsibilities',
          sectionIntroduction: '<p>Who is involved</p>',
          sectionRequirements: '<p></p>',
          sectionGuidance: null,
          sectionOrder: 1,
          questionId: 101,
          questionText: 'Will you use a repository?',
          // Some drivers return JSON columns as strings
          questionJSON: JSON.stringify(radioJSON),
          requirementText: null,
          guidanceText: '  <p>&nbsp;</p> ',
          sampleText: null,
          useSampleTextAsDefault: 0,
          required: 0,
          questionOrder: 2,
        },
        {
          ...baseRow,
          sectionId: 2,
          sectionTitle: 'Types of data produced',
          sectionIntroduction: null,
          sectionRequirements: null,
          sectionGuidance: '<p>Be detailed</p>',
          sectionOrder: 2,
          questionId: null,
        },
      ],
      fields: [],
    });

    const result = await loadTemplateNarrative(mockConfig, 10);
    expect(result?.section).toHaveLength(2);

    const [first, second] = result?.section ?? [];
    expect(first).toEqual({
      id: 1,
      order: 1,
      title: 'Roles and responsibilities',
      introduction: '<p>Who is involved</p>',
      requirements: undefined,
      guidance: undefined,
      question: [
        {
          id: 100,
          order: 1,
          text: '<p>What parties will be involved?</p>',
          type: 'textArea',
          json: textAreaJSON,
          requirementText: '<p>Required by NSF</p>',
          guidanceText: '<p>Use ORCIDs</p>',
          sampleText: '<p>The PI, Jane Doe, ...</p>',
          useSampleTextAsDefault: true,
          required: true,
        },
        {
          id: 101,
          order: 2,
          text: 'Will you use a repository?',
          type: 'radioButtons',
          json: radioJSON,
          requirementText: undefined,
          guidanceText: undefined,
          sampleText: undefined,
          useSampleTextAsDefault: false,
          required: false,
        },
      ],
    });

    expect(second?.title).toEqual('Types of data produced');
    expect(second?.guidance).toEqual('<p>Be detailed</p>');
    expect(second?.question).toEqual([]);
  });

  it('leaves the type and json undefined when the question JSON is invalid', async () => {
    (queryTable as jest.Mock).mockResolvedValueOnce({
      results: [{
        ...baseRow,
        sectionId: 1,
        sectionTitle: 'Section',
        sectionOrder: 1,
        questionId: 100,
        questionText: 'Question',
        questionJSON: '{not json',
        questionOrder: 1,
      }],
      fields: [],
    });

    const result = await loadTemplateNarrative(mockConfig, 10);
    expect(result?.section[0].question[0].type).toBeUndefined();
    expect(result?.section[0].question[0].json).toBeUndefined();
  });
});
