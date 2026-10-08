import { AnyAnswerType } from '@dmptool/types';
import {
  DisplayLogic,
  DisplayLogicAnswer,
  evaluateCondition,
  extractAnswerValues,
  findHiddenQuestionIds,
  isQuestionVisible,
} from '../displayLogic';

// Helper to build the display logic for a question with a single trigger question
const singleGroup = (
  triggerQuestionId: number | null,
  conditionType: string,
  conditionMatch: string,
  action = 'SHOW_QUESTION'
): DisplayLogic => ({
  action,
  matchType: 'ANY',
  groups: [{ triggerQuestionId, conditions: [{ conditionType, conditionMatch }] }],
});

// Helpers to build answers as they are stored in the answers.json column
const meta = { schemaVersion: '1.0' };
const radioAnswer = (answer: string): AnyAnswerType => ({ type: 'radioButtons', answer, meta });
const selectAnswer = (answer: string): AnyAnswerType => ({ type: 'selectBox', answer, meta });
const checkBoxesAnswer = (answer: string[]): AnyAnswerType => ({ type: 'checkBoxes', answer, meta });
const multiselectAnswer = (answer: string[]): AnyAnswerType => ({ type: 'multiselectBox', answer, meta });
const textAnswer = (answer: string): AnyAnswerType => ({ type: 'text', answer, meta });

describe('extractAnswerValues', () => {
  it('returns the selected value of a radioButtons/selectBox answer', () => {
    expect(extractAnswerValues(radioAnswer('Yes'))).toEqual(['Yes']);
    expect(extractAnswerValues(selectAnswer('NSF'))).toEqual(['NSF']);
  });

  it('returns the selected values of a checkBoxes/multiselectBox answer', () => {
    expect(extractAnswerValues(checkBoxesAnswer(['A', 'B']))).toEqual(['A', 'B']);
    expect(extractAnswerValues(multiselectAnswer(['C']))).toEqual(['C']);
  });

  it('parses an answer stored as a JSON string', () => {
    expect(extractAnswerValues('{"type":"selectBox","answer":"NSF"}')).toEqual(['NSF']);
  });

  it('treats an empty string and the checkBoxes default of [""] as unanswered', () => {
    expect(extractAnswerValues(radioAnswer(''))).toEqual([]);
    expect(extractAnswerValues(checkBoxesAnswer(['']))).toEqual([]);
  });

  it('returns an empty array for missing, non-option or invalid answers', () => {
    expect(extractAnswerValues(undefined)).toEqual([]);
    expect(extractAnswerValues(null)).toEqual([]);
    expect(extractAnswerValues({ type: 'boolean', answer: true, meta })).toEqual([]);
    expect(extractAnswerValues(textAnswer('Yes'))).toEqual([]);
    expect(extractAnswerValues('not json')).toEqual([]);
  });

  it('returns an empty array for an answer that does not match its type (the database is not validated)', () => {
    expect(extractAnswerValues({ type: 'radioButtons', answer: ['Yes'], meta } as unknown as AnyAnswerType)).toEqual([]);
    expect(extractAnswerValues({ type: 'checkBoxes', answer: 'A', meta } as unknown as AnyAnswerType)).toEqual([]);
    expect(extractAnswerValues({ answer: 'Yes' } as unknown as AnyAnswerType)).toEqual([]);
  });
});

describe('evaluateCondition', () => {
  it('EQUAL and INCLUDES match when the value is selected', () => {
    expect(evaluateCondition({ conditionType: 'EQUAL', conditionMatch: 'Yes' }, ['Yes'])).toBe(true);
    expect(evaluateCondition({ conditionType: 'EQUAL', conditionMatch: 'Yes' }, ['No'])).toBe(false);
    expect(evaluateCondition({ conditionType: 'INCLUDES', conditionMatch: 'B' }, ['A', 'B'])).toBe(true);
    expect(evaluateCondition({ conditionType: 'INCLUDES', conditionMatch: 'C' }, ['A', 'B'])).toBe(false);
  });

  it('DOES_NOT_EQUAL and DOES_NOT_INCLUDE match when the value is not selected', () => {
    expect(evaluateCondition({ conditionType: 'DOES_NOT_EQUAL', conditionMatch: 'Yes' }, ['No'])).toBe(true);
    expect(evaluateCondition({ conditionType: 'DOES_NOT_EQUAL', conditionMatch: 'Yes' }, ['Yes'])).toBe(false);
    expect(evaluateCondition({ conditionType: 'DOES_NOT_INCLUDE', conditionMatch: 'C' }, ['A', 'B'])).toBe(true);
    expect(evaluateCondition({ conditionType: 'DOES_NOT_INCLUDE', conditionMatch: 'B' }, ['A', 'B'])).toBe(false);
  });

  it('does not match an unknown condition type', () => {
    expect(evaluateCondition({ conditionType: 'UNKNOWN', conditionMatch: 'Yes' }, ['Yes'])).toBe(false);
  });
});

describe('isQuestionVisible', () => {
  const answers = (entries: [number, string[]][]) => new Map<number, string[]>(entries);

  it('shows a question that has no display logic', () => {
    expect(isQuestionVisible(undefined, answers([]))).toBe(true);
    expect(isQuestionVisible({ action: 'SHOW_QUESTION', matchType: 'ANY', groups: [] }, answers([]))).toBe(true);
  });

  describe('Unanswered trigger questions', () => {
    it('shows a SHOW_QUESTION question when its only trigger question is unanswered', () => {
      expect(isQuestionVisible(singleGroup(1, 'EQUAL', 'Yes'), answers([]))).toBe(true);
    });

    it('shows a HIDE_QUESTION question when its only trigger question is unanswered', () => {
      const logic = singleGroup(1, 'DOES_NOT_EQUAL', 'Yes', 'HIDE_QUESTION');
      expect(isQuestionVisible(logic, answers([]))).toBe(true);
    });

    it('shows the question when a checkBoxes trigger question has nothing checked', () => {
      const logic = singleGroup(1, 'DOES_NOT_INCLUDE', 'A', 'HIDE_QUESTION');
      expect(isQuestionVisible(logic, answers([[1, []]]))).toBe(true);
    });

    it('treats a trigger question that could not be resolved as unanswered', () => {
      expect(isQuestionVisible(singleGroup(null, 'EQUAL', 'Yes'), answers([[1, ['No']]]))).toBe(true);
    });

    describe('ALL: the logic is not applied while any trigger question is unanswered', () => {
      const logic = (action: string): DisplayLogic => ({
        action,
        matchType: 'ALL',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 4, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      });

      it('shows a HIDE_QUESTION question even though the answered trigger question matches', () => {
        expect(isQuestionVisible(logic('HIDE_QUESTION'), answers([[1, ['Yes']]]))).toBe(true);
      });

      it('shows a SHOW_QUESTION question even though the answered trigger question does not match', () => {
        expect(isQuestionVisible(logic('SHOW_QUESTION'), answers([[1, ['No']]]))).toBe(true);
      });
    });

    describe('ANY: one matching trigger question is enough, even if others are unanswered', () => {
      const logic = (action: string): DisplayLogic => ({
        action,
        matchType: 'ANY',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 4, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      });

      it('hides a HIDE_QUESTION question when one answered trigger question matches', () => {
        expect(isQuestionVisible(logic('HIDE_QUESTION'), answers([[1, ['Yes']]]))).toBe(false);
      });

      it('shows a SHOW_QUESTION question when one answered trigger question matches', () => {
        expect(isQuestionVisible(logic('SHOW_QUESTION'), answers([[4, ['NSF']]]))).toBe(true);
      });

      it('does not apply the logic when no answered trigger question matches and others are unanswered', () => {
        expect(isQuestionVisible(logic('SHOW_QUESTION'), answers([[1, ['No']]]))).toBe(true);
        expect(isQuestionVisible(logic('HIDE_QUESTION'), answers([[1, ['No']]]))).toBe(true);
      });
    });
  });

  describe('All trigger questions answered', () => {
    it('SHOW_QUESTION shows the question when the logic matches, and hides it otherwise', () => {
      const logic = singleGroup(1, 'EQUAL', 'Yes');
      expect(isQuestionVisible(logic, answers([[1, ['Yes']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['No']]]))).toBe(false);
    });

    it('HIDE_QUESTION hides the question when the logic matches, and shows it otherwise', () => {
      const logic = singleGroup(1, 'EQUAL', 'No', 'HIDE_QUESTION');
      expect(isQuestionVisible(logic, answers([[1, ['No']]]))).toBe(false);
      expect(isQuestionVisible(logic, answers([[1, ['Yes']]]))).toBe(true);
    });

    it('ANY: a group matches when any of its conditions match (OR)', () => {
      const logic: DisplayLogic = {
        action: 'SHOW_QUESTION',
        matchType: 'ANY',
        groups: [{
          triggerQuestionId: 1,
          conditions: [
            { conditionType: 'EQUAL', conditionMatch: 'NSF' },
            { conditionType: 'EQUAL', conditionMatch: 'NIH' },
          ],
        }],
      };
      expect(isQuestionVisible(logic, answers([[1, ['NIH']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['Other']]]))).toBe(false);
    });

    it('ALL: a group matches only when all of its conditions match (AND)', () => {
      // A checkBoxes trigger question where both A and B must be checked
      const logic: DisplayLogic = {
        action: 'SHOW_QUESTION',
        matchType: 'ALL',
        groups: [{
          triggerQuestionId: 1,
          conditions: [
            { conditionType: 'INCLUDES', conditionMatch: 'A' },
            { conditionType: 'INCLUDES', conditionMatch: 'B' },
          ],
        }],
      };
      expect(isQuestionVisible(logic, answers([[1, ['A', 'B']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['A']]]))).toBe(false);
    });

    it('ALL: combines "includes" and "does not include" conditions within a group', () => {
      const logic: DisplayLogic = {
        action: 'HIDE_QUESTION',
        matchType: 'ALL',
        groups: [{
          triggerQuestionId: 1,
          conditions: [
            { conditionType: 'INCLUDES', conditionMatch: 'A' },
            { conditionType: 'DOES_NOT_INCLUDE', conditionMatch: 'C' },
          ],
        }],
      };
      expect(isQuestionVisible(logic, answers([[1, ['A', 'B']]]))).toBe(false);
      expect(isQuestionVisible(logic, answers([[1, ['A', 'C']]]))).toBe(true);
    });

    it('requires every group to match when the matchType is ALL', () => {
      const logic: DisplayLogic = {
        action: 'SHOW_QUESTION',
        matchType: 'ALL',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 4, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      };
      expect(isQuestionVisible(logic, answers([[1, ['Yes']], [4, ['NSF']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['Yes']], [4, ['NIH']]]))).toBe(false);
    });

    it('requires only one group to match when the matchType is ANY', () => {
      const logic: DisplayLogic = {
        action: 'SHOW_QUESTION',
        matchType: 'ANY',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 4, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      };
      expect(isQuestionVisible(logic, answers([[1, ['No']], [4, ['NSF']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['No']], [4, ['NIH']]]))).toBe(false);
    });

    it('does not change visibility for any other action (e.g. SEND_EMAIL)', () => {
      const logic = singleGroup(1, 'EQUAL', 'Yes', 'SEND_EMAIL');
      expect(isQuestionVisible(logic, answers([[1, ['No']]]))).toBe(true);
      expect(isQuestionVisible(logic, answers([[1, ['Yes']]]))).toBe(true);
    });
  });

  describe('Rule 5: a hidden trigger question is treated as unanswered', () => {
    it('ignores the saved answer of a hidden trigger question', () => {
      // Q3 is hidden, so its saved answer (NSF) does not count and the logic is not applied
      expect(isQuestionVisible(singleGroup(3, 'EQUAL', 'NSF'), answers([[3, ['NSF']]]), new Set([3]))).toBe(true);
      expect(isQuestionVisible(singleGroup(3, 'EQUAL', 'NSF', 'HIDE_QUESTION'), answers([[3, ['NSF']]]), new Set([3])))
        .toBe(true);
    });

    it('ALL: does not apply the logic when one of the trigger questions is hidden', () => {
      const logic: DisplayLogic = {
        action: 'HIDE_QUESTION',
        matchType: 'ALL',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 3, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      };
      expect(isQuestionVisible(logic, answers([[1, ['Yes']], [3, ['NSF']]]), new Set([3]))).toBe(true);
    });

    it('ANY: still applies the logic when another, visible trigger question matches', () => {
      const logic: DisplayLogic = {
        action: 'HIDE_QUESTION',
        matchType: 'ANY',
        groups: [
          { triggerQuestionId: 1, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'Yes' }] },
          { triggerQuestionId: 3, conditions: [{ conditionType: 'EQUAL', conditionMatch: 'NSF' }] },
        ],
      };
      expect(isQuestionVisible(logic, answers([[1, ['Yes']], [3, ['NSF']]]), new Set([3]))).toBe(false);
    });
  });
});

describe('findHiddenQuestionIds', () => {
  it('returns an empty set when there is no display logic', () => {
    const answersByQuestionId = new Map<number, DisplayLogicAnswer>([[1, radioAnswer('No')]]);
    expect(findHiddenQuestionIds([1, 2, 3], new Map(), answersByQuestionId)).toEqual(new Set());
  });

  it('returns the questions that are hidden by their display logic', () => {
    const logic = new Map<number, DisplayLogic>([
      [2, singleGroup(1, 'EQUAL', 'Yes')],
      [3, singleGroup(1, 'EQUAL', 'No')],
    ]);
    const hidden = findHiddenQuestionIds([1, 2, 3], logic, new Map<number, DisplayLogicAnswer>([[1, radioAnswer('No')]]));
    expect(hidden).toEqual(new Set([2]));
  });

  it('Rule 4: uses the saved answers of hidden questions without changing them', () => {
    const answersByQuestionId = new Map<number, DisplayLogicAnswer>([[1, radioAnswer('No')], [2, textAnswer('Some text')]]);
    const logic = new Map<number, DisplayLogic>([[2, singleGroup(1, 'EQUAL', 'Yes')]]);
    const hidden = findHiddenQuestionIds([1, 2], logic, answersByQuestionId);

    expect(hidden).toEqual(new Set([2]));
    expect(answersByQuestionId.get(2)).toEqual(textAnswer('Some text'));
  });

  it('Rule 5: treats a hidden trigger question as unanswered for the questions that depend on it', () => {
    // Q1 "Is there funding?" = No hides Q3 "Who is the funder?" (saved answer: NSF). Q3 counts as
    // unanswered, so Q6's display logic ("show when Q3 = NSF") is not applied and Q6 is displayed
    const logic = new Map<number, DisplayLogic>([
      [3, singleGroup(1, 'EQUAL', 'Yes')],
      [6, singleGroup(3, 'EQUAL', 'NSF')],
    ]);
    // Answers stored as JSON strings are also accepted
    const answersByQuestionId = new Map<number, DisplayLogicAnswer>([
      [1, '{"type":"radioButtons","answer":"No","meta":{"schemaVersion":"1.0"}}'],
      [3, '{"type":"selectBox","answer":"NSF","meta":{"schemaVersion":"1.0"}}'],
    ]);
    expect(findHiddenQuestionIds([1, 3, 6], logic, answersByQuestionId)).toEqual(new Set([3]));
  });
});
