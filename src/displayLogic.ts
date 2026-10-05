// Display (conditional) logic rules for template questions.
//
// A question's display logic is made up of one or more groups. Each group checks the answer to a
// single prior "trigger" question against one or more conditions. These functions decide whether a
// question should be displayed based on that logic and a Plan's answers. Every service that needs to 
// know which questions are visible (the DMP Common Standard / narrative, Apollo server, etc.) applies 
// exactly the same rules.
//
// The rules:
//   1. Each group checks the answer to one trigger question against one or more conditions. The
//      question's matchType applies to ALL of its conditions, both within each group and across groups:
//        - ALL: every condition in every group must match (AND)
//        - ANY: at least one condition in any group must match (OR)
//   2. Unanswered trigger questions:
//        - ALL: if any trigger question is unanswered, the logic is not applied (the question is
//          displayed)
//        - ANY: the logic matches as soon as any condition on an answered trigger question matches,
//          even if other trigger questions are unanswered. If none match and some trigger questions
//          are still unanswered, the logic is not applied (the question is displayed). If they are
//          all answered and none match, the logic does not match
//   3. When the logic is applied:
//        - SHOW_QUESTION displays the question only when the logic matches
//        - HIDE_QUESTION hides the question when the logic matches
//        - any other action (e.g. SEND_EMAIL) has no effect on visibility
//   4. Hiding a question only affects its display. Its answer is not changed or removed
//   5. A question can be both a trigger question and have display logic of its own. A trigger question
//      that is hidden is treated as unanswered (its saved answer is ignored) and rule 2 applies

import { AnyAnswerType } from '@dmptool/types';

export type DisplayLogicConditionType = 'EQUAL' | 'DOES_NOT_EQUAL' | 'INCLUDES' | 'DOES_NOT_INCLUDE';
export type DisplayLogicAction = 'SHOW_QUESTION' | 'HIDE_QUESTION' | 'SEND_EMAIL';
export type DisplayLogicMatchType = 'ANY' | 'ALL';

export interface DisplayLogicCondition {
  conditionType: DisplayLogicConditionType | string;
  conditionMatch: string | null;
}

export interface DisplayLogicGroup {
  // The id of the trigger question. Null if it could not be resolved (it is then treated as unanswered)
  triggerQuestionId: number | null;
  conditions: DisplayLogicCondition[];
}

export interface DisplayLogic {
  action: DisplayLogicAction | string;
  matchType: DisplayLogicMatchType | string;
  groups: DisplayLogicGroup[];
}

// An answer as stored in the answers.json column. It is a MySQL JSON column, so mysql2 returns it as
// a parsed object, but a JSON string is also accepted. Null/undefined if the question is unanswered
export type DisplayLogicAnswer = AnyAnswerType | string | null | undefined;

/**
 * Extracts the selected option value(s) from an option based answer (radioButtons, selectBox,
 * checkBoxes, multiselectBox). These are the only answer types that can be used as trigger questions,
 * so any other answer type returns no values. Empty strings are ignored (e.g. the checkBoxes default
 * of `['']`).
 *
 * The answer comes from the database and is not validated against the schema, so the value of each
 * answer is still checked before it is used.
 *
 * @param answerJSON the answer JSON
 * @returns the selected value(s) as an array (empty if unanswered)
 */
export function extractAnswerValues(answerJSON: DisplayLogicAnswer): string[] {
  let json: AnyAnswerType | null | undefined;
  if (typeof answerJSON === 'string') {
    try {
      json = JSON.parse(answerJSON) as AnyAnswerType;
    } catch {
      return [];
    }
  } else {
    json = answerJSON;
  }

  switch (json?.type) {
    case 'radioButtons':
    case 'selectBox':
      return typeof json.answer === 'string' && json.answer !== '' ? [json.answer] : [];
    case 'checkBoxes':
    case 'multiselectBox':
      return Array.isArray(json.answer)
        ? json.answer.filter((val): val is string => typeof val === 'string' && val !== '')
        : [];
    default:
      return [];
  }
}

/**
 * Evaluates a single condition against the selected value(s) of the trigger question.
 *
 * @param condition the condition to evaluate
 * @param values the selected value(s) of the trigger question
 * @returns true if the condition matches
 */
export function evaluateCondition(condition: DisplayLogicCondition, values: string[]): boolean {
  const match = condition.conditionMatch ?? '';
  switch (condition.conditionType) {
    case 'EQUAL':
    case 'INCLUDES':
      return values.includes(match);
    case 'DOES_NOT_EQUAL':
    case 'DOES_NOT_INCLUDE':
      return !values.includes(match);
    default:
      return false;
  }
}

/**
 * Determines whether a question should be displayed based on its display logic.
 *
 * @param logic the display logic for the question
 * @param answerValues a map of question id to the selected values of its answer (see extractAnswerValues)
 * @param hiddenQuestionIds the ids of questions that have already been determined to be hidden. A hidden
 * trigger question is treated as unanswered
 * @returns true if the question should be displayed
 */
export function isQuestionVisible(
  logic: DisplayLogic | undefined,
  answerValues: Map<number, string[]>,
  hiddenQuestionIds = new Set<number>()
): boolean {
  if (!logic || logic.groups.length === 0) return true;
  if (logic.action !== 'SHOW_QUESTION' && logic.action !== 'HIDE_QUESTION') return true;

  // Rule 1: the matchType applies to all of the conditions, so a group matches when ALL (or ANY) of its
  // conditions match. Groups whose trigger question is unanswered, hidden (rule 5) or could not be
  // resolved are not evaluated
  const matchesAll = logic.matchType === 'ALL';
  const groupResults = logic.groups.map((group) => {
    const triggerId = group.triggerQuestionId;
    const values = triggerId != null && !hiddenQuestionIds.has(triggerId) ? answerValues.get(triggerId) ?? [] : [];
    const conditionMatches = group.conditions.map((condition) => evaluateCondition(condition, values));
    return {
      answered: values.length > 0,
      matched: values.length > 0 && (matchesAll ? conditionMatches.every(Boolean) : conditionMatches.some(Boolean)),
    };
  });
  const anyUnanswered = groupResults.some((result) => !result.answered);

  // Rule 2: combine the groups based on the matchType, allowing for unanswered trigger questions
  let matched: boolean;
  if (matchesAll) {
    // ALL: any unanswered trigger → not applied; otherwise every group must match
    if (anyUnanswered) return true;
    matched = groupResults.every((result) => result.matched);
  } else {
    // ANY: one matching group is enough, even if others are unanswered;
    // if none match and some are unanswered → not applied
    matched = groupResults.some((result) => result.matched);
    if (!matched && anyUnanswered) return true;
  }

  // Rule 3: apply the action
  return logic.action === 'SHOW_QUESTION' ? matched : !matched;
}

/**
 * Determines which questions should be hidden based on their display logic and a Plan's answers.
 *
 * @param orderedQuestionIds the ids of the questions in display order (trigger questions always
 * come before the questions that depend on them)
 * @param logicByQuestionId a map of question id to its display logic (questions without display
 * logic can be omitted)
 * @param answersByQuestionId a map of question id to its answer JSON (see DisplayLogicAnswer)
 * @returns the ids of the questions that should be hidden
 */
export function findHiddenQuestionIds(
  orderedQuestionIds: number[],
  logicByQuestionId: Map<number, DisplayLogic>,
  answersByQuestionId: Map<number, DisplayLogicAnswer>
): Set<number> {
  const hidden = new Set<number>();
  if (logicByQuestionId.size === 0) return hidden;

  const answerValues = new Map<number, string[]>();
  for (const [questionId, answer] of answersByQuestionId) {
    answerValues.set(questionId, extractAnswerValues(answer));
  }

  for (const questionId of orderedQuestionIds) {
    if (!isQuestionVisible(logicByQuestionId.get(questionId), answerValues, hidden)) {
      hidden.add(questionId);
    }
  }
  return hidden;
}
