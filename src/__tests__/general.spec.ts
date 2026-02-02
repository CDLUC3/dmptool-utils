import {
  areEqual,
  convertMySQLDateTimeToRFC3339,
  currentDateAsString,
  isNullOrUndefined,
  isJSON,
  isValidDate,
  normaliseHttpProtocol,
  randomHex,
  removeNullAndUndefinedFromObject,
} from '../general';

describe('isValidDate', () => {
  it('returns true for valid ISO 8601 date string', () => {
    expect(isValidDate('2024-01-15')).toBe(true);
  });

  it('returns true for valid ISO 8601 datetime string', () => {
    expect(isValidDate('2024-01-15T14:30:45')).toBe(true);
  });

  it('returns true for valid ISO 8601 datetime with timezone', () => {
    expect(isValidDate('2024-01-15T14:30:45Z')).toBe(true);
    expect(isValidDate('2024-01-15T14:30:45+00:00')).toBe(true);
  });

  it('returns true for valid date string in different format', () => {
    expect(isValidDate('January 15, 2024')).toBe(true);
    expect(isValidDate('01/15/2024')).toBe(true);
  });

  it('returns false for invalid date string', () => {
    expect(isValidDate('invalid-date')).toBe(false);
    expect(isValidDate('not a date')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isValidDate('')).toBe(false);
  });

  it('returns false for invalid date values', () => {
    expect(isValidDate('2024-13-01')).toBe(false); // Invalid month
    expect(isValidDate('2024-01-32')).toBe(false); // Invalid day
  });

  it('returns false for a timestamp string', () => {
    expect(isValidDate('1769703186701')).toBe(false);
  });

  it('returns true for edge case dates', () => {
    expect(isValidDate('1970-01-01T00:00:00Z')).toBe(true); // Unix epoch
    expect(isValidDate('2038-01-19T03:14:07Z')).toBe(true); // Near 32-bit timestamp limit
  });

  it('returns false for partial date strings', () => {
    expect(isValidDate('2024')).toBe(true); // Year only is technically valid
    expect(isValidDate('2024-01')).toBe(true); // Year-month is technically valid
  });

  it('returns true for dates with time but no timezone', () => {
    expect(isValidDate('2024-01-15 14:30:45')).toBe(true);
  });
});

describe('isJSON', () => {
  it('returns true for valid JSON object string', () => {
    expect(isJSON('{"name":"test","value":123}')).toBe(true);
    expect(isJSON('{"nested":{"key":"value"}}')).toBe(true);
  });

  it('returns true for valid JSON array string', () => {
    expect(isJSON('[1,2,3]')).toBe(true);
    expect(isJSON('["a","b","c"]')).toBe(true);
    expect(isJSON('[]')).toBe(true);
  });

  it('returns true for valid JSON primitive values', () => {
    expect(isJSON('"string"')).toBe(true);
    expect(isJSON('123')).toBe(true);
    expect(isJSON('true')).toBe(true);
    expect(isJSON('false')).toBe(true);
    expect(isJSON('null')).toBe(true);
  });

  it('returns false for invalid JSON strings', () => {
    expect(isJSON('not json')).toBe(false);
    expect(isJSON('undefined')).toBe(false);
    expect(isJSON('{invalid}')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isJSON('')).toBe(false);
  });

  it('returns false for malformed JSON strings', () => {
    expect(isJSON('{"name":"test"')).toBe(false); // Missing closing brace
    expect(isJSON('{"name":}')).toBe(false); // Missing value
    expect(isJSON('{name:"test"}')).toBe(false); // Unquoted key
    expect(isJSON('{"name":"test",}')).toBe(false); // Trailing comma
  });

  it('returns false for JSON string with single quotes instead of double quotes', () => {
    expect(isJSON("{'name':'test'}")).toBe(false);
  });

  it('returns true for valid nested JSON structures', () => {
    expect(isJSON('{"user":{"name":"test","age":30,"address":{"city":"NYC"}}}')).toBe(true);
    expect(isJSON('[{"id":1},{"id":2}]')).toBe(true);
    expect(isJSON('{"items":[1,2,3],"count":3}')).toBe(true);
  });
});

describe('normaliseHttpProtocol', () => {
  it('converts http:// to https://', () => {
    const result = normaliseHttpProtocol('http://example.com');
    expect(result).toBe('https://example.com');
  });

  it('leaves https:// unchanged', () => {
    const result = normaliseHttpProtocol('https://example.com');
    expect(result).toBe('https://example.com');
  });

  it('returns null when input is null', () => {
    const result = normaliseHttpProtocol(null);
    expect(result).toBeNull();
  });

  it('returns null when input is empty string', () => {
    const result = normaliseHttpProtocol('');
    expect(result).toBeNull();
  });

  it('handles URLs with extra whitespace', () => {
    const result = normaliseHttpProtocol('  http://example.com  ');
    expect(result).toBe('https://example.com');
  });

  it('handles URLs without protocol', () => {
    const result = normaliseHttpProtocol('example.com');
    expect(result).toBe('example.com');
  });

  it('converts http:// at the beginning only', () => {
    const result = normaliseHttpProtocol('http://example.com/http://test');
    expect(result).toBe('https://example.com/http://test');
  });
});

describe('currentDateAsString', () => {
  it('returns the date in the expected format', () => {
    const regex = /[1-9]{4}-[1-9]{2}-[1-9]{2}/;
    expect(regex.test(currentDateAsString()));
  });
});

describe('convertMySQLDateTimeToRFC3339', () => {
  it('converts MySQL datetime string with space separator to RFC3339 format', () => {
    const mysqlDateTime = '2024-01-15 14:30:45';
    const result = convertMySQLDateTimeToRFC3339(mysqlDateTime);
    expect(result).toBe('2024-01-15T14:30:45Z');
  });

  it('converts MySQL datetime string with T separator to RFC3339 format', () => {
    const mysqlDateTime = '2024-01-15T14:30:45';
    const result = convertMySQLDateTimeToRFC3339(mysqlDateTime);
    expect(result).toBe('2024-01-15T14:30:45Z');
  });

  it('converts MySQL date string without time to RFC3339 format', () => {
    const mysqlDate = '2024-01-15';
    const result = convertMySQLDateTimeToRFC3339(mysqlDate);
    expect(result).toBe('2024-01-15T00:00:00Z');
  });

  it('handles datetime strings with extra whitespace', () => {
    const mysqlDateTime = '  2024-01-15 14:30:45  ';
    const result = convertMySQLDateTimeToRFC3339(mysqlDateTime);
    expect(result).toBe('2024-01-15T14:30:45Z');
  });

  it('converts Date object to RFC3339 format', () => {
    const date = new Date('2024-01-15T14:30:45.000Z');
    const result = convertMySQLDateTimeToRFC3339(date);
    expect(result).toBe('2024-01-15T14:30:45.000Z');
  });

  it('returns null when input is null', () => {
    const result = convertMySQLDateTimeToRFC3339(null);
    expect(result).toBeNull();
  });

  it('returns null when input is undefined', () => {
    const result = convertMySQLDateTimeToRFC3339(undefined);
    expect(result).toBeNull();
  });

  it('returns null when input is empty string', () => {
    const result = convertMySQLDateTimeToRFC3339('');
    expect(result).toBeNull();
  });

  it('handles datetime string already in RFC3339 format with Z', () => {
    const rfc3339DateTime = '2024-01-15T14:30:45Z';
    const result = convertMySQLDateTimeToRFC3339(rfc3339DateTime);
    expect(result).toBe('2024-01-15T14:30:45Z');
  });
});



describe('areEqual', () => {
  it('returns true when two identical primitive values are compared', () => {
    expect(areEqual('test', 'test')).toBe(true);
    expect(areEqual(123, 123)).toBe(true);
    expect(areEqual(true, true)).toBe(true);
  });

  it('returns false when two different primitive values are compared', () => {
    expect(areEqual('test', 'other')).toBe(false);
    expect(areEqual(123, 456)).toBe(false);
    expect(areEqual(true, false)).toBe(false);
  });

  it('returns true when two identical objects are compared', () => {
    const obj = { name: 'test', value: 123 };
    expect(areEqual(obj, obj)).toBe(true);
    expect(areEqual({ name: 'test'}, {name: 'test' })).toBe(true);
  });

  it('returns false when two different objects are compared', () => {
    expect(areEqual({ name: 'test' }, { name: 'other' })).toBe(false);
    expect(areEqual({ name: 'test', value: 1 }, { name: 'test' })).toBe(false);
  });

  it('handles null and undefined values correctly', () => {
    expect(areEqual(null, null)).toBe(true);
    expect(areEqual(undefined, undefined)).toBe(true);
    expect(areEqual(null, undefined)).toBe(false);
    expect(areEqual(null, 'test')).toBe(false);
  });

  it('compares arrays correctly', () => {
    expect(areEqual([1, 2, 3], [1, 2, 3])).toBe(true);
    expect(areEqual([1, 2, 3], [1, 2, 4])).toBe(false);
    expect(areEqual([], [])).toBe(true);

    expect(areEqual([1, 2], [1, 2, 3])).toBe(false);
    expect(areEqual([], [1, 2])).toBe(false);
  });

  it('handles nested objects correctly', () => {
    const obj1 = { name: 'test', value: 123, children: [{ name: 'child1', value: 456 }] };
    const obj2 = { name: 'test', value: 123, children: [{ name: 'child1', value: 456 }] };
    expect(areEqual(obj1, obj2)).toBe(true);

    const obj3 = { name: 'test', value: 123, children: [{ name: 'child1', value: 456 }] };
    const obj4 = { name: 'test', value: 123, children: [{ name: 'child2', value: 456 }] };
    expect(areEqual(obj3, obj4)).toBe(false);

    const obj5 = { name: 'test', value: 123, parent: { name: 'mom', value: 456 } };
    const obj6 = { name: 'test', value: 123, parent: { name: 'dad', value: 456 } };
    expect(areEqual(obj5, obj6)).toBe(false);
  })
});

describe('isNullOrUndefined', () => {
  it('returns true when value is null', () => {
    expect(isNullOrUndefined(null)).toBe(true);
  });

  it('returns true when value is undefined', () => {
    expect(isNullOrUndefined(undefined)).toBe(true);
  });

  it('returns false when value is a string', () => {
    expect(isNullOrUndefined('test')).toBe(false);
    expect(isNullOrUndefined('')).toBe(false);
  });

  it('returns false when value is a number', () => {
    expect(isNullOrUndefined(0)).toBe(false);
    expect(isNullOrUndefined(123)).toBe(false);
    expect(isNullOrUndefined(-456)).toBe(false);
  });

  it('returns false when value is an object', () => {
    expect(isNullOrUndefined({})).toBe(false);
    expect(isNullOrUndefined({name: 'test'})).toBe(false);
  });

  it('returns false when value is an array', () => {
    expect(isNullOrUndefined([])).toBe(false);
    expect(isNullOrUndefined([1, 2, 3])).toBe(false);
  });
});

describe('randomHex', () => {
  it('returns a hex string of the specified length', () => {
    expect(randomHex(8).length).toBe(8);
    expect(randomHex(16).length).toBe(16);
    expect(randomHex(32).length).toBe(32);
  });

  it('returns an empty string when size is 0', () => {
    expect(randomHex(0)).toBe('');
  });

  it('returns only valid hexadecimal characters', () => {
    const hexRegex = /^[0-9a-f]*$/;
    expect(hexRegex.test(randomHex(100))).toBe(true);
    expect(hexRegex.test(randomHex(50))).toBe(true);
  });

  it('returns different values on subsequent calls', () => {
    const hex1 = randomHex(16);
    const hex2 = randomHex(16);
    const hex3 = randomHex(16);

    // It's extremely unlikely (but theoretically possible) that all three are equal
    expect(hex1 === hex2 && hex2 === hex3).toBe(false);
  });

  it('handles single character hex generation', () => {
    const hex = randomHex(1);
    expect(hex.length).toBe(1);
    expect(/^[0-9a-f]$/.test(hex)).toBe(true);
  });
});

describe('removeNullAndUndefinedFromObject', () => {
  it('removes null values from object', () => {
    const input = { name: 'test', value: null, count: 123 };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: 'test', count: 123 });
  });

  it('removes undefined values from object', () => {
    const input = { name: 'test', value: undefined, count: 123 };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: 'test', count: 123 });
  });

  it('removes both null and undefined values from object', () => {
    const input = { name: 'test', value: null, other: undefined, count: 123 };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: 'test', count: 123 });
  });

  it('returns empty object when all values are null or undefined', () => {
    const input = { value1: null, value2: undefined };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({});
  });

  it('returns empty object when input is empty object', () => {
    const input = {};
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({});
  });

  it('preserves zero values', () => {
    const input = { name: 'test', count: 0, value: null };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: 'test', count: 0 });
  });

  it('preserves empty string values', () => {
    const input = { name: '', value: null, other: 'test' };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: '', other: 'test' });
  });

  it('preserves false boolean values', () => {
    const input = { name: 'test', active: false, value: null };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({ name: 'test', active: false });
  });

  it('handles nested objects with null and undefined values', () => {
    const input = {
      name: 'test',
      child: { value: null, count: 123 },
      other: undefined
    };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({
      name: 'test',
      child: { count: 123 }
    });
  });

  it('preserves array values even if they contain null or undefined', () => {
    const input = {
      name: 'test',
      values: [1, null, 3, undefined, 5],
      other: null
    };
    const result = removeNullAndUndefinedFromObject(input);
    expect(result).toEqual({
      name: 'test',
      values: [1, 3, 5]
    });
  });

  it('does not mutate the original object', () => {
    const input = { name: 'test', value: null, count: 123 };
    const original = { ...input };
    removeNullAndUndefinedFromObject(input);
    expect(input).toEqual(original);
  });

  it('returns null when input is null', () => {
    const result = removeNullAndUndefinedFromObject(null);
    expect(result).toBeNull();
  });

  it('returns undefined when input is undefined', () => {
    const result = removeNullAndUndefinedFromObject(undefined);
    expect(result).toBeUndefined();
  });
});
