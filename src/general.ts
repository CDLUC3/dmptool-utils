/**
 * Convert an error to a string
 *
 * @param error the error to convert
 * @returns the error message
 */
export const toErrorMessage = (
  error: unknown
): string => {
  if (error instanceof Error) return error.message;
  return String(error);
}

/**
 * Convert http to https
 *
 * @param input a URL with the protocol
 * @returns a URL with the protocol converted to https
 */

export const normaliseHttpProtocol = (
  input: string | null
): string | null => {
  if (!input) {
    return null;
  }
  return input.trim().replace(/^http:\/\//, 'https://');
}

/**
 *Function to return the current date.
 *
 * @returns The current date as YYYY-MM-DD.
 */
// Function to return the current date as "YYYY-MM-DD"
export const currentDateAsString = () => {
  const now = new Date();

  // Extract components
  const year = now.getFullYear();
  // Months are 0-indexed so need to add 1 here
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');

  // Format as YYYY-MM-DD
  return `${year}-${month}-${day}`;
};

/**
 * Function to convert a MySQL DB date/time (in ISO `YYY-MM-DDThh:mm:ddZ` format to RFC3339 format
 *
 * @param dateTime the MySQL DB date/time to convert
 * @returns the converted date/time in RFC3339 format
 */
export const convertMySQLDateTimeToRFC3339 = (
  dateTime: string | Date | null | undefined
): string | null => {
  if (dateTime) {
    if (typeof dateTime !== 'string') return (dateTime as Date).toISOString();

    const newDate = (dateTime as string)?.trim()?.replace(' ', 'T')
    return newDate.includes('T')
      ? (newDate.endsWith('Z') ? newDate : newDate + 'Z')
      : newDate + 'T00:00:00Z';
  }
  return null;
}

/**
 * Generate a random hex code
 *
 * @param size the size of the hex code to generate
 * @returns a random hex code
 */
export const randomHex = (size: number): string => {
  return [...Array(size)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
}

/**
 * Function to check if a value is null or undefined.
 *
 * @param val
 * @returns True if the value is null or undefined, false otherwise.
 */
export const isNullOrUndefined = (val: unknown): boolean => {
  return val === null || val === undefined;
}

/**
 * Remove null and undefined values from an object or array.
 *
 * @param obj An object (may contain nested objects and arrays)
 * @returns The object with null and undefined values removed.
 */
export const removeNullAndUndefinedFromObject = (obj: any): any =>  {
  if (Array.isArray(obj)) {
    return obj.map(removeNullAndUndefinedFromObject).filter(v => !isNullOrUndefined(v));
  } else if (!isNullOrUndefined(obj) && typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj)
        .map(([k, v]) => [k, removeNullAndUndefinedFromObject(v)])
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        .filter(([_, v]) => !isNullOrUndefined(v))
    );
  }
  return obj;
}

/**
 * Function to test for equality between two objects, arrays or primitives
 *
 * @param a An object (may contain nested objects and arrays)
 * @param b Another object (may contain nested objects and arrays)
 * @returns True if the objects are equal, false otherwise
 */
export const areEqual = (a: any, b: any): boolean => {
  // Check for strict equality (handles primitives and same references)
  if (a === b) return true;

  // If either is not an object (or is null), they can't be equal
  if (typeof a !== 'object' || a === null || typeof b !== 'object' || b === null) {
    return false;
  }

  // If one is an array and the other isn't, they aren't equal
  if (Array.isArray(a) !== Array.isArray(b)) return false;

  // Handle Arrays specifically
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((item, index) => areEqual(item, b[index]));
  }

  // Handle Objects
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);

  // If the number of properties is different, they aren't equal'
  if (keysA.length !== keysB.length) return false;

  // Check each property for equality
  for (const key of keysA) {
    if (!keysB.includes(key) || !areEqual(a[key], b[key])) {
      return false;
    }
  }

  return true;
};
