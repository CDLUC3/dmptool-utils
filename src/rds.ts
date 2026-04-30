import { ResultSetHeader } from 'mysql2';
import * as mysql from 'mysql2/promise';
import { formatISO9075, isDate } from "date-fns";
import { Connection } from "mysql2/promise";
import { Logger } from 'pino';

export interface ConnectionParams {
  logger: Logger,
  host: string,
  port: number,
  user: string,
  password: string,
  database: string,
}

/**
 * Connect to the MySQL database and run a query.
 *
 * @param params Connection parameters to use for the connection.
 * @returns A new connection to the MySQL database.
 */
const createConnection = async (
  params: ConnectionParams,
): Promise<mysql.Connection> => {
  const { host, port, user, password, database } = params;
  return mysql.createConnection({
    host,
    port,
    user,
    password,
    database,
    multipleStatements: false,
    namedPlaceholders: true
  });
};

/**
 * Process the parameters for a SQL query.
 *
 * @param params the parameters to process
 * @returns the processed parameters
 */
const processParams = (
  params: Record<string, any> | any[]
): Record<string, any> | any[] => {
  // mysql2 already handles issues with SQL injection by escaping/preparation automatically.
  // We just ensure:
  // - undefined becomes null
  // - JSON arrays/objects are stringified
  // - Dates are formatted as strings.
  const processValue = (val: any) => {
    if (val === null || val === undefined) return null;
    // Stringify Objects/Arrays for JSON columns, but ignore Date objects
    if (typeof val === 'object' && !(val instanceof Date)) {
      return JSON.stringify(val);
    }
    if (val instanceof Date || (typeof val === 'string' && isDate(val))) return formatISO9075(val);

    return val;
  };

  if (Array.isArray(params)) {
    return params.map(processValue);
  } else if (params && typeof params === 'object') {
    return Object.fromEntries(
      Object.entries(params).map(([k, v]) => [k, processValue(v)])
    );
  }

  return params;
};

/**
 * Function to run a SQL query against the MySQL database.
 *
 * @param query the SQL query to run
 * @param connectionParams the connection parameters to use for the connection
 * @param params the parameters to use in the query
 * @returns the results of the query
 */
// Runs the provided SQL query and returns the results
export const queryTable = async (
  connectionParams: ConnectionParams,
  query: string,
  params: any = []
): Promise<{ results: any[], fields: any[] }> => {
  try {
    if (!connectionParams || !connectionParams || !query || query.trim() === '') {
      throw new Error('Missing connectionParameters or query');
    }

    const connection: Connection = await createConnection(connectionParams);

    // Remove all tabs and new lines
    const sql: string = query.split(/[\s\t\n]+/).join(' ');
    // Prepare the values for the query
    const vals: Record<string, any> | any[] = processParams(params);

    // Run the query and then close the connection
    connectionParams.logger.debug({ query, params }, 'Running MySQL query');
    const [results, fields] = await connection.query(sql, vals);
    await connection.end();

    return { results: results as any[], fields: fields as any[] };
  } catch (error) {
    connectionParams.logger.fatal({ query, params, error }, 'Error running MySQL query');
    return { results: [], fields: [] };
  }
};

/**
 * Execute a SQL statement against the MySQL database.
 *
 * @param connectionParams Connection parameters to use for the connection.
 * @param query The SQL query to execute.
 * @param params The parameters to use in the query.
 * @returns The results of the query.
 */
export const executeTable = async (
  connectionParams: ConnectionParams,
  query: string,
  params: Record<string, any> = {}
): Promise<ResultSetHeader | null> => {
  try {
    if (!connectionParams || !query || query.trim() === '') {
      throw new Error('Missing connectionParameters or query');
    }

    const connection: Connection = await createConnection(connectionParams);
    const vals: Record<string, any> | any[] = processParams(params);

    connectionParams.logger.debug({ query, params: vals }, 'Executing MySQL statement');

    // We only care about the first element [results]
    const [result] = await connection.execute(query, vals);
    await connection.end();

    return result as ResultSetHeader;
  } catch (error) {
    connectionParams.logger.fatal({ query, params, error }, 'Error executing MySQL statement');
    return null;
  }
};
