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
    multipleStatements: false
  });
};

/**
 * Function to convert the incoming value into an appropriate type for insertion
 * into a SQL query.
 * @param val the incoming value to convert
 * @param type the type of the incoming value
 * @returns the converted value
 */
const prepareValue = (val: any, type: any): any => {
  if (val === null || val === undefined) {
    return null;
  }
  switch (type) {
    case 'number':
      return Number(val);
    case 'json':
      return JSON.stringify(val);
    case Object:
    case Array:
      return JSON.stringify(val);
    case 'boolean':
      return Boolean(val);
    default:
      if (isDate(val)) {
        const date = new Date(val).toISOString();
        return formatISO9075(date);

      } else if (Array.isArray(val)) {
        return JSON.stringify(val);

      } else {
        return String(val);
      }
  }
}

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
  params: any[] = []
): Promise<{ results: any[], fields: any[] }> => {
  try {
    if (!connectionParams || !connectionParams || !query || query.trim() === '') {
      throw new Error('Missing connectionParameters or query');
    }

    const connection: Connection = await createConnection(connectionParams);

    // Remove all tabs and new lines
    const sql: string = query.split(/[\s\t\n]+/).join(' ');
    // Prepare the values for the query
    const vals: any[] = params.map((val: any) => prepareValue(val, typeof val));

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
