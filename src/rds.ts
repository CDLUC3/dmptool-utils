import * as mysql from 'mysql2/promise';
import { formatISO9075, isDate } from "date-fns";
import { Connection } from "mysql2/promise";

/**
 * Connect to the MySQL database and run a query.
 * @returns A new connection to the MySQL database.
 */
const createConnection = async (): Promise<mysql.Connection> => {
  const args = {
    host: process.env.RDS_HOST ?? 'localhost',
    port: process.env.RDS_PORT ? Number.parseInt(process.env.RDS_PORT) : 3306,
    user: process.env.RDS_USER ?? 'root',
    password: process.env.RDS_PASSWORD ?? 'password',
    database: process.env.RDS_DATABASE ?? 'dmp',
    multipleStatements: false,
  }

  return mysql.createConnection(args);
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
 * @param params the parameters to use in the query
 * @returns the results of the query
 */
// Runs the provided SQL query and returns the results
export const queryTable = async (
  query: string,
  params: any[] = []
): Promise<{ results: any[], fields: any[] }> => {
  const connection: Connection = await createConnection();

  // Remove all tabs and new lines
  const sql: string = query.split(/[\s\t\n]+/).join(' ');
  // Prepare the values for the query
  const vals: any[] = params.map((val: any) => prepareValue(val, typeof val));

  // Run the query and then close the connection
  const [results, fields] = await connection.query(sql, vals);
  await connection.end();

  return { results: results as any[], fields: fields as any[] };
};
