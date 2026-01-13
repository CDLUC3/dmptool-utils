import {
  SSMClient,
  GetParameterCommand,
  GetParameterCommandOutput
} from "@aws-sdk/client-ssm";
import { isNullOrUndefined } from "./general";

// Create an SSM client
const client = new SSMClient();

const ENV = process.env.NODE_ENV === 'production'
  ? 'prd' : (process.env.NODE_ENV === 'staging'
    ? 'stg' : 'dev');

const KEY_PREFIX = `/uc3/dmp/tool/${ENV}/`;

/**
 * Retrieve a variable from the SSM Parameter store.
 *
 * @param key The name of the variable to retrieve.
 * @returns The value of the variable, or undefined if the variable could not be found.
 * @throws
 */
export const getSSMParameter = async (
  key: string
): Promise<string | undefined> => {
  if (key && key.trim() !== '') {
    const command: GetParameterCommand = new GetParameterCommand({
      Name: `${KEY_PREFIX}${key}`,
      WithDecryption: true
    });
    const response: GetParameterCommandOutput = await client.send(command);

    if (!isNullOrUndefined(response) && !isNullOrUndefined(response.Parameter)) {
      return response.Parameter?.Value;
    }
  }
  return undefined;
};
