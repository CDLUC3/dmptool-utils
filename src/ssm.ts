import { EnvironmentEnum, isNullOrUndefined } from "./general";
import { Logger } from 'pino';
import {
  SSMClient,
  GetParameterCommand,
  GetParameterCommandOutput
} from "@aws-sdk/client-ssm";

export interface SsmConnectionParams {
  logger: Logger;
  region: string;
  useTLS?: boolean;
  endpoint?: string;
}

/**
 * Retrieve a variable from the SSM Parameter store.
 *
 * @param connectionParams The connection parameters to use for the SSM client.
 * @param key The name of the variable to retrieve.
 * @param env The environment to retrieve the variable from. Defaults to `EnvironmentEnum.DEV`.
 * Should be false when running in a local development environment.
 * @returns The value of the variable, or undefined if the variable could not be found.
 * @throws
 */
export const getSSMParameter = async (
  connectionParams: SsmConnectionParams,
  key: string,
  env: EnvironmentEnum = EnvironmentEnum.DEV,
): Promise<string | undefined> => {
  if (connectionParams.logger && key && key.trim() !== '') {
    // Create an SSM client (use the endpoint if we are not using TLS - local dev)
    const client = connectionParams.useTLS
      ? new SSMClient()
      : new SSMClient(connectionParams);

    const keyPrefix = `/uc3/dmp/tool/${env.toLowerCase()}/`;
    connectionParams.logger.debug(`Fetching parameter ${keyPrefix}${key}`);

    try {
      const command: GetParameterCommand = new GetParameterCommand({
        Name: `${keyPrefix}${key}`,
        WithDecryption: true
      });

      const response: GetParameterCommandOutput = await client.send(command);

      if (!isNullOrUndefined(response) && !isNullOrUndefined(response.Parameter)) {
        return response.Parameter?.Value;
      }
      connectionParams.logger.warn(`Parameter ${keyPrefix}${key} not found.`);
    } catch (error) {
      connectionParams.logger.fatal(`Error fetching parameter ${keyPrefix}${key}: ${error}`);
    }
  }
  return undefined;
};
