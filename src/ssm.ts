import { EnvironmentEnum, isNullOrUndefined } from "./general";
import { Logger } from 'pino';
import {
  SSMClient,
  GetParameterCommand,
  GetParameterCommandOutput
} from "@aws-sdk/client-ssm";

/**
 * Retrieve a variable from the SSM Parameter store.
 *
 * @param logger The logger to use for logging.
 * @param key The name of the variable to retrieve.
 * @param env The environment to retrieve the variable from. Defaults to `EnvironmentEnum.DEV`.
 * @returns The value of the variable, or undefined if the variable could not be found.
 * @throws
 */
export const getSSMParameter = async (
  logger: Logger,
  key: string,
  env: EnvironmentEnum = EnvironmentEnum.DEV
): Promise<string | undefined> => {
  if (logger && key && key.trim() !== '') {
    // Create an SSM client
    const client = new SSMClient();

    const keyPrefix = `/uc3/dmp/tool/${env.toLowerCase()}/`;
    logger.debug(`Fetching parameter ${keyPrefix}${key}`);

    try {
      const command: GetParameterCommand = new GetParameterCommand({
        Name: `${keyPrefix}${key}`,
        WithDecryption: true
      });

      const response: GetParameterCommandOutput = await client.send(command);

      if (!isNullOrUndefined(response) && !isNullOrUndefined(response.Parameter)) {
        return response.Parameter?.Value;
      }
      logger.warn(`Parameter ${keyPrefix}${key} not found.`);
    } catch (error) {
      logger.fatal(`Error fetching parameter ${keyPrefix}${key}: ${error}`);
    }
  }
  return undefined;
};
