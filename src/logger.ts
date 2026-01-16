import pino, { Logger } from 'pino';
import { pinoLambdaDestination } from 'pino-lambda';
import { ecsFormat } from '@elastic/ecs-pino-format';
import { Writable } from "node:stream";

// The available log levels
export enum LogLevelEnum {
  TRACE = 'trace',
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error',
  FATAL = 'fatal',
}

/**
 * Initialize a Pino logger with ECS formatting for the Lambda function.
 *
 * @param lambdaName Name of the function, used as the module name in the logs
 * @param logLevel Log level to use, defaults to 'LogLevelEnum.INFO'
 * @returns A Pino logger instance
 */
export const initializeLogger = (
  lambdaName: string,
  logLevel: LogLevelEnum = LogLevelEnum.INFO
): Logger => {
  const destination: Writable = pinoLambdaDestination();
  const logger: Logger = pino(
    {
      // Set the minimum log level
      level: logLevel || LogLevelEnum.INFO,
      // Format the log for OpenSearch using Elastic Common Schema
      ...ecsFormat
    },
    destination
  );

  // Define a standardized module name
  logger.child({ module: lambdaName });
  return logger;
}
