import { Logger } from 'pino';
import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandOutput
} from "@aws-sdk/client-sqs";
import { toErrorMessage } from "./general";

export interface SendMessageResponse {
  status: number,
  message: string,
  messageId?: string
}

/**
 * Send an SQS message to the specified queue.
 *
 * @param logger The logger to use for logging.
 * @param queueURL The endpoint of the SQS queue to send the message to.
 * @param source The name of the caller (e.g. the Lambda Function or Application Function)
 * @param detailType The type of message
 * @param detail The payload of the message (will be accessible to the invoked resource)
 * @param region The region to publish the message in. Defaults to 'us-west-2'.
 * @returns A SendMessageResponse object containing the status code and message info.
 * @throws Error if there was an error sending the message
 */
export const sendMessage = async (
  logger: Logger,
  queueURL: string,
  source: string,
  detailType: string,
  details: Record<string, unknown>,
  region = 'us-west-2'
): Promise<SendMessageResponse> => {
  let errMsg = '';

  if (logger && queueURL) {
    // Create a new SQS client instance
    const client = new SQSClient({ region });

    logger.debug({ queueURL, source, detailType, details }, 'Sending message');

    try {
      // Send the message
      const response: SendMessageCommandOutput = await client.send(
        new SendMessageCommand({
          QueueUrl: queueURL,
          MessageBody: JSON.stringify({
            ...details,
            source
          }),
          MessageAttributes: {
            "DetailType": {
              "DataType": "String",
              "StringValue": detailType
            }
          }
        }),
      );

      if (response) {
        const statusCode: number = response.$metadata?.httpStatusCode ?? 500;
        // We got a response, so return it.
        return {
          status: statusCode,
          message: statusCode >= 200 && statusCode <= 300 ? "Ok" : "Failure",
          messageId: response.MessageId
        };
      } else {
        logger.error({ queueURL, source, detailType, details }, 'No response from SQS');
        errMsg = 'No response from SQS';
      }
    } catch (error) {
      logger.fatal({ queueURL, source, detailType, details, error }, 'Error sending message');
      errMsg = `Error sending message: ${toErrorMessage(error)}`;
    }
  } else {
    errMsg = 'Missing logger or queueURL args!';
  }

  // The SQS was not available or the response was undefined
  return {
    status: 500,
    message: errMsg,
    messageId: undefined
  };
};
