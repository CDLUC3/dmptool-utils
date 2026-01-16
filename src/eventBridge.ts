import { Logger } from 'pino';
import {
  EventBridgeClient,
  PutEventsCommand,
  PutEventsCommandOutput
} from "@aws-sdk/client-eventbridge";
import {toErrorMessage} from "./general";

export interface PutEventResponse {
  status: number,
  message: string,
  eventId?: string
}

/**
 * Publishes an event to EventBridge.
 *
 * @param logger The logger to use for logging.
 * @param source The name of the caller (e.g. the Lambda Function or Application Function)
 * @param detailType The type of event (resources typically watch for specific types of events)
 * @param detail The payload of the event (will be accessible to the invoked resource)
 */
export const putEvent = async (
  logger: Logger,
  busName: string,
  source: string,
  detailType: string,
  details: Record<string, unknown>,
  region = 'us-west-2'
): Promise<PutEventResponse> => {
  let errMsg = '';

  if (logger && busName) {
    // Create a new EventBridge client instance
    const client = new EventBridgeClient({ region });

    logger.debug({ busName, source, detailType, details }, 'Publishing event');

    try {
      // Publish the event
      const response: PutEventsCommandOutput = await client.send(
        new PutEventsCommand({
          Entries: [
            {
              EventBusName: busName,
              Detail: details ? JSON.stringify(details) : undefined,
              DetailType: detailType,
              Source: source,
            },
          ],
        }),
      );

      if (response) {
        const statusCode: number = response.$metadata?.httpStatusCode ?? 500;
        // We got a response, so return it.
        return {
          status: statusCode,
          message: statusCode >= 200 && statusCode <= 300 ? "Ok" : "Failure",
          eventId: response.Entries?.[0]?.EventId
        };
      } else {
        logger.error({ busName, source, detailType, details }, 'No response from EventBridge');
        errMsg = 'No response from EventBridge';
      }
    } catch (error) {
      logger.fatal({ busName, source, detailType, details, error }, 'Error publishing event');
      errMsg = `Error publishing event: ${toErrorMessage(error)}`;
    }
  } else {
    errMsg = 'Missing logger or busName args!';
  }

  // The busName was not available or the response was undefined
  return {
    status: 500,
    message: errMsg,
    eventId: undefined
  };
};
