import {
  EventBridgeClient,
  PutEventsCommand,
  PutEventsCommandOutput
} from "@aws-sdk/client-eventbridge";

export interface PutEventResponse {
  status: number,
  message: string,
  eventId?: string
}

/**
 * Publishes an event to EventBridge.
 *
 * @param source The name of the caller (e.g. the Lambda Function or Application Function)
 * @param detailType The type of event (resources typically watch for specific types of events)
 * @param detail The payload of the event (will be accessible to the invoked resource)
 */
export const putEvent = async (
  source: string,
  detailType: string,
  details: Record<string, unknown>,
): Promise<PutEventResponse> => {
  const busName = process.env.EVENTBRIDGE_BUS_NAME;

  if (busName) {
    // Create a new EventBridge client instance
    const client = new EventBridgeClient({
      region: process.env.AWS_REGION ?? "us-west-2",

    });

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
    }
  }

  // The busName was not available or the response was undefined
  return {
    status: 500,
    message: `Failure: ${busName ? 'Unable to put event.' : 'Missing EVENTBRIDGE_BUS_NAME variable!'}`,
    eventId: undefined
  };
};
