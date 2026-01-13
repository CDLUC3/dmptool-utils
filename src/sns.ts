import {
  PublishCommand,
  PublishCommandOutput,
  SNSClient
} from "@aws-sdk/client-sns";

let snsClient: SNSClient;

export type publishResponse = {
  status: number,
  message: string,
  snsMessageId: string | undefined
}

/**
 * Generate a new SNS client.
 *
 * @returns Either the existing SNS client or generates a new one
 */
const getClient = (): SNSClient => {
  // If it's already been created, return it.
  if (snsClient) return snsClient;

  // Otherwise create a new client.
  const snsClient = new SNSClient({
    region: process.env.AWS_REGION ?? "us-west-2"
  });
  return snsClient;
}

/**
 * Publish a message to an SNS topic.
 *
 * @param {string | Record<string, any>} message - The message to send. Can be a plain string or an object
 *                                                 if you are using the `json` `MessageStructure`.
 * @param {string} topicArn - The ARN of the topic to which you would like to publish.
 * @returns {Promise<PublishCommandOutput>} - The response from the SNS publish operation.
 */
export const publishMessage = async (
  message: string | Record<string, any>,
  topicArn: string,
): Promise<publishResponse> => {
  const client = getClient();

  const response: PublishCommandOutput = await client.send(
    new PublishCommand({
      Message: message,
      TopicArn: topicArn,
    }),
  );

  if (response) {
    const statusCode: number = response.$metadata?.httpStatusCode ?? 500;
    // We got a response, so return it.
    return {
      status: statusCode,
      message: statusCode >= 200 && statusCode <= 300 ? "Ok" : "Failure",
      snsMessageId: response.MessageId
    }
  }

  // The response was undefined, so something went wrong.
  return {
    status: 500,
    message: "Failure",
    snsMessageId: undefined
  }
};
