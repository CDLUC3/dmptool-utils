import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";
import { mockClient } from "aws-sdk-client-mock";
import { publishMessage } from "../sns";

const snsMock = mockClient(SNSClient);

describe("SNS Module", () => {
  beforeEach(() => {
    snsMock.reset();
  });

  describe("publish", () => {
    const testTopicArn = "arn:aws:sns:us-west-2:123456789012:test-topic";
    const testMessage = "Test message";
    const testMessageId = "test-message-id-123";

    it("should successfully publish a string message", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result).toEqual({
        status: 200,
        message: "Ok",
        snsMessageId: testMessageId,
      });
    });

    it("should successfully publish an object message", async () => {
      const objectMessage = {key: "value", nested: {data: "test"}};

      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await publish(objectMessage, testTopicArn);

      expect(result).toEqual({
        status: 200,
        message: "Ok",
        snsMessageId: testMessageId,
      });
    });

    it("should handle successful response with status code 200", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(200);
      expect(result.message).toBe("Ok");
      expect(result.snsMessageId).toBe(testMessageId);
    });

    it("should handle response with error status code", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 400,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(400);
      expect(result.message).toBe("Failure");
      expect(result.snsMessageId).toBe(testMessageId);
    });

    it("should handle response with status code 500", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 500,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.snsMessageId).toBe(testMessageId);
    });

    it("should handle response with missing metadata", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {},
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.snsMessageId).toBe(testMessageId);
    });

    it("should verify PublishCommand is called with correct parameters", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      await publish(testMessage, testTopicArn);

      expect(snsMock.calls()).toHaveLength(1);
      const call = snsMock.call(0);
      expect(call.args[0].input).toEqual({
        Message: testMessage,
        TopicArn: testTopicArn,
      });
    });

    it("should handle boundary status code 300", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 300,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(300);
      expect(result.message).toBe("Ok");
      expect(result.snsMessageId).toBe(testMessageId);
    });

    it("should handle boundary status code 301", async () => {
      snsMock.on(PublishCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 301,
        },
      });

      const result = await publish(testMessage, testTopicArn);

      expect(result.status).toBe(301);
      expect(result.message).toBe("Failure");
      expect(result.snsMessageId).toBe(testMessageId);
    });
  });
});
