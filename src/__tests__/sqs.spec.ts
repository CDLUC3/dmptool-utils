import pino, { Logger } from 'pino';
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { mockClient } from "aws-sdk-client-mock";
import { sendMessage } from "../sqs";

const sqsMock = mockClient(SQSClient);

const mockLogger: Logger = pino({ level: 'silent' });
const mockQueueURL = 'https://example.com/test-bus';

describe("SQS Module", () => {
  beforeEach(() => {
    sqsMock.reset();
  });

  describe("sendMessage", () => {
    const testDetailType = "test-message";
    const testSource = "testing";
    const testDetail = { key: "value" };

    const testMessageId = '843gt38t-45gt425qgt-354gt4gt4tg-345gt45gt';

    it("should successfully send a message", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result).toEqual({
        status: 200,
        message: "Ok",
        messageId: testMessageId,
      });
    });

    it("should handle successful response with status code 200", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(200);
      expect(result.message).toBe("Ok");
      expect(result.messageId).toBe(testMessageId);
    });

    it("should handle response with error status code", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 400,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(400);
      expect(result.message).toBe("Failure");
      expect(result.messageId).toBe(testMessageId);
    });

    it("should handle response with status code 500", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 500,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.messageId).toBe(testMessageId);
    });

    it("should handle response with missing metadata", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {},
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.messageId).toBe(testMessageId);
    });

    it("should verify SendMessage is called with correct parameters", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 200,
        },
      });

      await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(sqsMock.calls()).toHaveLength(1);
      const call = sqsMock.call(0);
      expect(call.args[0].input).toEqual({
        QueueUrl: mockQueueURL,
        MessageBody: JSON.stringify({
          ...testDetail,
          source: testSource,
        }),
        MessageAttributes: {
          "DetailType": {
            "DataType": "String",
            "StringValue": testDetailType
          }
        }
      });
    });

    it("should handle boundary status code 300", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 300,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(300);
      expect(result.message).toBe("Ok");
      expect(result.messageId).toBe(testMessageId);
    });

    it("should handle boundary status code 301", async () => {
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: testMessageId,
        $metadata: {
          httpStatusCode: 301,
        },
      });

      const result = await sendMessage(
        mockLogger,
        mockQueueURL,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(301);
      expect(result.message).toBe("Failure");
      expect(result.messageId).toBe(testMessageId);
    });
  });
});
