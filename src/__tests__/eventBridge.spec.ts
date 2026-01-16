import pino, { Logger } from 'pino';
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import { mockClient } from "aws-sdk-client-mock";
import { putEvent } from "../eventBridge";

const ebMock = mockClient(EventBridgeClient);

const mockLogger: Logger = pino({ level: 'silent' });
const mockBusName = 'test-bus';

describe("SNS Module", () => {
  beforeEach(() => {
    ebMock.reset();
  });

  describe("publish", () => {
    const testDetailType = "test-event";
    const testSource = "testing";
    const testDetail = { key: "value" };

    const testEventId = '843gt38t-45gt425qgt-354gt4gt4tg-345gt45gt';

    it("should successfully put an event", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result).toEqual({
        status: 200,
        message: "Ok",
        eventId: testEventId,
      });
    });

    it("should successfully publish an object message", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result).toEqual({
        status: 200,
        message: "Ok",
        eventId: testEventId,
      });
    });

    it("should handle successful response with status code 200", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(200);
      expect(result.message).toBe("Ok");
      expect(result.eventId).toBe(testEventId);
    });

    it("should handle response with error status code", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 400,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(400);
      expect(result.message).toBe("Failure");
      expect(result.eventId).toBe(testEventId);
    });

    it("should handle response with status code 500", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 500,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.eventId).toBe(testEventId);
    });

    it("should handle response with missing metadata", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {},
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(500);
      expect(result.message).toBe("Failure");
      expect(result.eventId).toBe(testEventId);
    });

    it("should verify PublishCommand is called with correct parameters", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 200,
        },
      });

      await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(ebMock.calls()).toHaveLength(1);
      const call = ebMock.call(0);
      expect(call.args[0].input).toEqual({
        Entries: [{
          Source: testSource,
          DetailType: testDetailType,
          Detail: JSON.stringify(testDetail),
          EventBusName: 'test-bus',
        }]
      });
    });

    it("should handle boundary status code 300", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 300,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(300);
      expect(result.message).toBe("Ok");
      expect(result.eventId).toBe(testEventId);
    });

    it("should handle boundary status code 301", async () => {
      ebMock.on(PutEventsCommand).resolves({
        Entries: [{ EventId: testEventId }],
        $metadata: {
          httpStatusCode: 301,
        },
      });

      const result = await putEvent(
        mockLogger,
        mockBusName,
        testSource,
        testDetailType,
        testDetail
      );

      expect(result.status).toBe(301);
      expect(result.message).toBe("Failure");
      expect(result.eventId).toBe(testEventId);
    });
  });
});
