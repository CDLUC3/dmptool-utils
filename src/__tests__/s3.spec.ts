const mockSignedURLCommand = jest.fn();

jest.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: mockSignedURLCommand,
}));

import pino, { Logger } from 'pino';
import { Readable } from "stream";
import { mockClient } from "aws-sdk-client-mock";
import { sdkStreamMixin } from "@aws-sdk/util-stream-node";
import {
  listObjects,
  getObject,
  getPresignedURL,
  putObject
} from "../s3";
import {
  GetObjectCommand,
  GetObjectCommandOutput,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  PutObjectCommand,
  PutObjectCommandOutput,
  S3Client
} from "@aws-sdk/client-s3";

const mockLogger: Logger = pino({ level: 'silent' });
const s3Mock = mockClient(S3Client);

beforeEach(() => {
  s3Mock.reset();
})

describe('listObjects', () => {
  it('raises errors', async () => {
    s3Mock.on(ListObjectsV2Command).rejects(new Error('Test S3 error'));

    await expect(listObjects(mockLogger,'TestBucket', '/files')).rejects.toThrow('Test S3 error');
  });

  it('it returns an empty array if no bucket is specified', async () => {
    expect(await listObjects(mockLogger, '', '/files')).toEqual([]);
  });

  it('it returns the list of objects', async () => {
    const mockDate = new Date('2023-01-01T12:00:00.000Z');
    const items: ListObjectsV2CommandOutput = {
      $metadata: {
        httpStatusCode: 200,
      },
      Contents: [
        { Key: 'Test1', Size: 1234, LastModified: mockDate },
        { Key: 'Test2', Size: 12345, LastModified: mockDate }
      ]
    };
    s3Mock.on(ListObjectsV2Command).resolves(items);

    expect(await listObjects(mockLogger, 'TestBucket', '/files')).toEqual([
      { key: 'Test1', size: 1234, lastModified: mockDate },
      { key: 'Test2', size: 12345, lastModified: mockDate }
    ]);
  });
});

describe('getObject', () => {
  it('raises errors', async () => {
    s3Mock.on(GetObjectCommand).rejects(new Error('Test S3 error'));

    await expect(getObject(mockLogger, 'TestBucket', '/files')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await getObject(mockLogger, '', '/files')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await getObject(mockLogger, 'Test', '  ')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const content = '[{"key":"Test1"},{"key":"Test2","size":12345}]';
    const stream = new Readable();
    stream.push(content);
    stream.push(null);

    const sdkStream = sdkStreamMixin(stream);
    s3Mock.on(GetObjectCommand).resolves({
      $metadata: {
        httpStatusCode: 200,
      },
      Body: sdkStream,
    });

    const resp: GetObjectCommandOutput | undefined = await getObject(mockLogger, 'TestBucket', '/files');
    const payload = await resp?.Body?.transformToString();
    await expect(payload).toEqual(content);
  });
});

describe('putObject', () => {
  it('raises errors', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('Test S3 error'));
    await expect(putObject(mockLogger, 'TestBucket', '/files', '12345')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await putObject(mockLogger, '', '/files', '12345')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await putObject(mockLogger, 'Test', '  ', '12345')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const items: PutObjectCommandOutput = {
      $metadata: {
        httpStatusCode: 201,
      },
    };
    s3Mock.on(PutObjectCommand).resolves(items);

    expect(await putObject(mockLogger, 'TestBucket', '/files', '12345')).toEqual(items);
  });
});

describe('getPresignedURL', () => {
  it('raises errors', async () => {
    s3Mock.on(GetObjectCommand).rejects(new Error('Test S3 error'));
    mockSignedURLCommand.mockImplementation(() => { throw new Error('Test Signer error') });

    await expect(getPresignedURL(mockLogger, 'TestBucket', '/files')).rejects.toThrow('Test Signer error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await getPresignedURL(mockLogger, '', '/files')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await getPresignedURL(mockLogger, 'Test', '  ')).toEqual(undefined);
  });

  it('it returns the presigned URL', async () => {
    const key = '/tests/file.json';
    const presignedURL = 'http://testing.example.com/file/12345abcdefg';
    mockSignedURLCommand.mockResolvedValue(presignedURL);

    expect(await getPresignedURL(mockLogger, 'TestBucket', key)).toEqual(presignedURL);
  });
});
