const mockSignedURLCommand = jest.fn();
const mockCreatePresignedPost = jest.fn();

jest.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: mockSignedURLCommand,
}));

jest.mock("@aws-sdk/s3-presigned-post", () => ({
  createPresignedPost: mockCreatePresignedPost,
}));

import pino, { Logger } from 'pino';
import { Readable } from "stream";
import { mockClient } from "aws-sdk-client-mock";
import { sdkStreamMixin } from "@smithy/util-stream";

import {
  listObjects,
  getObject,
  getPresignedURL,
  getPresignedURLForImageUpload,
  putObject,
  removeObject
} from "../s3";
import {
  DeleteObjectCommand,
  DeleteObjectCommandOutput,
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
  mockCreatePresignedPost.mockReset();
})

describe('listObjects', () => {
  it('raises errors', async () => {
    s3Mock.on(ListObjectsV2Command).rejects(new Error('Test S3 error'));

    await expect(listObjects(mockLogger, 'TestBucket', '/files')).rejects.toThrow('Test S3 error');
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

describe('removeObject', () => {
  it('raises errors', async () => {
    s3Mock.on(DeleteObjectCommand).rejects(new Error('Test S3 error'));

    await expect(removeObject(mockLogger, 'TestBucket', '/files')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await removeObject(mockLogger, '', '/files')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await removeObject(mockLogger, 'Test', '  ')).toEqual(undefined);
  });

  it('it removes the object', async () => {
    const items: DeleteObjectCommandOutput = {
      $metadata: {
        httpStatusCode: 204,
      },
    };
    s3Mock.on(DeleteObjectCommand).resolves(items);

    expect(await removeObject(mockLogger, 'TestBucket', '/files')).toEqual(items);
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

describe('getPresignedURLForImageUpload', () => {
  it('returns undefined when S3 throws an error', async () => {
    mockCreatePresignedPost.mockImplementation(() => { throw new Error('Test Presigned Post error') });

    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', '/images/photo.jpg', 'image/jpeg')).toEqual(undefined);
  });

  it('returns undefined if no logger is provided', async () => {
    expect(await getPresignedURLForImageUpload(null as any, 'TestBucket', '/images/photo.jpg', 'image/jpeg')).toEqual(undefined);
  });

  it('returns undefined if no bucket is specified', async () => {
    expect(await getPresignedURLForImageUpload(mockLogger, '', '/images/photo.jpg', 'image/jpeg')).toEqual(undefined);
  });

  it('returns undefined if bucket is only whitespace', async () => {
    expect(await getPresignedURLForImageUpload(mockLogger, '   ', '/images/photo.jpg', 'image/jpeg')).toEqual(undefined);
  });

  it('returns undefined if no key is specified', async () => {
    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', '', 'image/jpeg')).toEqual(undefined);
  });

  it('returns undefined if key is only whitespace', async () => {
    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', '   ', 'image/jpeg')).toEqual(undefined);
  });

  it('returns the presigned URL and fields for image upload', async () => {
    const key = '/images/photo.jpg';
    const presignedURL = 'http://testing.example.com/images/photo.jpg?X-Amz-Signature=abc123';
    const fields = { 'Content-Type': 'image/jpeg', bucket: 'TestBucket' };
    mockCreatePresignedPost.mockResolvedValue({ url: presignedURL, fields });

    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', key, 'image/jpeg')).toEqual({
      url: presignedURL,
      fields: JSON.stringify(fields),
    });
  });

  it('uses a custom region when provided', async () => {
    const key = '/images/photo.jpg';
    const presignedURL = 'http://testing.example.com/images/photo.jpg?X-Amz-Signature=abc123';
    const fields = { 'Content-Type': 'image/png' };
    mockCreatePresignedPost.mockResolvedValue({ url: presignedURL, fields });

    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', key, 'image/png', 'eu-west-1')).toEqual({
      url: presignedURL,
      fields: JSON.stringify(fields),
    });
  });

  it('uses the default region (us-west-2) when no region is provided', async () => {
    const key = '/images/photo.jpg';
    const presignedURL = 'http://testing.example.com/images/photo.jpg?X-Amz-Signature=def456';
    const fields = { 'Content-Type': 'image/jpeg' };
    mockCreatePresignedPost.mockResolvedValue({ url: presignedURL, fields });

    expect(await getPresignedURLForImageUpload(mockLogger, 'TestBucket', key, 'image/jpeg')).toEqual({
      url: presignedURL,
      fields: JSON.stringify(fields),
    });
  });
});

