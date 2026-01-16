import { Logger } from 'pino';
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  GetObjectCommandOutput,
  PutObjectCommand,
  PutObjectCommandOutput,
  ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";

export interface DMPToolListObjectsOutput {
  key: string;
  lastModified: Date;
  size: number;
}

/**
 * List the contents of a bucket that match a key prefix.
 *
 * @param logger The logger to use for logging.
 * @param bucket The name of the bucket to list.
 * @param keyPrefix The prefix to filter the results by.
 * @param region The region to generate the URL in. Defaults to 'us-west-2'.
 * @returns A list of objects that match the key prefix, or undefined if the
 * bucket or key prefix are invalid.
 */
export const listObjects = async (
  logger: Logger,
  bucket: string,
  keyPrefix: string,
  region = 'us-west-2'
): Promise<DMPToolListObjectsOutput[] | []> => {
  if (logger && bucket && bucket.trim() !== '') {
    const s3Client = new S3Client({ region });

    try {
      const listObjectsCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: keyPrefix
      });
      logger.debug({ bucket, keyPrefix }, 'Listing objects in bucket');

      const response: ListObjectsV2CommandOutput = await s3Client.send(listObjectsCommand);

      if (Array.isArray(response.Contents) && response.Contents.length > 0) {
        const objects: DMPToolListObjectsOutput[] = [];
        for (const obj of response.Contents) {
          if (obj && obj.Key && obj.LastModified && obj.Size) {
            objects.push({
              key: obj.Key,
              lastModified: obj.LastModified,
              size: obj.Size
            });
          }
        }
        logger.debug({ objects }, 'Found objects in bucket');
        return objects;
      }
    } catch (error) {
      logger.fatal({ bucket, error }, 'Error listing objects in bucket');
      throw error;
    }
  }
  return [];
}

/**
 * Get an object from the specified bucket.
 *
 * @param logger The logger to use for logging.
 * @param bucket The name of the bucket to get the object from.
 * @param key The key of the object to get.
 * @param region The region to generate the URL in. Defaults to 'us-west-2'.
 * @returns The object, or undefined if the bucket or key are invalid.
 */
export const getObject = async (
  logger: Logger,
  bucket: string,
  key: string,
  region = 'us-west-2'
): Promise<GetObjectCommandOutput | undefined> => {
  if (logger && bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    try {
      const s3Client = new S3Client({ region });

      const command = new GetObjectCommand({ Bucket: bucket, Key: key });
      logger.debug({ bucket, key }, 'Getting object from bucket');
      return await s3Client.send(command);
    } catch (error) {
      logger.fatal({ bucket, key, error }, 'Error getting object from bucket');
      throw error;
    }
  }
  return undefined;
}

/**
 * Put an object into the specified bucket.
 *
 * @param logger The logger to use for logging.
 * @param bucket The name of the bucket to put the object into.
 * @param key The key of the object to put.
 * @param body The object to put.
 * @param contentType The content type of the object.
 * @param contentEncoding The content encoding of the object.
 * @param region The region to generate the URL in. Defaults to 'us-west-2'.
 * @returns The response from the S3 putObject operation, or undefined if the
 * bucket or key are invalid.
 */
export const putObject = async (
  logger: Logger,
  bucket: string,
  key: string,
  body: any,
  contentType = 'application/json',
  contentEncoding = 'utf-8',
  region = 'us-west-2'
): Promise<PutObjectCommandOutput | undefined> => {
  if (logger && bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    try {
      const s3Client = new S3Client({ region });

      const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
        ContentEncoding: contentEncoding,
      });
      logger.debug({ bucket, key }, 'Putting object into bucket');
      return await s3Client.send(command);
    } catch (error) {
      logger.fatal({ bucket, key, error }, 'Error putting object into bucket');
      throw error;
    }
  }
  return undefined;
}

/**
 * Generate a Pre-signed URL for an S3 object.
 *
 * @param logger The logger to use for logging.
 * @param bucket The name of the bucket to generate the URL for.
 * @param key The key of the object to generate the URL for.
 * @param usePutMethod Whether to use the PUT method for the URL. Defaults to false.
 * @param region The region to generate the URL in. Defaults to 'us-west-2'.
 * @returns The Pre-signed URL, or undefined if the bucket or key are invalid.
 */
export const getPresignedURL = async (
  logger: Logger,
  bucket: string,
  key: string,
  usePutMethod = false,
  region = 'us-west-2'
): Promise<string | undefined> => {
  if (logger && bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const s3Client = new S3Client({ region });

    const params = { Bucket: bucket, Key: key };
    try {
      const command: GetObjectCommand | PutObjectCommand = usePutMethod
        ? new PutObjectCommand(params)
        : new GetObjectCommand(params);
      logger.debug({ ...params, usePutMethod }, 'Generating presigned URL');
      return await getSignedUrl(s3Client, command, { expiresIn: 900 });;
    } catch (error) {
      logger.fatal({ ...params, usePutMethod, error }, 'Error generating a presigned URL');
      throw error;
    }
  }
  return undefined;
}
