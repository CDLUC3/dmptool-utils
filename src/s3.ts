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

const s3Client = new S3Client({});

export interface DMPToolListObjectsOutput {
  key: string;
  lastModified: Date;
  size: number;
}

/**
 * List the contents of a bucket that match a key prefix.
 *
 * @param bucket The name of the bucket to list.
 * @param keyPrefix The prefix to filter the results by.
 * @returns A list of objects that match the key prefix, or undefined if the
 * bucket or key prefix are invalid.
 */
export const listObjects = async (
  bucket: string,
  keyPrefix: string
): Promise<DMPToolListObjectsOutput[] | []> => {
  if (bucket && bucket.trim() !== '') {
    const listObjectsCommand = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: keyPrefix
    });
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
      return objects;
    }
  }
  return [];
}

/**
 * Get an object from the specified bucket.
 *
 * @param bucket The name of the bucket to get the object from.
 * @param key The key of the object to get.
 * @returns The object, or undefined if the bucket or key are invalid.
 */
export const getObject = async (
  bucket: string,
  key: string
): Promise<GetObjectCommandOutput | undefined> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    return await s3Client.send(command);
  }
  return undefined;
}

/**
 * Put an object into the specified bucket.
 *
 * @param bucket The name of the bucket to put the object into.
 * @param key The key of the object to put.
 * @param body The object to put.
 * @param contentType The content type of the object.
 * @param contentEncoding The content encoding of the object.
 * @returns The response from the S3 putObject operation, or undefined if the
 * bucket or key are invalid.
 */
export const putObject = async (
  bucket: string,
  key: string,
  body: any,
  contentType = 'application/json',
  contentEncoding = 'utf-8'
): Promise<PutObjectCommandOutput | undefined> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
      ContentEncoding: contentEncoding,
    });
    return await s3Client.send(command);
  }
  return undefined;
}

/**
 * Generate a Pre-signed URL for an S3 object.
 *
 * @param bucket The name of the bucket to generate the URL for.
 * @param key The key of the object to generate the URL for.
 * @param usePutMethod Whether to use the PUT method for the URL. Defaults to false.
 * @returns The Pre-signed URL, or undefined if the bucket or key are invalid.
 */
export const getPresignedURL = async (
  bucket: string,
  key: string,
  usePutMethod = false
): Promise<string | undefined> => {
  if (bucket && key && bucket.trim() !== '' && key.trim() !== '') {
    const params = { Bucket: bucket, Key: key };
    const command: GetObjectCommand | PutObjectCommand = usePutMethod
      ? new PutObjectCommand(params)
      : new GetObjectCommand(params);
    return await getSignedUrl(s3Client, command, { expiresIn: 900 });;
  }
  return undefined;
}
