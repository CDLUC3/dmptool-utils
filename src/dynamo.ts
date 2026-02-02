import { Logger } from 'pino';
import http from "http";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import {
  AttributeValue,
  DeleteItemCommand,
  DynamoDBClient,
  DynamoDBClientConfig,
  PutItemCommand,
  QueryCommand,
  QueryCommandOutput,
  ScanCommand,
  ScanCommandOutput,
} from "@aws-sdk/client-dynamodb";
import {
  DMPToolDMPType,
  DMPToolExtensionType,
  RDACommonStandardDMPType
} from '@dmptool/types';
import {
  convertMySQLDateTimeToRFC3339,
  isNullOrUndefined,
  toErrorMessage
} from "./general";

const DMP_PK_PREFIX = 'DMP';
// VERSION records store the RDA Common Standard metadata
const DMP_VERSION_PREFIX = 'VERSION';
// EXTENSION records store the DMP Tool specific metadata
const DMP_EXTENSION_PREFIX = 'EXTENSION';
export const DMP_LATEST_VERSION = 'latest';
export const DMP_TOMBSTONE_VERSION = 'tombstone';

export interface DynamoConnectionParams {
  logger: Logger;
  region: string;
  tableName: string;
  endpoint?: string;
  maxAttempts: number;
}

// The list of properties that are extensions to the RDA Common Standard
const EXTENSION_KEYS: string[] = [
  'featured',
  'funding_opportunity',
  'funding_project',
  'narrative',
  'provenance',
  'privacy',
  'rda_schema_version',
  'registered',
  'research_domain',
  'research_facility',
  'status',
  'tombstoned',
  'version',
];

export interface DMPVersionType {
  dmpId: string;
  modified: string;
}

interface DynamoItemType {
  PK: string;
  SK: string;
}

type RDAInner = RDACommonStandardDMPType extends { dmp: infer T } ? T : any;
type DynamoVersionType = RDAInner & DynamoItemType;

type RDACommonStandardInnerType = RDACommonStandardDMPType extends { dmp: infer T } ? T : any;

type DynamoVersionItemType = RDACommonStandardInnerType & DynamoItemType;
type DynamoExtensionItemType = DMPToolExtensionType & DynamoItemType;

class DMPToolDynamoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DMPToolDynamoError';
  }
}

// Initialize AWS SDK clients (outside the handler function)
const getDynamoDBClient = (
  dynamoConfigParams: DynamoDBClientConfig
): DynamoDBClient => {
  const { region, maxAttempts, endpoint } = dynamoConfigParams;
  // If an endpoint was specified, we are running in a local environment
  return endpoint === undefined
    ? new DynamoDBClient({ region, maxAttempts })
    : new DynamoDBClient({
      region,
      maxAttempts,
      endpoint,
      requestHandler: new NodeHttpHandler({
        httpAgent: new http.Agent({ keepAlive: true }),
      }),
    });
}

/**
 * Lightweight query just to check if the DMP exists.
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @returns true if the DMP exists, false otherwise.
 * @throws DMPToolDynamoError if the record could not be fetched due to an error
 */
export const DMPExists = async (
  dynamoConnectionParams: DynamoConnectionParams,
  dmpId: string
): Promise<boolean> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo config or DMP ID');
  }

  // Very lightweight here, just returning a PK if successful
  const params = {
    KeyConditionExpression: "PK = :pk AND SK = :sk",
    ExpressionAttributeValues: {
      ":pk": { S: dmpIdToPK(dmpId) },
      ":sk": { S: versionToSK(DMP_LATEST_VERSION) }
    },
    ProjectExpression: "PK"
  }

  dynamoConnectionParams.logger.debug({ ...params, dmpId}, 'Checking if DMP exists in DynamoDB')
  try {
    const response = await queryTable(dynamoConnectionParams, params);
    return !isNullOrUndefined(response)
      && Array.isArray(response.Items)
      && response.Items.length > 0;

  } catch (err) {
    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal({ ...params, dmpId, errMsg}, 'Failed to check for DMP existence' )
    throw new DMPToolDynamoError(
      `Unable to check if DMP exists id: ${dmpId} - ${errMsg}`
    );
  }
}

/**
 * Fetch the latest version for every unique DMP ID
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @returns The latest version for every unique DMP ID
 * @throws DMPToolDynamoError if the records could not be fetched due to an error
 */
export const getAllUniqueDMPIds = async (
  dynamoConnectionParams: DynamoConnectionParams
): Promise<Map<string, string>> => {
  if (!dynamoConnectionParams) {
    throw new DMPToolDynamoError('Missing Dynamo config');
  }

  // Get the PK, SK and modified timestamps for all the latest versions
  const params = {
    ProjectionExpression: "PK, #mod", // `modified` is a reserved word in DynamoDB
    FilterExpression: `SK = :sk`,
    ExpressionAttributeNames: { "#mod": "modified" },
    ExpressionAttributeValues: {
      ":sk": { S: `${DMP_VERSION_PREFIX}#${DMP_LATEST_VERSION}` }
    }
  }

  try {
    dynamoConnectionParams.logger.debug({ ...params }, 'Scanning for latest DMP versions in DynamoDB')
    const response: DynamoVersionItemType[] = await scanTable(dynamoConnectionParams, params);

    if (Array.isArray(response) && response.length > 0) {
      const versions = new Map<string, string>();
      for (const item of response) {
        const unmarshalled: Record<string, any> = unmarshall(item);

        if (unmarshalled.PK && unmarshalled.modified) {
          const dmpId = unmarshalled.PK.replace(`${DMP_PK_PREFIX}#`, 'https://');
          versions.set(dmpId, unmarshalled.modified);
        }
      }
      return versions
    }
    return new Map<string, string>();

  } catch (err) {
    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal({ ...params, errMsg }, 'Failed to fetch all unique DMPs' )
    throw new DMPToolDynamoError(
      `Unable to fetch all unique DMPs: ${errMsg}`
    );
  }
}

/**
 * Fetch the version timestamps (including DMP_LATEST_VERSION) for the specified DMP ID.
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @returns The timestamps as strings (e.g. '2026-11-01T13:08:19Z' or 'latest')
 * @throws DMPToolDynamoError if the records could not be fetched due to an error
 */
export const getDMPVersions = async (
  dynamoConnectionParams: DynamoConnectionParams,
  dmpId: string
): Promise<DMPVersionType[] | []> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo Config or DMP ID');
  }

  const params = {
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :sk)",
    ExpressionAttributeValues: {
      ":pk": { S: dmpIdToPK(dmpId) },
      ":sk": { S: DMP_VERSION_PREFIX }
    },
    ProjectionExpression: "PK, SK, modified"
  }

  dynamoConnectionParams.logger.debug({ ...params, dmpId }, 'Fetching DMP versions from DynamoDB')
  try {
    const response: QueryCommandOutput = await queryTable(dynamoConnectionParams, params);

    if (Array.isArray(response.Items) && response.Items.length > 0) {
      const versions: DMPVersionType[] = [];
      for (const item of response.Items) {
        const unmarshalled: Record<string, any> = unmarshall(item);

        if (unmarshalled.PK && unmarshalled.SK && unmarshalled.modified) {
          versions.push({
            dmpId: unmarshalled.PK.replace(`${DMP_PK_PREFIX}#`, 'https://'),
            modified: unmarshalled.modified
          });
        }
      }
      return versions.sort((a: DMPVersionType, b: DMPVersionType) => {
        return b.modified.localeCompare(a.modified);
      });
    }
    return [];
  } catch (err) {
    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal({ ...params, dmpId, errMsg }, 'Failed to fetch DMP versions' )
    throw new DMPToolDynamoError(
      `Unable to fetch DMP versions id: ${dmpId} - ${errMsg}`
    );
  }
}

/**
 * Fetch the RDA Common Standard metadata record with DMP Tool specific extensions
 * for the specified DMP ID.
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param domainName The domain name of the DMPTool instance (e.g. 'dmptool.org')
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The complete RDA Common Standard metadata record with the DMP extension
 * metadata or an empty array if none were found.
 * @throws DMPToolDynamoError if the records could not be fetched due to an error
 */
// Fetch the specified DMP metadata record
//   - Version is optional, if it is not provided, ALL versions will be returned
//   - If you just want the latest version, use the DMP_LATEST_VERSION constant
export const getDMPs = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  version: string | null,
  includeExtensions = true
): Promise<DMPToolDMPType[] | []> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo config or DMP ID');
  }

  let params = {};

  if (version) {
    params = {
      KeyConditionExpression: "PK = :pk and SK = :sk",
      ExpressionAttributeValues: {
        ":pk": { S: dmpIdToPK(dmpId) },
        ":sk": { S: versionToSK(version) }
      }
    }
  } else {
    params = {
      KeyConditionExpression: "PK = :pk AND begins_with(SK, :sk)",
      ExpressionAttributeValues: {
        ":pk": { S: dmpIdToPK(dmpId) },
        ":sk": { S: DMP_VERSION_PREFIX }
      }
    }
  }

  dynamoConnectionParams.logger.debug(
    { ...params, dmpId, version, includeExtensions },
    'Fetching DMPs from DynamoDB'
  );

  try {
    const response: QueryCommandOutput = await queryTable(dynamoConnectionParams, params);
    if (response && response.Items && response.Items.length > 0) {
      const unmarshalled: DynamoVersionType[] = response.Items.map(item => unmarshall(item));

      // sort the results by the SK (version) descending
      const items: DynamoVersionType[] = unmarshalled.sort((a:DynamoVersionType, b: DynamoVersionType) => {
        return (b.SK).toString().localeCompare((a.SK).toString());
      });

      // If we are including the DMP Tool extensions, then fetch them
      if (includeExtensions) {
        // We need to remove properties specific to our DynamoDB table and then
        // merge in any DMP Tool specific extensions to the RDA Common Standard
        return await Promise.all(items.map(async (item: DynamoVersionType) => {
          // Fetch the DMP Tool extensions
          const extensions: DMPToolExtensionType[] = await getDMPExtensions(
            dynamoConnectionParams,
            domainName,
            dmpId,
            item.SK.replace(`${DMP_VERSION_PREFIX}#`, '')
          );

          // Destructure the Dynamo item because we don't need to return the PK and SK
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          const { PK, SK, ...version } = item;

          if (Array.isArray(extensions) && extensions.length > 0) {
            return {
              dmp: {
                ...version,
                ...extensions[0]
              }
            }
          }
          return {
            dmp: version,
          };
        }));

      } else {
        // Just return the RDA Common Standard metadata record
        return items.map(item => ({ dmp: item }));
      }
    }
  } catch (err) {
    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal(
      { ...params, dmpId, version, includeExtensions, errMsg },
      errMsg
    );
    throw new DMPToolDynamoError(
      `Unable to fetch DMP id: ${dmpId}, ver: ${version} - ${errMsg}`
    );
  }
  return [];
}

/**
 * Fetch the specified DMP Extensions metadata record
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @returns The DMP extension metadata records or an empty array if none were found.
 * @throws DMPToolDynamoError if the record could not be fetched
 */
const getDMPExtensions = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  version: string | null
): Promise<DMPToolExtensionType[] | []> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo Config or DMP ID');
  }

  let params = {};

  if (version) {
    params = {
      KeyConditionExpression: "PK = :pk and SK = :sk",
      ExpressionAttributeValues: {
        ":pk": { S: dmpIdToPK(dmpId) },
        ":sk": { S: versionToExtensionSK(version) }
      }
    }
  } else {
    params = {
      KeyConditionExpression: "PK = :pk AND begins_with(SK, :sk)",
      ExpressionAttributeValues: {
        ":pk": { S: dmpIdToPK(dmpId) },
        ":sk": { S: DMP_EXTENSION_PREFIX }
      }
    }
  }

  dynamoConnectionParams.logger.debug({ ...params, dmpId, version }, 'Fetching DMP Extensions from DynamoDB');

  const response: QueryCommandOutput = await queryTable(dynamoConnectionParams, params);
  if (response && response.Items && response.Items.length > 0) {
    const unmarshalled: DynamoExtensionItemType[] = response.Items.map(item => unmarshall(item) as DynamoExtensionItemType);

    // sort the results by the SK (version) descending
    const items: DynamoExtensionItemType[] = unmarshalled.sort((a: DynamoExtensionItemType, b: DynamoExtensionItemType) => {
      return (b.SK).toString().localeCompare((a.SK).toString());
    });

    // Coerce the items to the DMP Tool Extension schema
    return Promise.all(items.map(async (item: DynamoExtensionItemType) => {
      // Destructure the Dynamo item because we don't need to return the PK and SK
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const {PK, SK, ...extension} = item;

      // Fetch all the version timestamps
      const versions: DMPVersionType[] = await getDMPVersions(
        dynamoConnectionParams,
        dmpId
      );

      if (Array.isArray(versions) && versions.length > 0) {
        // Return the versions sorted descending
        extension.version = versions
          .map((v: DMPVersionType, idx: number) => {
            // The latest version doesn't have a query param appended to the URL
            const queryParam = idx === 0
              ? ''
              : `?version=${v.modified}`;
            const dmpIdWithoutProtocol = dmpId.replace(/^https?:\/\//, '');
            const accessURLBase = `https://${domainName}/dmps/`
            return {
              access_url: `${accessURLBase}${dmpIdWithoutProtocol}${queryParam}`,
              version: v.modified,
            }
          });
      }

      return extension;
    }));
  }
  return [];
}

/**
 * Persists the specified DMP metadata record to the DynamoDB table.
 * This function will handle the separation of RDA Common Standard and DMP Tool
 * specific metadata.
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param domainName The domain name of the DMPTool instance (e.g. 'dmptool.org')
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param dmp The DMP metadata record to persist as either an RDA Common Standard
 * or the standard with DMP Tool specific extensions.
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The persisted DMP metadata record as an RDA Common Standard DMP
 * metadata record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if the record could not be persisted
 */
export const createDMP = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  dmp: DMPToolDMPType,
  version = DMP_LATEST_VERSION,
  includeExtensions = true
): Promise<DMPToolDMPType | undefined> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0 || !dmp) {
    throw new DMPToolDynamoError('Missing Dynamo config, DMP ID or DMP metadata record');
  }

  // If the version is LATEST, then first make sure there is not already one present!
  if (version === DMP_LATEST_VERSION) {
    const exists: boolean = await DMPExists(dynamoConnectionParams, dmpId);

    if (exists) {
      dynamoConnectionParams.logger.error({dmpId}, 'Latest version already exists');
      throw new DMPToolDynamoError('Latest version already exists');
    }
  }

  try {
    // If the metadata is nested in a top level 'dmp' property, then unwrap it
    const innerMetadata = dmp?.dmp ?? dmp;

    // Separate the RDA Common Standard metadata from the DMP Tool specific extensions
    const dmptoolExtension = pick(innerMetadata, [...EXTENSION_KEYS]);
    const rdaCommonStandard = pick(
      innerMetadata,
      Object.keys(innerMetadata).filter(k => !EXTENSION_KEYS.includes(k))
    );

    const newVersionItem: DynamoVersionItemType = {
      ...rdaCommonStandard,
      PK: dmpIdToPK(dmpId),
      SK: versionToSK(version),
    }

    dynamoConnectionParams.logger.debug({ dmpId, version, includeExtensions }, 'Persisting DMP to DynamoDB');

    // Insert the RDA Common Standard metadata record into the DynamoDB table
    await putItem(
      dynamoConnectionParams,
      marshall(newVersionItem, { removeUndefinedValues: true })
    );

    // Create the DMP Tool extensions metadata record. We ALWAYS do this even if
    // the caller does not want them returned
    await createDMPExtensions(
      dynamoConnectionParams,
      dmpId,
      dmptoolExtension as DMPToolExtensionType, version
    );

    // Fetch the complete DMP metadata record including the RDA Common Standard
    // and the DMP Tool extensions
    return (await getDMPs(
      dynamoConnectionParams,
      domainName,
      dmpId,
      DMP_LATEST_VERSION,
      includeExtensions
    ))[0];
  } catch (err) {
    // If it was a DMPToolDynamoError that bubbled up, just throw it
    if (err instanceof DMPToolDynamoError) throw err;

    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal({ dmpId, version, includeExtensions }, errMsg);
    throw new DMPToolDynamoError(
      `Unable to create DMP id: ${dmpId}, ver: ${version} - ${errMsg}`
    );
  }
}

/**
 * Create a new DMP Extensions metadata record
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param dmp The DMP Tool extensions metadata record
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @returns The persisted DMP Tool extensions metadata record.
 */
const createDMPExtensions = async (
  dynamoConnectionParams: DynamoConnectionParams,
  dmpId: string,
  dmp: DMPToolExtensionType,
  version = DMP_LATEST_VERSION
): Promise<void> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0 || !dmp) {
    throw new DMPToolDynamoError('Missing Dynamo config, DMP ID or DMP metadata record');
  }

  const newExtensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(version),
  }

  dynamoConnectionParams.logger.debug({ dmpId, version }, 'Persisting DMP Extensions to DynamoDB');

  // Insert the DMP Tool Extensions metadata record into the DynamoDB table
  await putItem(
    dynamoConnectionParams,
    marshall(newExtensionItem, { removeUndefinedValues: true })
  );
}

/**
 * Update the specified DMP metadata record.
 * This function will handle the separation of RDA Common Standard and DMP Tool
 * specific metadata. We always update the latest version of the DMP metadata record.
 * Historical versions are immutable.
 *
 * A snapshot of the current "latest" version of the DMP's metadata will be taken
 * under the following circumstances:
 *   - If the `provenance` of the incoming record does not match the one on the
 *     latest record
 *   - If the `modified` timestamp of the latest record is older than 2 hours ago
 *
 * If a snapshot is made, the timestamp and link to retrieve it will appear
 * in the `versions` array
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param domainName The domain name of the DMPTool instance (e.g. 'dmptool.org')
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param dmp The DMP metadata record to persist as either an RDA Common Standard
 * or the standard with DMP Tool specific extensions.
 * @param gracePeriodInMS The grace period in milliseconds to wait before creating
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The persisted DMP metadata record as an RDA Common Standard DMP
 * metadata record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if the record could not be persisted
 */
export const updateDMP = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  dmp: DMPToolDMPType,
  gracePeriodInMS = 7200000, // 2 hours in milliseconds
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  if (!dynamoConnectionParams || !dmp || !dmpId) {
    throw new DMPToolDynamoError('Missing Dynamo config, DMP ID or DMP metadata record');
  }

  try {
    // If the metadata is nested in a top level 'dmp' property, then unwrap it
    const innerMetadata = dmp?.dmp ?? dmp;

    // Separate the RDA Common Standard metadata from the DMP Tool specific extensions
    const dmptoolExtension = pick(innerMetadata, [...EXTENSION_KEYS]);
    const rdaCommonStandard = pick(
      innerMetadata,
      Object.keys(innerMetadata).filter(k => !EXTENSION_KEYS.includes(k))
    );

    // Fetch the current latest version of the plan's maDMP record. Always get
    // the extensions because we need to check the provenance
    const latest: DMPToolDMPType = (await getDMPs(
      dynamoConnectionParams,
      domainName,
      dmpId,
      DMP_LATEST_VERSION,
      true
    ))[0];

    // Bail if there is no latest version (it has never been created yet or its tombstoned)
    // Or if the incoming modified timestamp is newer than the latest version's
    // modified timestamp (collision)
    if (isNullOrUndefined(latest)
      || latest.dmp?.tombstoned
      || latest.dmp?.modified > innerMetadata.modified) {
      throw new DMPToolDynamoError(
        `Cannot update a historical DMP id: ${dmpId}, ver: ${DMP_LATEST_VERSION}`
      );
    }

    const lastModified = new Date(latest.dmp?.modified).getTime();
    const now = Date.now();
    const gracePeriod = gracePeriodInMS ? Number(gracePeriodInMS) : 7200000;

    // We need to version the DMP if the provenance doesn't match or the modified
    // timestamp is older than 2 hours ago
    const needToVersion: boolean = dmptoolExtension.provenance !== latest.dmp.provenance
      || (now - lastModified) > gracePeriod;

    dynamoConnectionParams.logger.debug(
      { dmpId, lastModified, now, gracePeriod, needToVersion },
      'Determining if we need to version the DMP'
    );

    // If it was determined that we need to version the DMP, then create a new snapshot
    // using the modified date of the current latest version
    if (needToVersion) {
      await createDMP(
        dynamoConnectionParams,
        domainName,
        dmpId,
        latest.dmp,
        latest.dmp.modified
      );
    }

    // Updates can only ever occur on the latest version of the DMP (the Plan logic
    // should handle creating a snapshot of the original version of the DMP when
    // appropriate)
    const versionItem: DynamoVersionItemType = {
      ...rdaCommonStandard,
      PK: dmpIdToPK(dmpId),
      SK: versionToSK(DMP_LATEST_VERSION),
    }

    dynamoConnectionParams.logger.debug({ dmpId, versionItem }, 'Persisting DMP to DynamoDB');
    // Insert the RDA Common Standard metadata record into the DynamoDB table
    await putItem(
      dynamoConnectionParams,
      marshall(versionItem, { removeUndefinedValues: true })
    );

    // Update the DMP Tool extensions metadata record. We ALWAYS do this even if
    // the caller does not want them returned
    await updateDMPExtensions(
      dynamoConnectionParams,
      dmpId,
      dmptoolExtension as DMPToolExtensionType
    );

    // Fetch the complete DMP metadata record including the RDA Common Standard
    // and the DMP Tool extensions
    return (await getDMPs(
      dynamoConnectionParams,
      domainName,
      dmpId,
      DMP_LATEST_VERSION,
      includeExtensions)
    )[0];
  } catch (err) {
    // If it was a DMPToolDynamoError that bubbled up, just throw it
    if (err instanceof DMPToolDynamoError) throw err;
    const errMsg: string = toErrorMessage(err);
    dynamoConnectionParams.logger.fatal({ dmpId, includeExtensions, errMsg }, 'Unable to update DMP -');
    throw new DMPToolDynamoError(
      `Unable to update DMP id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${errMsg}`
    );
  }
}

/**
 * Update the specified DMP Extensions metadata record
 * We always update the latest version of the DMP metadata record. Historical versions are immutable.
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param dmpId the DMP ID (e.g. 'doi.org/11.12345/A1B2C3D4')
 * @param dmp
 * @returns The persisted DMP Tool extensions metadata record.
 */
const updateDMPExtensions = async (
  dynamoConnectionParams: DynamoConnectionParams,
  dmpId: string,
  dmp: DMPToolExtensionType
): Promise<void> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0 || !dmp) {
    throw new DMPToolDynamoError('Missing Dynamo config, DMP ID or DMP metadata record');
  }

  // Updates can only ever occur on the latest version of the DMP (the Plan logic
  // should handle creating a snapshot of the original version of the DMP when appropriate)
  const extensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(DMP_LATEST_VERSION),
  }

  dynamoConnectionParams.logger.debug({ dmpId }, 'Persisting DMP Extensions to DynamoDB');
  await putItem(
    dynamoConnectionParams,
    marshall(extensionItem, { removeUndefinedValues: true })
  );
}

/**
 * Create a Tombstone for the specified DMP metadata record
 * (registered/published DMPs only!)
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param domainName The domain name of the DMPTool instance (e.g. 'dmptool.org')
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The new tombstone DMP metadata record as an RDA Common Standard DMP
 * metadata record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if a tombstone could not be created
 */
export const tombstoneDMP = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo config or DMP ID');
  }

  // Get the latest version of the DMP including the extensions because we need
  // to check the registered status
  const dmp: DMPToolDMPType = (await getDMPs(
    dynamoConnectionParams,
    domainName,
    dmpId,
    DMP_LATEST_VERSION,
    true)
  )[0];
  if (!dmp) {
    dynamoConnectionParams.logger.error({ dmpId }, 'Unable to find current latest version of the DMP');
    throw new DMPToolDynamoError(`Unable to find DMP id: ${dmpId}, ver: ${DMP_LATEST_VERSION}`);
  }

  // If the DMP has been registered (aka published) we can create a tombstone
  if (dmp.dmp.registered) {
    // If the metadata is nested in a top level 'dmp' property, then unwrap it
    const innerMetadata = dmp?.dmp ?? dmp;

    // Separate the RDA Common Standard metadata from the DMP Tool specific extensions.
    const dmptoolExtension = pick(innerMetadata, [...EXTENSION_KEYS]);
    const rdaCommonStandard = pick(
      innerMetadata,
      Object.keys(innerMetadata).filter(k => !EXTENSION_KEYS.includes(k))
    );

    const now: string | null = convertMySQLDateTimeToRFC3339(new Date());
    if (isNullOrUndefined(now)) {
      dynamoConnectionParams.logger.error({ dmpId }, 'Unable to create modified date');
      throw new DMPToolDynamoError('Unable to create modified date');
    }

    const versionItem: DynamoVersionItemType = {
      ...rdaCommonStandard,
      PK: dmpIdToPK(dmpId),
      SK: versionToSK(DMP_TOMBSTONE_VERSION),
      title: `OBSOLETE: ${dmp.dmp?.title}`,
      modified: now,
    }

    try {
      // Update the RDA Common Standard metadata record
      await putItem(
        dynamoConnectionParams,
        marshall(versionItem, {removeUndefinedValues: true})
      );
      await deleteItem(
        dynamoConnectionParams,
        {
          PK: { S: dmpIdToPK(dmpId) },
          SK: { S: versionToSK(DMP_LATEST_VERSION) }
        }
      );

      // Tombstone the DMP Tool Extensions metadata record. We ALWAYS do this even
      // if the caller does not want them returned
      await tombstoneDMPExtensions(
        dynamoConnectionParams,
        dmpId,
        dmptoolExtension as DMPToolExtensionType
      );

      // Fetch the complete DMP metadata record including the RDA Common Standard
      // and the DMP Tool extensions
      return (await getDMPs(
        dynamoConnectionParams,
        domainName,
        dmpId,
        DMP_TOMBSTONE_VERSION,
        includeExtensions)
      )[0];
    } catch (err) {
      if (err instanceof DMPToolDynamoError) throw err;

      const errMsg: string = toErrorMessage(err);
      dynamoConnectionParams.logger.fatal({ dmpId, errMsg }, 'Unable to tombstone DMP');
      throw new DMPToolDynamoError(
        `Unable to tombstone id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${errMsg}`
      );
    }
  } else {
    dynamoConnectionParams.logger.warn({ dmpId }, 'Unable to tombstone an unregistered DMP');
    throw new DMPToolDynamoError(
      `Unable to tombstone DMP id: ${dmpId} because it is not registered/published`
    );
  }
}

/**
 * Add a tombstone date to the specified DMP Extensions metadata record
 * (registered/published DMPs only!)
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param dmp The DMP Tool specific extensions record to update.
 * @throws DMPToolDynamoError if the tombstone date could not be added
 */
const tombstoneDMPExtensions = async (
  dynamoConnectionParams: DynamoConnectionParams,
  dmpId: string,
  dmp: DMPToolExtensionType
): Promise<void> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0 || !dmp) {
    throw new DMPToolDynamoError('Missing Dynamo config, DMP ID or DMP metadata record');
  }

  const now = convertMySQLDateTimeToRFC3339(new Date());
  if (!now) {
    dynamoConnectionParams.logger.error({ dmpId }, 'Unable to create tombstone date');
    throw new DMPToolDynamoError('Unable to create tombstone date');
  }

  const extensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(DMP_TOMBSTONE_VERSION),
    tombstoned: now
  }

  dynamoConnectionParams.logger.debug({ dmpId }, 'Tombstoning DMP Extensions in DynamoDB');
  // Update the DMP Tool Extensions metadata record
  await putItem(
    dynamoConnectionParams,
    marshall(extensionItem, { removeUndefinedValues: true })
  );
  // Then delete the old latest version
  await deleteItem(
    dynamoConnectionParams,
    {
      PK: { S: dmpIdToPK(dmpId) },
      SK: { S: versionToExtensionSK(DMP_LATEST_VERSION) }
    }
  );
}

/**
 * Delete the specified DMP metadata record and any associated DMP Tool extension records.
 * This will NOT work on DMPs that have been registered/published.
 *
 * @param dynamoConnectionParams The DynamoDB connection parameters
 * @param domainName The domain name of the DMPTool instance (e.g. 'dmptool.org')
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param includeExtensions Whether or not to include the DMP Tool specific extensions
 * in the returned record. Defaults to true.
 * @returns The deleted DMP metadata record as an RDA Common Standard DMP metadata
 * record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if the record could not be deleted
 */
export const deleteDMP = async (
  dynamoConnectionParams: DynamoConnectionParams,
  domainName: string,
  dmpId: string,
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  if (!dynamoConnectionParams || !dmpId || dmpId.trim().length === 0) {
    throw new DMPToolDynamoError('Missing Dynamo config or DMP ID');
  }

  // Get the latest version of the DMP. Always get the extensions because we need
  // to check the registered status
  const dmps: DMPToolDMPType[] = await getDMPs(
    dynamoConnectionParams,
    domainName,
    dmpId,
    DMP_LATEST_VERSION,
    true
  );

  if (Array.isArray(dmps) && dmps.length > 0) {
    const latest: DMPToolDMPType = dmps[0];
    // If the caller wants just the RDA Common Standard metadata, then reload the
    // latest version without extensions
    const rdaOnly: RDACommonStandardDMPType = pick(
      latest.dmp,
      Object.keys(latest.dmp).filter(k => !EXTENSION_KEYS.includes(k))
    );
    const toReturn: DMPToolDMPType = includeExtensions
      ? latest
      : { dmp: rdaOnly };

    // If the latest version was found, and it has NOT been registered/published
    if (latest && !latest.dmp.registered) {
      try {
        dynamoConnectionParams.logger.debug({ dmpId }, 'Deleting DMP from DynamoDB');

        // Delete all records with that DMP ID
        await deleteItem(
          dynamoConnectionParams,
          { PK: { S: dmpIdToPK(dmpId) } }
        );
        return toReturn as DMPToolDMPType;
      } catch (err) {
        const errMsg: string = toErrorMessage(err);
        dynamoConnectionParams.logger.fatal({ dmpId, errMsg }, 'Unable to delete DMP');
        throw new DMPToolDynamoError(
          `Unable to delete id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${errMsg}`
        );
      }
    } else {
      dynamoConnectionParams.logger.error({ dmpId }, 'Unable to delete an unregistered DMP');
      throw new DMPToolDynamoError(
        `Unable to delete id: ${dmpId} because it does not exist or is registered`
      );
    }
  } else {
    dynamoConnectionParams.logger.error({ dmpId }, 'Unable to find current latest version of the DMP');
    throw new DMPToolDynamoError(
      `Unable to find id: ${dmpId}, ver: ${DMP_LATEST_VERSION}`
    );
  }
}

/**
 * Scan the specified DynamoDB table using the specified criteria
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param params the query parameters
 * @returns an array of DynamoDB items
 */
// We're not currently using it, but did not want to remove it just in case
// we need it in the future
//
const scanTable = async (
  dynamoConnectionParams: DynamoConnectionParams,
  params: object
): Promise<DynamoVersionItemType[] | DynamoExtensionItemType[] | []> => {
  if (!dynamoConnectionParams || !params) {
    throw new DMPToolDynamoError('Missing Dynamo config or params');
  }

  let items: DynamoVersionItemType[] | DynamoExtensionItemType[] = [];
  let lastEvaluatedKey;

  // Query the DynamoDB index table for all DMP metadata (with pagination)
  do {
    const command: ScanCommand = new ScanCommand({
      TableName: dynamoConnectionParams.tableName,
      ExclusiveStartKey: lastEvaluatedKey,
      ConsistentRead: false,
      ReturnConsumedCapacity: 'TOTAL',
      ...params
    });

    try {
      const dynamoDBClient = getDynamoDBClient(dynamoConnectionParams);
      dynamoConnectionParams.logger.debug(
        { table: dynamoConnectionParams.tableName, params },
        'Scanning DynamoDB table'
      );
      const response: ScanCommandOutput = await dynamoDBClient.send(command);

      // Collect items and update the pagination key
      items = items.concat(
        response.Items as DynamoVersionItemType[] | DynamoExtensionItemType[] || []
      );
      // LastEvaluatedKey is the position of the end cursor from the query that was just run
      // when it is undefined, then the query reached the end of the results.
      lastEvaluatedKey = response?.LastEvaluatedKey;
    } catch (error) {
      const errMsg: string = toErrorMessage(error);
      dynamoConnectionParams.logger.fatal(
        { ...params, ...dynamoConnectionParams, errMsg },
        'Unable to scan DynamoDB table'
      );
      throw new DMPToolDynamoError(`Unable to scan DynamoDB table - ${errMsg}`);
    }
  } while (lastEvaluatedKey);

  // Deserialize and split items into multiple files if necessary
  return items;
}

/**
 * Query the specified DynamoDB table using the specified criteria
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param params
 * @returns an array of DynamoDB items
 */
const queryTable = async (
  dynamoConnectionParams: DynamoConnectionParams,
  params: object = {}
): Promise<QueryCommandOutput> => {
  // Query the DynamoDB index table for all DMP metadata (with pagination)
  const command = new QueryCommand({
    TableName: dynamoConnectionParams.tableName,
    ConsistentRead: false,
    ReturnConsumedCapacity: 'TOTAL',
    ...params
  });

  const dynamoDBClient = getDynamoDBClient(dynamoConnectionParams);
  return await dynamoDBClient.send(command);
}

/**
 * Create/Update an item in the specified DynamoDB table
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param item the item to insert/update
 */
const putItem = async (
  dynamoConnectionParams: DynamoConnectionParams,
  item: Record<string, AttributeValue>
): Promise<void> => {
  const dynamoDBClient = getDynamoDBClient(dynamoConnectionParams)
  // Delete the item from the DynamoDB table
  await dynamoDBClient.send(new PutItemCommand({
    TableName: dynamoConnectionParams.tableName,
    ReturnConsumedCapacity: 'TOTAL',
    Item: item
  }));
  return;
}

/**
 * Delete an item from the specified DynamoDB table
 *
 * @param dynamoConnectionParams the DynamoDB connection parameters
 * @param key the partition (and sort key if applicable) of the item to delete
 */
const deleteItem = async (
  dynamoConnectionParams: DynamoConnectionParams,
  key: Record<string, AttributeValue>
): Promise<void> => {
  const dynamoDBClient = getDynamoDBClient(dynamoConnectionParams)

  // Delete the item from the DynamoDB table
  await dynamoDBClient.send(new DeleteItemCommand({
    TableName: dynamoConnectionParams.tableName,
    ReturnConsumedCapacity: 'TOTAL',
    Key: key
  }));
}

/**
 * Convert a DMP ID into a PK for the DynamoDB table
 *
 * @param dmpId
 */
const dmpIdToPK = (dmpId: string): string => {
  // Remove the protocol and slashes from the DMP ID
  const id = dmpId?.replace(/(^\w+:|^)\/\//, '');
  return `${DMP_PK_PREFIX}#${id}`;
}

/**
 * Convert a DMP ID version timestamp into a SK for the DynamoDB table for the
 * RDA Common Standard metadata record
 *
 * @param version the version as a timestamp or "latest"
 * (e.g. "2026-11-01T13:08:19Z", "latest")
 */
const versionToSK = (version = DMP_LATEST_VERSION): string => {
  return `${DMP_VERSION_PREFIX}#${version}`;
}

/**
 * Convert a DMP ID version timestamp into a SK for the DynamoDB table for a
 * DMP Tool extension record
 *
 * @param version the version as a timestamp or "latest"
 * (e.g. "2026-11-01T13:08:19Z", "latest")
 * @returns string
 */
const versionToExtensionSK = (version = DMP_LATEST_VERSION): string => {
  return `${DMP_EXTENSION_PREFIX}#${version}`;
}

/**
 * Extract a subset of keys from an object
 *
 * @param obj
 * @param keys
 */
function pick<T extends object, K extends keyof T>(obj: T, keys: K[]): Pick<T, K> {
  const result = {} as Pick<T, K>;
  keys.forEach((key) => {
    if (key in obj) {
      result[key] = obj[key];
    }
  });
  return result;
}
