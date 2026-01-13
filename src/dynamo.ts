import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import {
  AttributeValue,
  DeleteItemCommand,
  DynamoDBClient,
  DynamoDBClientConfig, PutItemCommand,
  QueryCommand,
  QueryCommandOutput,
  ScanCommand, ScanCommandOutput,
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

// The list of properties that are extensions to the RDA Common Standard
const EXTENSION_KEYS: string[] = [
  'provenance',
  'privacy',
  'featured',
  'registered',
  'tombstoned',
  'narrative',
  'research_domain',
  'research_facility',
  'version',
  'funding_opportunity',
  'funding_project'
];

interface DMPVersionType {
  PK: string;
  SK: string;
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

const dynamoConfigParams: DynamoDBClientConfig = {
  region: process.env.AWS_REGION || 'us-west-2',
  maxAttempts: process.env.DYNAMO_MAX_ATTEMPTS
    ? parseInt(process.env.DYNAMO_MAX_ATTEMPTS)
    : 3,
}

class DMPToolDynamoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DMPToolDynamoError';
  }
}

// Initialize AWS SDK clients (outside the handler function)
const dynamoDBClient = new DynamoDBClient(dynamoConfigParams);

/**
 * Lightweight query just to check if the DMP exists.
 *
 * @param dmpId
 * @returns true if the DMP exists, false otherwise.
 * @throws DMPToolDynamoError if the record could not be fetched due to an error
 */
export const DMPExists = async (
  dmpId: string
): Promise<boolean> => {
  // Very lightweight here, just returning a PK if successful
  const params = {
    KeyConditionExpression: "PK = :pk AND SK = :sk",
    ExpressionAttributeValues: {
      ":pk": { S: dmpIdToPK(dmpId) },
      ":sk": { S: versionToSK(DMP_LATEST_VERSION) }
    },
    ProjectExpression: "PK"
  }

  try {
    const response = await queryTable(params);
    return !isNullOrUndefined(response)
      && Array.isArray(response.Items)
      && response.Items.length > 0;

  } catch (err) {
    throw new DMPToolDynamoError(
      `Unable to check if DMP exists id: ${dmpId} - ${toErrorMessage(err)}`
    );
  }
}

/**
 * Fetch the version timestamps (including DMP_LATEST_VERSION) for the specified DMP ID.
 *
 * @param dmpId
 * @returns The timestamps as strings (e.g. '2026-11-01T13:08:19Z' or 'latest')
 * @throws DMPToolDynamoError if the records could not be fetched due to an error
 */
export const getDMPVersions = async (
  dmpId: string
): Promise<DMPVersionType[] | []> => {
  const params = {
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :sk)",
    ExpressionAttributeValues: {
      ":pk": { S: dmpIdToPK(dmpId) },
      ":sk": { S: DMP_VERSION_PREFIX }
    },
    ProjectionExpression: "PK, SK, modified"
  }
  try {
    const response: QueryCommandOutput = await queryTable(params);

    if (Array.isArray(response.Items) && response.Items.length > 0) {
      const versions: DMPVersionType[] = [];
      for (const item of response.Items) {
        const unmarshalled: Record<string, any> = unmarshall(item);

        if (unmarshalled.PK && unmarshalled.SK && unmarshalled.modified) {
          versions.push({
            PK: unmarshalled.PK,
            SK: unmarshalled.SK,
            modified: unmarshalled.modified
          });
        }
      }
      return versions
    }
    return [];
  } catch (err) {
    throw new DMPToolDynamoError(
      `Unable to fetch DMP versions id: ${dmpId} - ${toErrorMessage(err)}`
    );
  }
}

/**
 * Fetch the RDA Common Standard metadata record with DMP Tool specific extensions
 * for the specified DMP ID.
 *
 * @param dmpId
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
  dmpId: string,
  version: string | null,
  includeExtensions = true
): Promise<DMPToolDMPType[] | []> => {
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

  try {
    const response: QueryCommandOutput = await queryTable(params);
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
        return items.map(item => ({ dmp: unmarshall(item) }));
      }
    }
  } catch (err) {
    throw new DMPToolDynamoError(
      `Unable to fetch DMP id: ${dmpId}, ver: ${version} - ${toErrorMessage(err)}`
    );
  }
  return [];
}

/**
 * Fetch the specified DMP Extensions metadata record
 *
 * @param dmpId
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @returns The DMP extension metadata records or an empty array if none were found.
 * @throws DMPToolDynamoError if the record could not be fetched
 */
const getDMPExtensions = async (
  dmpId: string,
  version: string | null
): Promise<DMPToolExtensionType[] | []> => {
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

  const response: QueryCommandOutput = await queryTable(params);
  if (response && response.Items && response.Items.length > 0) {
    const unmarshalled: DynamoExtensionItemType[] = response.Items.map(item => unmarshall(item) as DynamoExtensionItemType);

    // sort the results by the SK (version) descending
    const items: DynamoExtensionItemType[] = unmarshalled.sort((a:DynamoExtensionItemType, b: DynamoExtensionItemType) => {
      return (b.SK).toString().localeCompare((a.SK).toString());
    });

    // Coerce the items to the DMP Tool Extension schema
    return Promise.all(items.map(async (item: DynamoExtensionItemType) => {
      // Destructure the Dynamo item because we don't need to return the PK and SK
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { PK, SK, ...extension } = item;

      // Fetch all the version timestamps
      const versions: DMPVersionType[] = await getDMPVersions(dmpId)

      if (Array.isArray(versions) && versions.length > 0) {
        // Return the versions sorted descending
        extension.version = versions
          .sort((a: DMPVersionType, b: DMPVersionType) => b.modified.localeCompare(a.modified))
          .map((v: DMPVersionType) => {
            // The latest version doesn't have a query param appended to the URL
            const queryParam = v.SK.endsWith(DMP_LATEST_VERSION)
              ? ''
              : `?version=${v.modified}`;
            const dmpIdWithoutProtocol = dmpId.replace(/^https?:\/\//, '');
            const accessURLBase = `https://${process.env.DOMAIN_NAME}/dmps/`
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
 * @param dmpId The DMP ID (e.g. '123456789')
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
  dmpId: string,
  dmp: DMPToolDMPType,
  version = DMP_LATEST_VERSION,
  includeExtensions = true
): Promise<DMPToolDMPType | undefined> => {
  if (!dmpId || dmpId.trim().length === 0 || !dmp) {
    throw new DMPToolDynamoError('Missing DMP ID or DMP metadata record');
  }

  // If the version is LATEST, then first make sure there is not already one present!
  const exists: boolean = await DMPExists(dmpId);
  if (exists) {
    throw new DMPToolDynamoError('Latest version already exists');
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

    // Insert the RDA Common Standard metadata record into the DynamoDB table
    await putItem(marshall(newVersionItem, {removeUndefinedValues: true}));

    // Create the DMP Tool extensions metadata record. We ALWAYS do this even if
    // the caller does not want them returned
    await createDMPExtensions(dmpId, dmptoolExtension as DMPToolExtensionType, version);

    // Fetch the complete DMP metadata record including the RDA Common Standard
    // and the DMP Tool extensions
    return (await getDMPs(dmpId, DMP_LATEST_VERSION, includeExtensions))[0];
  } catch (err) {
    // If it was a DMPToolDynamoError that bubbled up, just throw it
    if (err instanceof DMPToolDynamoError) throw err;
    throw new DMPToolDynamoError(
      `Unable to create DMP id: ${dmpId}, ver: ${version} - ${toErrorMessage(err)}`
    );
  }
}

/**
 * Create a new DMP Extensions metadata record
 *
 * @param dmpId
 * @param dmp
 * @param version The version of the DMP metadata record to persist
 * (e.g. '2026-11-01T13:08:19Z').
 * If not provided, the latest version will be used. Defaults to DMP_LATEST_VERSION.
 * @returns The persisted DMP Tool extensions metadata record.
 */
const createDMPExtensions = async (
  dmpId: string,
  dmp: DMPToolExtensionType,
  version = DMP_LATEST_VERSION
): Promise<void> => {
  const newExtensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(version),
  }

  // Insert the DMP Tool Extensions metadata record into the DynamoDB table
  await putItem(marshall(newExtensionItem, { removeUndefinedValues: true }));
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
 * @param dmp
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The persisted DMP metadata record as an RDA Common Standard DMP
 * metadata record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if the record could not be persisted
 */
export const updateDMP = async (
  dmp: DMPToolDMPType,
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  const dmpId: string = dmp.dmp?.dmp_id?.identifier;

  if (!dmp || !dmpId) {
    throw new DMPToolDynamoError('Missing DMP ID or DMP metadata record');
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
    const latest: DMPToolDMPType = (await getDMPs(dmpId, DMP_LATEST_VERSION, true))[0];

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
    const gracePeriod = process.env.VERSION_GRACE_PERIOD ? Number(process.env.VERSION_GRACE_PERIOD) : 7200000;

    // We need to version the DMP if the provenance doesn't match or the modified
    // timestamp is older than 2 hours ago
    const needToVersion: boolean = dmptoolExtension.provenance !== latest.dmp.provenance
      || (now - lastModified) > gracePeriod;

    // If it was determined that we need to version the DMP, then create a new snapshot
    // using the modified date of the current latest version
    if (needToVersion) {
      await createDMP(dmpId, latest.dmp, latest.dmp.modified);
    }

    // Updates can only ever occur on the latest version of the DMP (the Plan logic
    // should handle creating a snapshot of the original version of the DMP when
    // appropriate)
    const versionItem: DynamoVersionItemType = {
      ...rdaCommonStandard,
      PK: dmpIdToPK(dmpId),
      SK: versionToSK(DMP_LATEST_VERSION),
    }

    // Insert the RDA Common Standard metadata record into the DynamoDB table
    await putItem(marshall(versionItem, { removeUndefinedValues: true }));

    // Update the DMP Tool extensions metadata record. We ALWAYS do this even if
    // the caller does not want them returned
    await updateDMPExtensions(dmpId, dmptoolExtension as DMPToolExtensionType);

    // Fetch the complete DMP metadata record including the RDA Common Standard
    // and the DMP Tool extensions
    return (await getDMPs(dmpId, DMP_LATEST_VERSION, includeExtensions))[0];
  } catch (err) {
    // If it was a DMPToolDynamoError that bubbled up, just throw it
    if (err instanceof DMPToolDynamoError) throw err;
    throw new DMPToolDynamoError(
      `Unable to create DMP id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${toErrorMessage(err)}`
    );
  }
}

/**
 * Update the specified DMP Extensions metadata record
 * We always update the latest version of the DMP metadata record. Historical versions are immutable.
 *
 * @param dmpId
 * @param dmp
 * @returns The persisted DMP Tool extensions metadata record.
 */
const updateDMPExtensions = async (
  dmpId: string,
  dmp: DMPToolExtensionType
): Promise<void> => {
  // Updates can only ever occur on the latest version of the DMP (the Plan logic
  // should handle creating a snapshot of the original version of the DMP when appropriate)
  const extensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(DMP_LATEST_VERSION),
  }

  await putItem(marshall(extensionItem, { removeUndefinedValues: true }));
}

/**
 * Create a Tombstone for the specified DMP metadata record
 * (registered/published DMPs only!)
 *
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param includeExtensions Whether or not to include the DMP Tool specific
 * extensions in the returned record. Defaults to true.
 * @returns The new tombstone DMP metadata record as an RDA Common Standard DMP
 * metadata record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if a tombstone could not be created
 */
export const tombstoneDMP = async (
  dmpId: string,
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  // Get the latest version of the DMP including the extensions because we need
  // to check the registered status
  const dmp: DMPToolDMPType = (await getDMPs(dmpId, DMP_LATEST_VERSION, true))[0];
  if (!dmp) {
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
      await putItem(marshall(versionItem, {removeUndefinedValues: true}));
      await deleteItem({
        PK: { S: dmpIdToPK(dmpId) },
        SK: { S: versionToSK(DMP_LATEST_VERSION) }
      });

      // Tombstone the DMP Tool Extensions metadata record. We ALWAYS do this even
      // if the caller does not want them returned
      await tombstoneDMPExtensions(dmpId, dmptoolExtension as DMPToolExtensionType);

      // Fetch the complete DMP metadata record including the RDA Common Standard
      // and the DMP Tool extensions
      return (await getDMPs(dmpId, DMP_TOMBSTONE_VERSION, includeExtensions))[0];
    } catch (err) {
      if (err instanceof DMPToolDynamoError) throw err;
      throw new DMPToolDynamoError(
        `Unable to tombstone id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${toErrorMessage(err)}`
      );
    }
  } else {
    throw new DMPToolDynamoError(
      `Unable to tombstone DMP id: ${dmpId} because it is not registered/published`
    );
  }
}

/**
 * Add a tombstone date to the specified DMP Extensions metadata record
 * (registered/published DMPs only!)
 *
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param dmp The DMP Tool specific extensions record to update.
 * @throws DMPToolDynamoError if the tombstone date could not be added
 */
const tombstoneDMPExtensions = async (
  dmpId: string,
  dmp: DMPToolExtensionType
): Promise<void> => {
  const now = convertMySQLDateTimeToRFC3339(new Date());
  if (!now) {
    throw new DMPToolDynamoError('Unable to create tombstone date');
  }

  const extensionItem: DynamoExtensionItemType = {
    ...dmp,
    PK: dmpIdToPK(dmpId),
    SK: versionToExtensionSK(DMP_TOMBSTONE_VERSION),
    tombstoned: now
  }

  // Update the DMP Tool Extensions metadata record
  await putItem(marshall(extensionItem, { removeUndefinedValues: true }));
  // Then delete the old latest version
  await deleteItem({
    PK: { S: dmpIdToPK(dmpId) },
    SK: { S: versionToExtensionSK(DMP_LATEST_VERSION) }
  });
}

/**
 * Delete the specified DMP metadata record and any associated DMP Tool extension records.
 * This will NOT work on DMPs that have been registered/published.
 *
 * @param dmpId The DMP ID (e.g. '11.12345/A1B2C3')
 * @param includeExtensions Whether or not to include the DMP Tool specific extensions
 * in the returned record. Defaults to true.
 * @returns The deleted DMP metadata record as an RDA Common Standard DMP metadata
 * record with the DMP Tool specific extensions merged in.
 * @throws DMPToolDynamoError if the record could not be deleted
 */
export const deleteDMP = async (
  dmpId: string,
  includeExtensions = true
): Promise<DMPToolDMPType> => {
  // Get the latest version of the DMP. Always get the extensions because we need
  // to check the registered status
  const dmps: DMPToolDMPType[] = await getDMPs(dmpId, DMP_LATEST_VERSION, true);

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
        // Delete all records with that DMP ID
        await deleteItem({PK: {S: dmpIdToPK(dmpId)}});
        return toReturn as DMPToolDMPType;
      } catch (err) {
        throw new DMPToolDynamoError(
          `Unable to delete id: ${dmpId}, ver: ${DMP_LATEST_VERSION} - ${toErrorMessage(err)}`
        );
      }
    } else {
      throw new DMPToolDynamoError(
        `Unable to delete id: ${dmpId} because it does not exist or is registered`
      );
    }
  } else {
    throw new DMPToolDynamoError(
      `Unable to find id: ${dmpId}, ver: ${DMP_LATEST_VERSION}`
    );
  }
}

/**
 * Scan the specified DynamoDB table using the specified criteria
 * @param table
 * @param params
 * @returns an array of DynamoDB items
 */
// We're not currently using it, but did not want to remove it just in case
// we need it in the future
//
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const scanTable = async (
  table: string,
  params: object
): Promise<DynamoVersionItemType[] | DynamoExtensionItemType[] | []> => {
  let items: DynamoVersionItemType[] | DynamoExtensionItemType[] = [];
  let lastEvaluatedKey;

  // Query the DynamoDB index table for all DMP metadata (with pagination)
  do {
    const command: ScanCommand = new ScanCommand({
      TableName: table,
      ExclusiveStartKey: lastEvaluatedKey,
      ConsistentRead: false,
      ReturnConsumedCapacity: 'TOTAL',
      ...params
    });

    const response: ScanCommandOutput = await dynamoDBClient.send(command);

    // Collect items and update the pagination key
    items = items.concat(
      response.Items as DynamoVersionItemType[] | DynamoExtensionItemType[] || []
    );
    // LastEvaluatedKey is the position of the end cursor from the query that was just run
    // when it is undefined, then the query reached the end of the results.
    lastEvaluatedKey = response?.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  // Deserialize and split items into multiple files if necessary
  return items;
}

/**
 * Query the specified DynamoDB table using the specified criteria
 *
 * @param params
 * @returns an array of DynamoDB items
 */
const queryTable = async (
  params: object = {}
): Promise<QueryCommandOutput> => {
  // Query the DynamoDB index table for all DMP metadata (with pagination)
  const command = new QueryCommand({
    TableName: process.env.DYNAMODB_TABLE_NAME,
    ConsistentRead: false,
    ReturnConsumedCapacity: 'TOTAL',
    ...params
  });

  return await dynamoDBClient.send(command);
}

/**
 * Create/Update an item in the specified DynamoDB table
 *
 * @param item
 */
const putItem = async (
  item: Record<string, AttributeValue>
): Promise<void> => {
  // Delete the item from the DynamoDB table
  await dynamoDBClient.send(new PutItemCommand({
    TableName: process.env.DYNAMO_TABLE_NAME,
    ReturnConsumedCapacity: 'TOTAL',
    Item: item
  }));
  return;
}

/**
 * Delete an item from the specified DynamoDB table
 *
 * @param key
 */
const deleteItem = async (
  key: Record<string, AttributeValue>
): Promise<void> => {
  // Delete the item from the DynamoDB table
  await dynamoDBClient.send(new DeleteItemCommand({
    TableName: process.env.DYNAMO_TABLE_NAME,
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
