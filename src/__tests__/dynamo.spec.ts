import pino, { Logger } from 'pino';
import { mockClient } from 'aws-sdk-client-mock';
import { DMPToolDMPType } from "@dmptool/types";
import {
  DeleteItemCommand,
  DynamoDBClient,
  PutItemCommand,
  QueryCommand,
  ScanCommand
} from '@aws-sdk/client-dynamodb';
import {
  DMPExists,
  getAllUniqueDMPIds,
  getDMPVersions,
  getDMPs,
  DMP_LATEST_VERSION,
  createDMP,
  updateDMP,
  DMP_TOMBSTONE_VERSION,
  tombstoneDMP,
  deleteDMP
} from '../dynamo';

const mockLogger: Logger = pino({ level: 'silent' });
const mockDomain = 'example.com';

const dynamoMock = mockClient(DynamoDBClient);

const mockConfig = {
  tableName: 'test-table',
  maxAttempts: 3,
  region: 'us-west-2',
  logger: mockLogger,
};

describe('DMPExists', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  it('should return true when DMP exists in DynamoDB', async () => {
    const mockDmpId = 'doi.org/11.12345/A1B2C3';

    dynamoMock.on(QueryCommand).resolves({
      Items: [{
        PK: { S: `DMP#${mockDmpId}` },
        SK: { S: 'VERSION#latest' },
      }],
    });

    const result = await DMPExists(mockConfig, mockDmpId);

    expect(result).toBe(true);
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should return false when DMP does not exist in DynamoDB', async () => {
    const mockDmpId = 'nonexistent-dmp-456';

    dynamoMock.on(QueryCommand).resolves({
      Items: [],
    });

    const result = await DMPExists(mockConfig, mockDmpId);

    expect(result).toBe(false);
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should throw error when DynamoDB operation fails', async () => {
    const mockDmpId = 'test-dmp-789';
    const errorMessage = 'DynamoDB error';

    dynamoMock.on(QueryCommand).rejects(new Error(errorMessage));

    await expect(DMPExists(mockConfig, mockDmpId)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(1);
  });
});

describe('getDMPVersions', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  it('should return array of versions when DMP versions exist in DynamoDB', async () => {
    const mockDmpId = 'doi.org/11.12345/A1B2C3';

    dynamoMock.on(QueryCommand).resolves({
      Items: [
        {
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: 'VERSION#2025-01-01T01:02:03Z' },
          modified: { S: '2025-01-01T01:02:03Z' }
        },
        {
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: 'VERSION#2025-09-30T23:59:59Z' },
          modified: { S: '2025-09-30T23:59:59Z' }
        },
        {
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: 'VERSION#latest' },
          modified: { S: '2025-12-24T00:00:00Z' }
        }
      ],
    });

    const result = await getDMPVersions(mockConfig, mockDmpId);

    expect(result).toHaveLength(3);
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should return empty array when no DMP versions exist in DynamoDB', async () => {
    const mockDmpId = 'nonexistent-dmp-456';

    dynamoMock.on(QueryCommand).resolves({
      Items: [],
    });

    const result = await getDMPVersions(mockConfig, mockDmpId);

    expect(result).toEqual([]);
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should throw error when DynamoDB operation fails', async () => {
    const mockDmpId = 'test-dmp-789';
    const errorMessage = 'DynamoDB error';

    dynamoMock.on(QueryCommand).rejects(new Error(errorMessage));

    await expect(getDMPVersions(mockConfig, mockDmpId)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(1);
  });
});

describe('getAllUniqueDMPIds', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  it('should return array of latest DMP versions when DMPs exist in DynamoDB', async () => {
    const mockDmpId1 = 'doi.org/11.12345/A1B2C3';
    const mockDmpId2 = 'doi.org/11.12345/D4E5F6';
    const mockDmpId3 = 'doi.org/11.12345/G7H8I9';

    dynamoMock.on(ScanCommand).resolves({
      Items:[
        {
          PK: { S: `DMP#${mockDmpId1}` },
          SK: { S: 'VERSION#latest' },
          modified: { S: '2025-01-01T12:00:00Z' }
        },
        {
          PK: { S: `DMP#${mockDmpId2}` },
          SK: { S: 'VERSION#latest' },
          modified: { S: '2025-01-15T08:30:00Z' }
        },
        {
          PK: { S: `DMP#${mockDmpId3}` },
          SK: { S: 'VERSION#latest' },
          modified: { S: '2025-02-01T14:45:00Z' }
        }
      ]
    });

    const result: Map<string, string> = await getAllUniqueDMPIds(mockConfig);

    expect(result).toBeDefined();
    expect(result.get(`https://${mockDmpId1}`)).toEqual('2025-01-01T12:00:00Z');
    expect(result.get(`https://${mockDmpId2}`)).toEqual('2025-01-15T08:30:00Z');
    expect(result.get(`https://${mockDmpId3}`)).toEqual('2025-02-01T14:45:00Z');
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should return empty array when no DMPs exist in DynamoDB', async () => {
    dynamoMock.on(ScanCommand).resolves({ Items: [] });

    const result: Map<string, string> = await getAllUniqueDMPIds(mockConfig);

    expect(result).toEqual(new Map());
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should return empty array when response is undefined', async () => {
    dynamoMock.on(ScanCommand).resolves({ Items:undefined });

    const result: Map<string, string> = await getAllUniqueDMPIds(mockConfig);

    expect(result).toEqual(new Map());
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should throw error when dynamoConnectionParams is missing', async () => {
    await expect(getAllUniqueDMPIds(null as any)).rejects.toThrow('Missing Dynamo config');
    expect(dynamoMock.calls()).toHaveLength(0);
  });

  it('should throw error when DynamoDB operation fails', async () => {
    const errorMessage = 'DynamoDB error';

    dynamoMock.on(ScanCommand).rejects(new Error(errorMessage));

    await expect(getAllUniqueDMPIds(mockConfig)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(1);
  });
});

describe('getDMPs', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  it('should return the specific DMP version when it exists and a version is specified', async () => {
    const mockDmpId = 'doi.org/11.12345/A1B2C3';
    const mockVersion = '2025-01-01T01:02:03Z';

    dynamoMock.on(QueryCommand)
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${mockVersion}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-01T00:00:00Z' },
          modified: { S: '2025-01-01T00:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }],
      })
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${mockVersion}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }, {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#2024-11-21T12:00:00Z' },
            modified: { S: '2024-11-21T12:00:00Z' }
          }
        ]
      });

    const result: DMPToolDMPType[] = await getDMPs(
      mockConfig,
      mockDomain,
      mockDmpId,
      mockVersion
    );

    expect(result).toBeDefined();
    expect(result[0]?.dmp?.dmp_id?.identifier).toEqual(`https://${mockDmpId}`);
    expect(result[0]?.dmp?.featured).toEqual('yes');
    expect(result[0]?.dmp?.version.length).toEqual(2);
    expect(result[0]?.dmp?.version[0].version).toEqual('2025-01-01T00:00:00Z');
    expect(result[0]?.dmp?.version[0].access_url).toEqual(`https://${mockDomain}/dmps/${mockDmpId}`);
    expect(result[0]?.dmp?.version[1].version).toEqual('2024-11-21T12:00:00Z');
    expect(result[0]?.dmp?.version[1].access_url).toEqual(`https://${mockDomain}/dmps/${mockDmpId}?version=2024-11-21T12:00:00Z`);

    expect(dynamoMock.calls()).toHaveLength(3);
    const firstCall: any = dynamoMock.call(0);
    expect(firstCall.args[0].input['ExpressionAttributeValues'][':pk']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(firstCall.args[0].input['ExpressionAttributeValues'][':sk']['S']).toEqual(`VERSION#${mockVersion}`);
    const lastCall: any = dynamoMock.call(1);
    expect(lastCall.args[0].input['ExpressionAttributeValues'][':pk']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(lastCall.args[0].input['ExpressionAttributeValues'][':sk']['S']).toEqual(`EXTENSION#${mockVersion}`);
  });

  it('should default to latest version when version is not specified', async () => {
    const mockDmpId = 'doi.org/11.12345/A1B2C3';

    dynamoMock.on(QueryCommand)
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP Latest' },
          created: { S: '2025-01-01T00:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }],
      })
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'no' },
          privacy: { S: 'private' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    const result: DMPToolDMPType[] = await getDMPs(
      mockConfig,
      mockDomain,
      mockDmpId,
      null
    );

    expect(result).toBeDefined();
    // Make sure it has data from the RDA Common Standard metadata in the `SK: VERSION#???` DynamoDB item
    expect(result[0]?.dmp?.dmp_id?.identifier).toEqual(`https://${mockDmpId}`);
    // Make sure it has data from the DMP Tool extensions metadata in the `SK: EXTENSION#???` DynamoDB item
    expect(result[0]?.dmp?.featured).toEqual('no');

    expect(dynamoMock.calls()).toHaveLength(3);
    const firstCall: any = dynamoMock.call(0);
    expect(firstCall.args[0].input['ExpressionAttributeValues'][':pk']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(firstCall.args[0].input['ExpressionAttributeValues'][':sk']['S']).toEqual(`VERSION`);
    const lastCall: any = dynamoMock.call(1);
    expect(lastCall.args[0].input['ExpressionAttributeValues'][':pk']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(lastCall.args[0].input['ExpressionAttributeValues'][':sk']['S']).toEqual(`EXTENSION#${DMP_LATEST_VERSION}`);
  });


  it('should return undefined when DMP does not exist in DynamoDB', async () => {
    const mockDmpId = 'nonexistent-dmp-456';
    const mockVersion = 'latest';

    dynamoMock.on(QueryCommand).resolves({
      Items: undefined,
    });

    const result: DMPToolDMPType[] = await getDMPs(
      mockConfig,
      mockDomain,
      mockDmpId,
      mockVersion
    );

    expect(result).toEqual([]);
    expect(dynamoMock.calls()).toHaveLength(1);
  });

  it('should throw error when DynamoDB operation fails', async () => {
    const mockDmpId = 'test-dmp-789';
    const mockVersion = 'latest';
    const errorMessage = 'DynamoDB error';

    dynamoMock.on(QueryCommand).rejects(new Error(errorMessage));

    await expect(getDMPs(mockConfig, mockDomain, mockDmpId, mockVersion)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(1);
  });
});

describe('createDMP', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  const mockDmpId = 'doi.org/11.12345/A1B2C3';
  const mockVersion = '2025-01-15T12:00:00Z';
  const mockDMP: DMPToolDMPType = {
    dmp: {
      title: 'Test DMP',
      created: mockVersion,
      modified: mockVersion,
      dmp_id: {
        identifier: `https://${mockDmpId}`,
        type: 'doi'
      },
      rda_schema_version: '1.2',
      provenance: 'tester',
      featured: 'yes',
      privacy: 'public',
      status: 'complete',
      version: [{
        access_url: `https://${mockDomain}/dmps/${mockDmpId}`,
        version: mockVersion,
      }]
    }
  };

  it('should successfully create a DMP with version and extension records', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call to getDMPs
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${mockVersion}` },
          title: { S: 'Test DMP' },
          created: { S: mockVersion },
          modified: { S: mockVersion },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${mockVersion}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: mockVersion }
          }
        ]
      });

    const result = await createDMP(
      mockConfig,
      mockDomain,
      mockDmpId,
      mockDMP,
      mockVersion
    );

    expect(result).toEqual(mockDMP);
    expect(dynamoMock.calls()).toHaveLength(5);

    const firstCall: any = dynamoMock.call(0);
    expect(firstCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(firstCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#${mockVersion}`);
    expect(firstCall.args[0].input['Item']['title']['S']).toEqual('Test DMP');
    expect(firstCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(firstCall.args[0].input['Item']['modified']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(firstCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(firstCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const secondCall: any = dynamoMock.call(1);
    expect(secondCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(secondCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#${mockVersion}`);
    expect(secondCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(secondCall.args[0].input['Item']['featured']['S']).toEqual('yes');
    expect(secondCall.args[0].input['Item']['privacy']['S']).toEqual('public');
  });

  it('doesn\'t allow a version to be created if it already exists', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    // Call to DMPExists returns true
    dynamoMock.on(QueryCommand).resolvesOnce({ Items: [ { PK: { S: `DMP#${mockDmpId}` } }] });

    await expect(createDMP(mockConfig, mockDomain, mockDmpId, mockDMP, mockVersion)).rejects.toThrow();
  });

  it('should throw error when DynamoDB PutItem operation fails', async () => {
    const mockDmpId = 'doi.org/11.12345/A1B2C3';
    const mockVersion = '2025-01-15T12:00:00Z';
    const mockDMP: DMPToolDMPType = {
      dmp: {
        title: 'Test DMP',
        created: '2025-01-15T12:00:00Z',
        dmp_id: {
          identifier: `https://${mockDmpId}`,
          type: 'doi'
        }
      }
    };
    const errorMessage = 'DynamoDB PutItem error';

    dynamoMock.on(PutItemCommand).rejects(new Error(errorMessage));

    dynamoMock.on(QueryCommand).resolvesOnce({ Items: [] });

    await expect(createDMP(mockConfig, mockDomain, mockDmpId, mockDMP, mockVersion)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(1);
  });
});

describe('updateDMP', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  const mockDmpId = 'doi.org/11.12345/A1B2C3';
  const modifiedTstamp = new Date().toISOString();
  const mockDMP: DMPToolDMPType = {
    dmp: {
      title: 'Updated Test DMP',
      created: '2025-01-15T12:00:00Z',
      modified: modifiedTstamp,
      dmp_id: {
        identifier: `https://${mockDmpId}`,
        type: 'doi'
      },
      rda_schema_version: '1.2',
      provenance: 'tester',
      featured: 'no',
      privacy: 'private',
      status: 'complete',
      version: [
        {
          access_url: `https://${mockDomain}/dmps/${mockDmpId}`,
          version: modifiedTstamp,
        },
        {
          access_url: `https://${mockDomain}/dmps/${mockDmpId}?version=2025-01-15T12:00:00Z`,
          version: '2025-01-15T12:00:00Z',
        }
      ]
    }
  };

  it('should successfully update the DMP without creating a version snapshot', async () => {
    // We're not creating a new version snapshot, so define the expected output locally
    const dmp: DMPToolDMPType = {
      dmp: {
        title: 'Updated Test DMP',
        created: '2025-01-15T12:00:00Z',
        modified: modifiedTstamp,
        dmp_id: {
          identifier: `https://${mockDmpId}`,
          type: 'doi'
        },
        rda_schema_version: '1.2',
        provenance: 'tester',
        featured: 'no',
        privacy: 'private',
        status: 'complete',
        version: [
          {
            access_url: `https://${mockDomain}/dmps/${mockDmpId}`,
            version: modifiedTstamp,
          }
        ]
      }
    };
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: modifiedTstamp },          // current timestamp
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call getDMPs to get the latest version after update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Updated Test DMP' },         // Updated param
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: modifiedTstamp },  // Updated date
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'no' },                    // Updated param
          privacy: { S: 'private' },                // Updated param
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: modifiedTstamp }
          }
        ]
      });

    const result = await updateDMP(mockConfig, mockDomain, mockDmpId, dmp);

    expect(result).toEqual(dmp);
    expect(dynamoMock.calls()).toHaveLength(8);

    const thirdCall: any = dynamoMock.call(3);
    expect(thirdCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(thirdCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#${DMP_LATEST_VERSION}`);
    expect(thirdCall.args[0].input['Item']['title']['S']).toEqual('Updated Test DMP');
    expect(thirdCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(thirdCall.args[0].input['Item']['modified']['S']).toEqual(modifiedTstamp);
    expect(thirdCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(thirdCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const fourthCall: any = dynamoMock.call(4);
    expect(fourthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fourthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#${DMP_LATEST_VERSION}`);
    expect(fourthCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(fourthCall.args[0].input['Item']['featured']['S']).toEqual('no');
    expect(fourthCall.args[0].input['Item']['privacy']['S']).toEqual('private');
  });

  it('should successfully update the DMP and create a version snapshot when provenance changes', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'another-system' },              // A different provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call to getDMPs
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#2025-01-30T11:30:00Z` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#2025-01-30T11:30:00Z` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'another-system' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call getDMPs to get the latest version after update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Updated Test DMP' },         // Updated param
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: modifiedTstamp },  // Updated date
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // The new provenance
          featured: { S: 'no' },                    // Updated param
          privacy: { S: 'private' },                // Updated param
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#2025-01-15T12:00:00Z' },
            modified: { S: '2025-01-15T12:00:00Z' }
          },
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: modifiedTstamp }
          }
        ]
      });

    const result = await updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP);

    expect(result).toEqual(mockDMP);
    expect(dynamoMock.calls()).toHaveLength(13);

    // Test new version creation
    const fourthCall: any = dynamoMock.call(3);
    expect(fourthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fourthCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#2025-01-30T11:30:00Z`);
    expect(fourthCall.args[0].input['Item']['title']['S']).toEqual('Test DMP');
    expect(fourthCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(fourthCall.args[0].input['Item']['modified']['S']).toEqual('2025-01-30T11:30:00Z');
    expect(fourthCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(fourthCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const fifthCall: any = dynamoMock.call(4);
    expect(fifthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fifthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#2025-01-30T11:30:00Z`);
    expect(fifthCall.args[0].input['Item']['provenance']['S']).toEqual('another-system');
    expect(fifthCall.args[0].input['Item']['featured']['S']).toEqual('yes');
    expect(fifthCall.args[0].input['Item']['privacy']['S']).toEqual('public');

    // Test updates to latest version
    const ninthCall: any = dynamoMock.call(8);
    expect(ninthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(ninthCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#${DMP_LATEST_VERSION}`);
    expect(ninthCall.args[0].input['Item']['title']['S']).toEqual('Updated Test DMP');
    expect(ninthCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(ninthCall.args[0].input['Item']['modified']['S']).toEqual(modifiedTstamp);
    expect(ninthCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(ninthCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const tenthCall: any = dynamoMock.call(9);
    expect(tenthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(tenthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#${DMP_LATEST_VERSION}`);
    expect(tenthCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(tenthCall.args[0].input['Item']['featured']['S']).toEqual('no');
    expect(tenthCall.args[0].input['Item']['privacy']['S']).toEqual('private');
  });

  it('should successfully update the DMP and create a version snapshot when enough time has elapsed', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T05:30:00Z' },  // several hours ago
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call to getDMPs
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#2025-01-30T11:30:00Z` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T05:30:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#2025-01-30T11:30:00Z` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call getDMPs to get the latest version after update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Updated Test DMP' },         // Updated param
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: modifiedTstamp },  // Updated date
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'no' },                    // Updated param
          privacy: { S: 'private' },                // Updated param
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#2025-01-15T12:00:00Z' },
            modified: { S: '2025-01-15T12:00:00Z' }
          },
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: modifiedTstamp }
          }
        ]
      });

    const result = await updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP);

    expect(result).toEqual(mockDMP);
    expect(dynamoMock.calls()).toHaveLength(13);

    // Test new version creation
    const fourthCall: any = dynamoMock.call(3);
    expect(fourthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fourthCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#2025-01-30T05:30:00Z`);
    expect(fourthCall.args[0].input['Item']['title']['S']).toEqual('Test DMP');
    expect(fourthCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(fourthCall.args[0].input['Item']['modified']['S']).toEqual('2025-01-30T05:30:00Z');
    expect(fourthCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(fourthCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const fifthCall: any = dynamoMock.call(4);
    expect(fifthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fifthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#2025-01-30T05:30:00Z`);
    expect(fifthCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(fifthCall.args[0].input['Item']['featured']['S']).toEqual('yes');
    expect(fifthCall.args[0].input['Item']['privacy']['S']).toEqual('public');

    // Test updates to latest version
    const ninthCall: any = dynamoMock.call(8);
    expect(ninthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(ninthCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#${DMP_LATEST_VERSION}`);
    expect(ninthCall.args[0].input['Item']['title']['S']).toEqual('Updated Test DMP');
    expect(ninthCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(ninthCall.args[0].input['Item']['modified']['S']).toEqual(modifiedTstamp);
    expect(ninthCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(ninthCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');

    const tenthCall: any = dynamoMock.call(9);
    expect(tenthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(tenthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#${DMP_LATEST_VERSION}`);
    expect(tenthCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(tenthCall.args[0].input['Item']['featured']['S']).toEqual('no');
    expect(tenthCall.args[0].input['Item']['privacy']['S']).toEqual('private');
  });

  it('doesn\'t allow updates if the DMP does not have a latest version', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({ Items: [] })
      // Call to getDMPExtensions
      .resolvesOnce({ Items: [] })

    await expect(updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP)).rejects.toThrow();
  });

  it('doesn\'t allow updates if the DMP is tombstoned', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' },
          tombstoned: { S: '2025-01-30T11:30:00Z' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP)).rejects.toThrow();
  });

  it('doesn\'t allow updates if the modified date is older than the current modified date', async () => {
    const ONE_HOUR = 60 * 60 * 1000;
    const futureTStamp = new Date(Date.now() + ONE_HOUR).toISOString();

    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: futureTStamp },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP)).rejects.toThrow();
  });

  it('should throw error when DynamoDB PutItem operation fails', async () => {
    const errorMessage = 'DynamoDB PutItem error';

    dynamoMock.on(PutItemCommand).rejects(new Error(errorMessage));

    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(updateDMP(mockConfig, mockDomain, mockDmpId, mockDMP)).rejects.toThrow(errorMessage);
    expect(dynamoMock.calls()).toHaveLength(4);
  });
});

describe('tombstoneDMP', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  const mockDmpId = 'doi.org/11.12345/A1B2C3';
  const newDate = new Date().toISOString();
  const mockDMP: DMPToolDMPType = {
    dmp: {
      title: 'OBSOLETE: Test DMP',
      created: '2025-01-15T12:00:00Z',
      modified: newDate,
      dmp_id: {
        identifier: `https://${mockDmpId}`,
        type: 'doi'
      },
      rda_schema_version: '1.2',
      provenance: 'tester',
      featured: 'no',
      privacy: 'public',
      registered: '2025-01-15T12:00:00Z',
      status: 'complete',
      tombstoned: newDate,
      version: [{
        access_url: `https://${mockDomain}/dmps/${mockDmpId}`,
        version: newDate
      }]
    }
  };

  it('should successfully tombstone the DMP', async () => {
    dynamoMock.on(PutItemCommand).resolves({});
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-15T12:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' },
          registered: { S: '2025-01-15T12:00:00Z' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      })
      // Call getDMPs to get the latest version after update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_TOMBSTONE_VERSION}` },
          title: { S: 'OBSOLETE: Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: newDate },  // Updated date
          dmp_id: {
            M: {
              identifier: {S: `https://${mockDmpId}`},
              type: {S: 'doi'}
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_TOMBSTONE_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'no' },
          privacy: { S: 'public' },
          status: { S: 'complete' },
          registered: { S: '2025-01-15T12:00:00Z' },
          tombstoned: { S: newDate }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#tombstone' },
            modified: { S: newDate }
          }
        ]
      });

    const result = await tombstoneDMP(mockConfig, mockDomain, mockDmpId);

    expect(result).toEqual(mockDMP);
    expect(dynamoMock.calls()).toHaveLength(10);

    // Created the tombstone version
    const thirdCall: any = dynamoMock.call(3);
    expect(thirdCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(thirdCall.args[0].input['Item']['SK']['S']).toEqual(`VERSION#${DMP_TOMBSTONE_VERSION}`);
    expect(thirdCall.args[0].input['Item']['title']['S']).toEqual('OBSOLETE: Test DMP');
    expect(thirdCall.args[0].input['Item']['created']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(thirdCall.args[0].input['Item']['modified']['S'].split(':')[0]).toEqual(newDate.split(':')[0]);
    expect(thirdCall.args[0].input['Item']['dmp_id']['M']['identifier']['S']).toEqual(`https://${mockDmpId}`);
    expect(thirdCall.args[0].input['Item']['dmp_id']['M']['type']['S']).toEqual('doi');
    expect(thirdCall.args[0].input['Item']['tombstoned']).toBeUndefined();

    // Deleted the latest version
    const fourthCall: any = dynamoMock.call(4);
    expect(fourthCall.args[0].input['Key']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fourthCall.args[0].input['Key']['SK']['S']).toEqual(`VERSION#${DMP_LATEST_VERSION}`);

    // Created the extension for the tombstone version
    const fifthCall: any = dynamoMock.call(5);
    expect(fifthCall.args[0].input['Item']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(fifthCall.args[0].input['Item']['SK']['S']).toEqual(`EXTENSION#${DMP_TOMBSTONE_VERSION}`);
    expect(fifthCall.args[0].input['Item']['provenance']['S']).toEqual('tester');
    expect(fifthCall.args[0].input['Item']['featured']['S']).toEqual('yes');
    expect(fifthCall.args[0].input['Item']['privacy']['S']).toEqual('public');
    expect(fifthCall.args[0].input['Item']['registered']['S']).toEqual('2025-01-15T12:00:00Z');
    expect(fifthCall.args[0].input['Item']['tombstoned']['S'].split(':')[0]).toEqual(newDate.split(':')[0]);
    expect(fifthCall.args[0].input['Item']['title']).toBeUndefined();
    expect(fifthCall.args[0].input['Item']['modified']).toBeUndefined();

    // Deleted the extension for the latest version
    const sixthCall: any = dynamoMock.call(6);
    expect(sixthCall.args[0].input['Key']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(sixthCall.args[0].input['Key']['SK']['S']).toEqual(`EXTENSION#${DMP_LATEST_VERSION}`);
  });

  it('should not tombstone the DMP if it is not registered', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-15T12:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(tombstoneDMP(mockConfig, mockDomain, mockDmpId)).rejects.toThrow();
  });

  it('should not tombstone the DMP if it has no latest version', async () => {
    dynamoMock.on(PutItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({ Items: [] })
      // Call to getDMPExtensions
      .resolvesOnce({ Items: [] })

    await expect(tombstoneDMP(mockConfig, mockDomain, mockDmpId)).rejects.toThrow();
  });

  it('should throw error when DynamoDB PutItem operation fails', async () => {
    const errorMessage = 'DynamoDB PutItem error';

    dynamoMock.on(PutItemCommand).rejects(new Error(errorMessage));

    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'yes' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(tombstoneDMP(mockConfig, mockDomain, mockDmpId)).rejects.toThrow();
  });
});

describe('deleteDMP', () => {
  beforeEach(() => {
    dynamoMock.reset();
  });

  const mockDmpId = 'doi.org/11.12345/A1B2C3';
  const mockDMP: DMPToolDMPType = {
    dmp: {
      title: 'Test DMP',
      created: '2025-01-15T12:00:00Z',
      modified: '2025-01-15T12:00:00Z',
      dmp_id: {
        identifier: `https://${mockDmpId}`,
        type: 'doi'
      },
      rda_schema_version: '1.2',
      provenance: 'tester',
      featured: 'no',
      privacy: 'public',
      status: 'complete',
      version: [{
        access_url: `https://${mockDomain}/dmps/${mockDmpId}`,
        version: '2025-01-15T12:00:00Z'
      }]
    }
  };

  it('should successfully delete the DMP', async () => {
    dynamoMock.on(DeleteItemCommand).resolves({});
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before delete
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-15T12:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'no' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-15T12:00:00Z' }
          }
        ]
      });

    const result = await deleteDMP(mockConfig, mockDomain, mockDmpId);

    expect(result).toEqual(mockDMP);
    expect(dynamoMock.calls()).toHaveLength(4);

    // Created the tombstone version
    const thirdCall: any = dynamoMock.call(3);
    expect(thirdCall.args[0].input['Key']['PK']['S']).toEqual(`DMP#${mockDmpId}`);
    expect(thirdCall.args[0].input['Key']['SK']).toBeUndefined();
  });

  it('should not delete the DMP if it is registered', async () => {
    dynamoMock.on(DeleteItemCommand).resolves({})
    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-15T12:00:00Z' },
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },
          featured: { S: 'no' },
          privacy: { S: 'public' },
          status: { S: 'complete' },
          registered: { S: '2025-01-15T12:00:00Z' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(deleteDMP(mockConfig, mockDomain, mockDmpId)).rejects.toThrow();
  });

  it('should throw error when DynamoDB DeleteItem operation fails', async () => {
    const errorMessage = 'DynamoDB DeleteItem error';

    dynamoMock.on(DeleteItemCommand).rejects(new Error(errorMessage));

    dynamoMock.on(QueryCommand)
      // Call getDMPs to get the latest version before update
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `VERSION#${DMP_LATEST_VERSION}` },
          title: { S: 'Test DMP' },
          created: { S: '2025-01-15T12:00:00Z' },
          modified: { S: '2025-01-30T11:30:00Z' },  // 30 minutes before incoming
          dmp_id: {
            M: {
              identifier: { S: `https://${mockDmpId}` },
              type: { S: 'doi' }
            }
          }
        }]
      })
      // Call to getDMPExtensions
      .resolvesOnce({
        Items: [{
          PK: { S: `DMP#${mockDmpId}` },
          SK: { S: `EXTENSION#${DMP_LATEST_VERSION}` },
          rda_schema_version: { S: '1.2' },
          provenance: { S: 'tester' },              // Same provenance
          featured: { S: 'no' },
          privacy: { S: 'public' },
          status: { S: 'complete' }
        }]
      })
      // Fetch the versions
      .resolvesOnce({
        Items: [
          {
            PK: { S: `DMP#${mockDmpId}` },
            SK: { S: 'VERSION#latest' },
            modified: { S: '2025-01-01T00:00:00Z' }
          }
        ]
      });

    await expect(deleteDMP(mockConfig, mockDomain, mockDmpId)).rejects.toThrow();
  });
});
