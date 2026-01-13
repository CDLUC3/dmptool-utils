# dmptool-aws

Functions that provide AWS functionality for the following DMPTool projects:
- Lambda Functions in [dmptool-infrastructure](https://github.com/CDLUC3/dmptool-infrastructure)
- Apollo Server in [dmsp_backend_prototype](https://github.com/CDLUC3/dmsp_backend_prototype)
- Narrative Generator Service in [dmptool-narrative-generator](https://github.com/CDLUC3/dmptool-narrative-generator)

## Table of Contents
- [AWS CloudFormation Stack Output Access](#cloudformation-support)
- [AWS DynamoDB Table Access](#dynamodb-support)
- [General Helper Functions](#general-helper-functions)
- [Logger Support (Pino with ECS formatting)](#logger-support-pino-with-ecs-formatting)
- [maDMP Support (serialization and deserialization)](#madmp-support-serialization-and-deserialization)
- [AWS RDS MySQL Database Access](#rds-mysql-support)
- [S3 Bucket Access](#s3-support)
- [SNS Topic Publication](#sns-support)
- [SSM Parameter Store Access](#ssm-support)

This package has the following dependencies:
- [@dmptool-types](https://github.com/CDLUC3/dmptool-types) For TypeScript types and Zod schemas to help with maDMP serialization/deserialization.
- [@elastic/ecs-pino-format](https://www.npmjs.com/package/@elastic/ecs-pino-format) For formatting logs for ECS (OpenSearch).
- [date-fns](https://www.npmjs.com/package/date-fns) For date manipulation.
- [jsonschema](https://www.npmjs.com/package/jsonschema) For validating maDMP JSON objects against the RDA Common Metadata Standard.
- [mysql2](https://www.npmjs.com/package/mysql2) For interacting with the RDS MySQL database.
- [pino](https://www.npmjs.com/package/pino) For logging.
- [pino-lambda](https://www.npmjs.com/package/pino-lambda) For setting up automatic request tracing in Pino logs.

This package also requires the following AWS dependencies. Note that the Lambda environment preinstalls these so you only need to include them in the `devDependencies` of your `package.json` for that environment.
- [@aws-sdk/client-cloudformation](https://www.npmjs.com/package/@aws-sdk/client-cloudformation) For interacting with CloudFormation stacks.
- [@aws-sdk/client-dynamodb](https://www.npmjs.com/package/@aws-sdk/client-dynamodb) For interacting with the RDS MySQL database.
- [@aws-sdk/client-s3](https://www.npmjs.com/package/@aws-sdk/client-s3) For interacting with S3 buckets.
- [@aws-sdk/s3-request-presigner](https://www.npmjs.com/package/@aws-sdk/s3-request-presigner) For generating pre-signed URLs for S3 objects.
- [@aws-sdk/client-sns](https://www.npmjs.com/package/@aws-sdk/client-sns) For publishing messages to SNS topics.
- [@aws-sdk/client-ssm](https://www.npmjs.com/package/@aws-sdk/client-ssm) For interacting with AWS Systems Manager Parameter Store.
- [@aws-sdk/util-dynamodb](https://www.npmjs.com/package/@aws-sdk/util-dynamodb) For converting DynamoDB items to JSON objects.

Many of these utilities require specific environment variables to be set. See each section below for specifics.

## CloudFormation Support
Provides access to CloudFormation stack outputs.

For example, our CloudFormation stack for the S3 buckets outputs the names of each bucket. This code allows a Lambda Function to access those bucket names.

Environment variable requirements:
- `AWS_REGION` The AWS region where the DynamoDB table is located

### Example usage
```typescript
import { getExport } from '@dmptool/dmptool-utils';

const tableName = await getExport('DynamoTableNames');
console.log(tableName);
```

## DynamoDB Support

This code can be used to access maDMP data stored in the DynamoDB table.

It supports:
- Checking if a DMP exists (tombstoned DMPs are considered non-existent)
- Retrieving a DMP by ID and version
- Retrieving all versions timestamps for a DMP ID
- Creating a DMP
- Updating a DMP
- Tombstoning a DMP
- Deleting a DMP
  
The code handles the logic to determine the correct DynamoDB partition key (PK) and sort key (SK). It also handles the separation of the RDA Common Standard portion of the DMP JSON from the DMP Tool extensions and stores them separately in the DynamoDB table to help prevent us from performing large reads when it is not necessary.

A DMP PK looks like this: `DMP#doi.org/11.12345/A6B7C9D0`
A DMP SK for the current version of the RDA Common Standard looks like this: `VERSION#latest`
A DMP SK for the current version of the DMP Tool extensions looks like this: `EXTENSION#latest`
A DMP SK for a specific version of the RDA Common Standard looks like this: `VERSION#2025-11-21T13:41:32.000Z`
A DMP SK for a specific version of the DMP Tool extensions looks like this: `EXTENSION#2025-11-21T13:41:32.000Z`

These keys are attached to the DMP JSON when persisting it to DynamoDB and removed when returning it from DynamoDB.

Environment variable requirements:
- `AWS_REGION` The AWS region where the DynamoDB table is located
- `DOMAIN_NAME` The domain name of the application
- `DYNAMODB_TABLE_NAME` The name of the DynamoDB table
- `DYNAMO_MAX_ATTEMPTS` The maximum number of times to retry a DynamoDB operation (defaults to 3)
- `VERSION_GRACE_PERIOD` The number of seconds to wait before considering a change should generate a version snapshot (defaults to 7200000 => 2 hours)

## Example Usage:
```typescript
import { DMPToolDMPType } from '@dmptool/types';
import {
  createDMP,
  deleteDMP,
  DMPExists,
  DMP_LATEST_VERSION,
  getDMPs,
  getDMPVersions,
  tombstoneDMP,
  updateDMP
} from 'dmptool-dynamo';

process.env.AWS_REGION = 'eu-west-1';
process.env.DOMAIN_NAME = 'my-application.org';
process.env.DYNAMODB_TABLE_NAME = 'my-dynamo-table';

const dmpId = '123456789';

const dmpObj: DMPToolDMPType = {
  dmp: {
    title: 'Test DMP',
    dmp_id: {
      identifier: dmpId,
      type: 'other'
    },
    created: '2021-01-01 03:11:23Z',
    modified: '2021-01-01 02:23:11Z',
    ethical_issues_exist: 'unknown',
    language: 'eng',
    contact: {
      name: 'Test Contact',
      mbox: 'tester@example.com',
      contact_id: [{
        identifier: '123456789',
        type: 'other'
      }]
    },
    dataset: [{
      title: 'Test Dataset',
      dataset_id: {
        identifier: 'your-application.projects.123.dmp.12.outputs.1',
        type: 'other'
      },
      personal_data: 'unknown',
      sensitive_data: 'no',
    }],
    rda_schema_version: "1.2",
    provenance: 'your-application',
    status: 'draft',
    privacy: 'private',
    featured: 'no',
  }
}

// First make sure the DMP doesn't already exist
const exists = await DMPExists(dmpId);
if (exists) {
  console.log('DMP already exists');

} else {
  // Create the DMP
  const created: DMPToolDMPType = await createDMP(dmpId, dmpObj);
  if (!created) { 
    console.log('Failed to create DMP');
  
  } else {
    dmpObj.dmp.privacy = 'public';
    dmpObj.dmp.modified = '2026-01-10T03:43:11Z';

    // Update the DMP
    const updated: DMPToolDMPType = await updateDMP(dmpObj);
    if (!updated) { 
      console.log('Failed to update DMP');
      
    } else {
      // Fetch the DMP version timestamps (should only be two)
      const versions = await getDMPVersions(dmpId);
      console.log(versions);
      
      // Fetch the latest version of the DMP
      const latest = await getDMP(dmpId, DMP_LATEST_VERSION);
      if (!latest) { 
        console.log('Failed to fetch latest version of DMP');
        
      } else {
        // If the DMP has a `registered` timestamp then it is published and can be tombstoned not deleted
        // Since our example is not, we include this code here for reference only
        
        // const tombstoned = await tombstoneDMP(dmpId);
        // console.log( tombstoned
        
        // Delete the DMP (can be done because the DMP is not published)
        const deleted = await deleteDMP(dmpId);
        console.log(deleted);
      }
    }
  }
}
```

## General Helper Functions

Generic helper functions:
- `areEqual`: Compares two values for equality (including deep equality for objects and arrays)
- `convertMySQLDateTimeToRFC3339`: Converts a MySQL datetime string to RFC3339 format
- `currentDateAsString`: Returns the current date as a string in YYYY-MM-DD format
- `isNullOrUndefined`: Checks if a value is null or undefined
- `normaliseHttpProtocol`: Normalizes the protocol of a URL to either http or https
- `randomHex`: Generates a random hex string of a specified length
- `removeNullAndUndefinedFromObject`: Removes all null and undefined values from an object.

Environment variable requirements:
- NONE

### Example usage
```typescript
import {
  areEqual,
  convertMySQLDateTimeToRFC3339,
  currentDateAsString,
  isNullOrUndefined,
  normaliseHttpProtocol,
  randomHex,
  removeNullAndUndefinedFromObject,
} from "dmptool-general";

console.log(areEqual("foo", "foo")); // Returns true
console.log(areEqual(123, "123")); // Returns false
console.log(areEqual("foo", undefined)); // Returns false
console.log(areEqual(["foo"], ["foo", "bar"])); // Returns false
console.log(areEqual({ a: "foo", c: "bar" }, { c: "bar", a: "foo" })); // Returns true
console.log(areEqual({ a: "foo", c: "bar" }, { a: "foo", c: { d: "bar" } })); // Returns false

console.log(convertMySQLDateTimeToRFC3339("2021-01-01 00:00:00")); // Returns "2021-01-01T00:00:00.000Z"

console.log(currentDateAsString()); // Returns "2021-01-01"

console.log(isNullOrUndefined(null)); // Returns true

console.log(normaliseHttpProtocol("http://www.example.com")); // Returns "https://www.example.com"

console.log(randomHex(16)); // Returns something like "a3f2c1b8e4d5f0a1"

console.log(removeNullAndUndefinedFromObject({ a: "foo", b: null, c: { d: "bar", e: undefined } }));
```

## Logger Support (Pino with ECS formatting)

This code can be used by Lambda Functions to provide access to a Pino logger formatted for ECS.

It provides a single `initializeLogger` function that can be used to create a Pino logger with standard formatting and a `LogLevel` enum that contains valid log levels.

Environment variable requirements:
- NONE

### Example usage
```typescript
import { Logger } from 'pino';
import { initializeLogger, LogLevel } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';

const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';

// Initialize the logger
const logger: Logger = initializeLogger('GenerateMaDMPRecordLambda', LogLevel[LOG_LEVEL]);

// Setup the LambdaRequestTracker for the logger
const withRequest = lambdaRequestTracker();

export const handler: Handler = async (event: EventBridgeEvent<string, EventBridgeDetails>, context: Context) => {
  // Log the incoming event and context
  logger.debug({ event, context }, 'Received event');
  
  // Initialize the logger by setting up automatic request tracing.
  withRequest(event, context);

  logger.info({ log_level: LOG_LEVEL, foo: "bar" }, 'Hello World!');
}
```

## maDMP Support (serialization and deserialization)

This Lambda Layer contains code that fetches data about a Plan from the RDS MySQL database and converts it into a JSON object that conforms to the RDA Common Metadata Standard for DMPs with DMP Tool extensions.

Details about the RDA Common Metadata Standard can be found in the JSON examples folder of their [repository](https://github.com/RDA-DMP-Common/RDA-DMP-Common-Standard)

**Current RDA Common Metadata Standard Version:** v1.2

Environment variable requirements:
- `AWS_REGION` - The AWS region where the Lambda Function is running
- `ENV`: The AWS environment (e.g. `dev`, `stg`, `prd`)
- `APPLICATION_NAME`: The name of your application (NO spaces!, this is used to construct identifier namespaces)
- `DOMAIN_NAME`: The domain name of your application

### Notes

**DMP IDs:**
Every Plan in the RDS MySQL database has a `dmpId` defined. These values are DOIs once they have been registered/minted with DataCite/EZID.

If a Plan has a `registered` date, then the `dmp_id` in the JSON object will be a DOI (e.g. `{ "identifier": "https://doi.org/11.22222/C3PO}", "type": "doi" }`). The DOI will resolve to the DMP's landing page.
If not, then the `dmp_id` will be the URL to access the Plan in the DMP Tool (e.g. `https://your-domain.com/projects/123/dmp/12`).

**Privacy:**
The `privacy` property in the JSON object represents the privacy level set in the DMP Tool. This should be used to determine whether the caller has access to the entire DMP (e.g. the narrative). Please adhere to this value when accessing the DMP.

**Datasets:**
The `dataset` property in the JSON object represents the Research Outputs associated with the DMP. If the DMP has no Research Outputs, the JSON object qill contain a default generic Dataset (see minimal JSON example below). This is included because the RDA Common Standard requires a Dataset to be present in the JSON object.

**Other IDs:**
The `project_id` and `dataset_id` properties in the JSON object are constructed in a manner that uses namespacing to allow them to be unique across all DMP systems (e.g. DMP Tool, DSW, DMP Online, etc.). These ids also allow us to tie them back to the records in the RDS MySQL database.

For example `your-domain.com.projects.123.dmp.12` ties to Project `123` and Plan `12` in the RDS MySQL database.

In situations where an identifier would normally resolve to a repository record (e.g. ROR, ORCID, re3data, etc.) and no value is found in the RDS MySQL database, we construct one that is unique and can be tied back to the record in the RDS MySQL database. For example: `your-application.projects.123.dmp.12.members.1` would be a unique identifier for a member of Project `123`, Plan `12` and PlanMember `1` in the RDS MySQL database.

**Ethical Issues:**
The `ethical_issues_exist` property in the JSON object is only set to `unknown` if the Plan has no defined Research Outputs OR the Ressearch Outputs do not capture the `personal_data` or `sensitive_data` properties. Otherwise, those properties determine whether the DMP contains ethical issues.

**Narrative:**
The `narrative` property in the JSON object represents the Template, Sections, Question text and Answers to the DMP within the DMP Tool.

### Example usage
```typescript
import { DMPToolDMPType } from '@dmptool/types';
import { planToDMPCommonStandard } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';
process.env.ENV = 'stg';
process.env.APPLICATION_NAME = 'your-application';
process.env.DOMAIN_NAME = 'your-domain.com';

const planId = '12345';
const dmp: DMPToolDMPType = await planToDMPCommonStandard(planId);
```

## Example of a minimal JSON object:
```
{
  dmp: {
    # RDA Common Standard properties:
    title: 'Test DMP',
    dmp_id: {
      identifier: 'https://your-domain.com/projects/123/dmp/12',
      type: 'other'
    },
    created: '2021-01-01 03:11:23Z',
    modified: '2021-01-01 02:23:11Z',
    ethical_issues_exist: 'unknown',
    language: 'eng',
    contact: {
      name: 'Test Contact',
      mbox: 'tester@example.com',
      contact_id: [{
        identifier: '123456789',
        type: 'other'
      }]
    },
    dataset: [{
      title: 'Test Dataset',
      dataset_id: {
        identifier: 'your-application.projects.123.dmp.12.outputs.1',
        type: 'other'
      },
      personal_data: 'unknown',
      sensitive_data: 'no',
    }],
    
    # DMP Tool extension properties:
    rda_schema_version: "1.2",
    provenance: 'your-application',
    privacy: 'private',
    featured: 'no'
  }
}
```

## Example of a complete JSON object:
```
{
  dmp: {
    # RDA Common Standard properties:
    title: 'Test DMP',
      description: 'This is a test DMP',
      dmp_id: {
      identifier: '123456789',
        type: 'other'
    },
    created: '2021-01-01 03:11:23Z',
    modified: '2021-01-01 02:23:11Z',
    ethical_issues_exist: 'yes',
    ethical_issues_description: 'This DMP contains ethical issues',
    ethical_issues_report: 'https://example.com/ethical-issues-report',
    language: 'eng',
    contact: {
      name: 'Test Contact',
        mbox: 'tester@example.com',
        contact_id: [{
        identifier: 'https://orcid.org/0000-0000-0000-0000',
        type: 'orcid'
      }],
        affiliation: [{
        name: 'Test University',
        affiliation_id: {
          identifier: 'https://ror.org/01234567890',
          type: 'ror'
        }
      }],
    },
    contributor: [{
      name: 'Test Contact',
      contributor_id: [{
        identifier: 'https://orcid.org/0000-0000-0000-0000',
        type: 'orcid'
      }],
      affiliation: [{
        name: 'Test University',
        affiliation_id: {
          identifier: 'https://ror.org/01234567890',
          type: 'ror'
        }
      }],
      role: ['https://example.com/roles/investigation', 'https://example.com/roles/other']
    }],
    cost: [{
      title: 'Budget Cost',
      description: 'Description of budget costs',
      value: 1234.56,
      currency_code: 'USD'
    }],
    dataset: [{
      title: 'Test Dataset',
      type: 'dataset',
      description: 'This is a test dataset',
      dataset_id: {
        identifier: 'your-application.projects.123.dmp.12.outputs.1',
        type: 'other'
      },
      personal_data: 'unknown',
      sensitive_data: 'no',
      data_quality_assurance: ['Statement about data quality assurance'],
      is_reused: false,
      issued: '2026-01-03',
      keyword: ['test', 'dataset'],
      language: 'eng',
      metadata: [{
        description: 'Description of metadata',
        language: 'eng',
        metadata_standard_id: [{
          identifier: 'https://example.com/metadata-standards/123',
          type: 'url'
        }]
      }],
      preservation_statement: 'Statement about preservation',
      security_and_privacy: [{
        title: 'Security and Privacy Statement',
        description: 'Description of security and privacy statement'
      }],
      alternate_identifier: [{
        identifier: 'https://example.com/dataset/123',
        type: 'url'
      }],
      technical_resource: [{
        name: 'Test Server',
        description: 'This is a test server',
        technical_resource_id: [{
          identifier: 'https://example.com/server/123',
          type: 'url'
        }],
      }],
      distribution: [{
        title: 'Test Distribution',
        description: 'This is a test distribution',
        access_url: 'https://example.com/dataset/123/distribution/123456789',
        download_url: 'https://example.com/dataset/123/distribution/123456789/download',
        byte_size: 123456789,
        format: ['application/zip'],
        data_access: 'open',
        issued: '2026-01-03',
        license: [{
          license_ref: 'https://spdx.org/licenses/CC-BY-4.0.html',
          start_date: '2026-01-03'
        }],
        host: {
          title: 'Test Host',
          description: 'This is a test host',
          url: 'https://re3data.org/2784y97245792756789',
          host_id: [{
            identifier: 'https://re3data.org/2784y97245792756789',
            type: 'url'
          }],
          availability: '99.99',
          backup_frequency: 'weekly',
          backup_type: 'tapes',
          certified_with: 'coretrustseal',
          geo_location: 'US',
          pid_system: ['doi', 'ark'],
          storage_type: 'LTO-8 tape',
          support_versioning: 'yes'
        }
      }]
    }],
    related_identifier: [{
      identifier: 'https://doi.org/10.1234/dmp.123456789',
      relation_type: 'cites',
      resource_type: 'dataset',
      type: 'doi'
    }],
    alternate_identifier: [{
      identifier: 'https://example.com/dmp/123456789',
      type: 'url'
    }],
  },
  project: [{
    title: 'Test Project',
    description: 'This is a test project',
    project_id: [{
      identifier: 'your-application.projects.123.dmp.12',
      type: 'other'
    }],
    start: '2025-01-01',
    end: '2028-01-31',
    funding: [{
      name: 'Funder Organization',
      funding_status: 'granted',
      funder_id: {
        identifier: 'https://ror.org/0987654321',
        type: 'ror'
      },
      grant_id: [{
        identifier: '123456789',
        type: 'other'
      }]
    }]
  }],
  
  # DMP Tool extension properties:
  rda_schema_version: "1.2",
  provenance: 'your-application',
  privacy: 'private',
  featured: 'no',
  registered: '2026-01-01T10:32:45Z',
  research_domain: {
    name: 'biology',
    research_domain_identifier: {
      identifier: 'https://example.com/01234567',
      type: 'url'
    }
  },
  research_facility: [{
    name: 'Super telescope',
    type: 'observatory',
    research_facility_identifier: {
      identifier: 'https://example.com/01234567',
      type: 'url'
    }
  }],
  funding_opportunity: [{
    # Used to tie the opportunity_identifier to a project[0].funding[?]
    project_id: {
      identifier: 'your-application.projects.123.dmp.12',
      type: 'other'
    },
    # Used to tie the opportunity_identifier to a project[0].funding[?]
    funder_id: {
      identifier: 'https://ror.org/0987654321',
      type: 'ror'
    },
    opportunity_identifier: {
      identifier: 'https://example.com/01234567',
      type: 'url'
    }
  }],
  funding_project: [{
    # Used to tie the opportunity_identifier to a project[0].funding[?]
    project_id: {
      identifier: 'your-application.projects.123.dmp.12',
      type: 'other'
    },
    funder_id: {
      identifier: 'https://ror.org/0987654321',
      type: 'ror'
    },
    project_identifier: {
      identifier: 'https://example.com/erbgierg',
      type: 'url'
    }
  }],
  version: [{
    access_url: 'https://example.com/dmps/123456789?version=2026-01-01T10:32:45Z',
    version: '2026-01-01T10:32:45Z',
  }],
  narrative: {
    # URL to fetch the narrative from the narrative generator (PDF by default but MIME type negotiation is supported)
    download_url: 'https://example.com/dmps/123456789/narrative',
    template: {
      id: 1234567,
      title: 'Narrative Template',
      description: 'This is a test template for a DMP narrative',
      version: 'v1',
      section: [{
        id: 9876,
        title: 'Section one',
        description: 'The first section of the narrative',
        order: 1,
        question: [{
          id: 1234,
          text: 'What is the purpose of this DMP?',
          order: 1,
          answer: {
            id: 543,
            json: {
              type: 'repositorySearch',
              answer: [{
                repositoryId: 'https://example.com/repository/123456789',
                repositoryName: 'Example Repository',
              }],
              meta: {schemaVersion: '1.0'}
            }
          },
        }]
      }]
    }
  }
}
```

## RDS MySQL Support

This code can be used by to provide access to the RDS MySQL database.

It provides a simple `queryTable` function which can be used to query a table. Similar to the way we do so within the Apollo server backend code.

Environment variable requirements:
- `AWS_REGION` - The AWS region where the Lambda Function is running
- `RDS_HOST` The endpoint of the RDS instance
- `RDS_PORT` The port (defaults to 3306)
- `RDS_USER` The name of the user (defaults to "root")
- `RDS_PASSWORD` The user's password
- `RDS_DATABASE` The name of the database

### Example usage
```typescript
import { queryTable } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';
process.env.RDS_HOST = 'some-rds-instance.us-east-1.rds.amazonaws.com';
process.env.RDS_PORT = '3306';
process.env.RDS_USER = 'my_user';
process.env.RDS_PASSWORD = 'open-sesame';
process.env.RDS_DATABASE = 'my_database';

const sql = 'SELECT * FROM some_table WHERE id = ?';
const id = 1234;
const resp = await queryTable(sql, [planId.toString()])

if (resp && Array.isArray(resp.results) && resp.results.length > 0) {
  console.log('It worked!', resp.results[0]);
} else {
  console.log('No results found');
}
```

## S3 Support

This code can be used to interact with objects in an S3 bucket.

It currently allows you to:
- List buckets
- Get an object from a bucket
- Put an object into a bucket
- Generate a pre-signed URL for an object in a bucket

Environment variable requirements:
- `AWS_REGION` - The AWS region where the Lambda Function is running

### Example usage
```typescript
import { getObject, getPresignedURL, listBuckets, putObject } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';

const bucketName = 'my-bucket';
const objectKey = 'my-object.txt';

const fileName = 'my-file.json.gz'
const gzippedData = zlib.gzipSync(JSON.stringify({ testing: { foo: 'bar' } }));

// List the objects to verify that we're able to access the bucket)
const s3Objects = await listObjects(bucketName, '');
console.log('Objects in bucket:', s3Objects);

// First put the item into the bucket
const response = await putObject(
  bucketName, 
  fileName, 
  gzippedData, 
  'application/json', 'gzip'
);

if (response) {
  console.log('Object uploaded successfully');

  // Get the object we just uploaded from the bucket
  const object = await getObject(bucketName, objectKey);
  console.log('Object fetched from bucket:', object);
  
  // Generate a presigned URL to access the object from outside the VPC
  const url = await getPresignedURL(bucketName, objectKey);
  console.log('Presigned URL to fetch the Object:', url);
  
  // Generate a presigned URL to put an object into the bucket from outside the VPC
  const putURL = await getPresignedURL(bucketName, `2nd-${objectKey}`, true);
  console.log('Presigned URL to put a new the Object into the bucket', putURL);
} else {
  console.log('Failed to upload object');
}
```

## SNS Messaging Support

This code can be used to publish messages to an SNS topic.

Environment variable requirements:
- `AWS_REGION` - The AWS region where the Lambda Function is running

### Example usage
```typescript
import { publishMessage } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';

const topicArn = 'arn:aws:sns:us-east-1:123456789012:my-topic';

// See the documentation for the AWS Lambda you are trying to invoke to determine what the
// `detail-type` and `detail` payload should look like.
const message = {
  'detail-type': 'my-event',  
  detail: {
    property1: 'value1',
    property2: 'value2'
  }
}

const response = await publishMessage(
  message,
  topicArn
);

if (response.statusCode === 200) {
  console.log('Message published successfully', response.body);
} else {
  console.log('Error publishing message', response.body);
}
```

## SSM Parameter Store Support

This code provides a simple `getSSMParameter` function which can be used to fetch an SSM Parameter.

Environment variable requirements:
- `AWS_REGION` - The AWS region where the Lambda Function is running
- `NODE_ENV` - The environment the Lambda Function is running in (e.g. `production`, `staging` or `development`)

The code will use that value to construct the appropriate prefix for the key. For example if you are running in the AWS development environment it will use `/uc3/dmp/tool/dev/` as the prefix.

### Example usage
```typescript
import { getSSMParameter } from '@dmptool/utils';

process.env.AWS_REGION = 'us-west-2';

const paramName = 'RdsDatabase';

const response = await getSSMParameter(paramName);

if (response) {
  console.log('SSM Parameter fetched successfully', response);
} else {
  console.log('Error fetching SSM Parameter');
}
```
