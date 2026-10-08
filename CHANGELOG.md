# dmptool-aws CHANGELOG

- Fixed research outputs with blank fields making the DMP fail RDA Common Standard validation (e.g. when downloading the DMP). Only the title is required on the Research Output form, but blank columns were exported as-is (e.g. `issued: ""`, and repositories, metadata standards and licenses with an empty ID). Blank fields and entries with no ID are now left out of the export
- The form's `restricted` access level is now exported as the RDA Common Standard's `shared` (`restricted` isn't an allowed `data_access` value), and a missing access level defaults to `closed`
- A license's `start_date` (required by the RDA Common Standard) is the Anticipated Release Date, or the current date when there isn't one
- Fixed `PB` file sizes not being converted to bytes


## v2.2.0
- Added `displayLogic` helpers (`isQuestionVisible`, `findHiddenQuestionIds`, `evaluateCondition`, `extractAnswerValues`) that decide which questions are hidden by their display (conditional) logic, so every service applies the same rules
- Fixed research outputs never being added to the DMP `dataset`: the query looked for the answer type `researchOutputsTable` instead of `researchOutputTable`, and the `answers.json` value (returned by mysql2 as an object) was passed to `JSON.parse`
- `planToDMPCommonStandard` now applies display (conditional) logic: questions hidden by their display logic are left out of the narrative, and research outputs entered on a hidden question are left out of the `dataset` list. The answers to hidden questions are not changed in the database
- Type-check the tests: `tsconfig.json` now includes `src/__tests__` (TypeScript 6 no longer includes `@types/jest` automatically, so editors reported errors in the excluded test files), and the build uses the new `tsconfig.build.json`, which still excludes them from the build
- Removed an `it.only` in `maDMP.spec.ts` that was skipping 12 tests, and fixed the 4 of them that were failing (out of date mocks and a wrong assertion)

## v2.1.9
- Fixed double protocol issues `download_url` and `access_url` by using `ensureHttpsProtocol` with them.

## v2.1.8
- Added `relationType` field to the Related works query in the maDMP generator
- Upgraded `@dmptool/types` to 4.0.1
- Removed old overrides for `@babel/core` and `js-yaml` since they are no longer needed
- Added override for `brace-expansion` to address security vulnerability

## v2.1.7
- Fixed bug with loading a plan's alternate identifiers

## v2.1.6
- Update `@dmptool/types` dependency to `v4.0.0` to support the new RDA Common Standard fields in the DMP Tool extensions schema
- Updated other dependencies and added overrides for `@babel/core` and `js-yaml`

## v2.1.5
- Added `alternate_identifier` array to the maDMP generator
- Updated all Jest tests to include Jest global imports and added type `never` for mock resolved values

## v2.1.4
- Updated dependencies
- Switched to Typescript 6 and NodeNext module resolution

## v2.1.3
- Added a `removeObject` function to `s3.ts` for deleting objects from S3 buckets

## v2.1.2
- Added `getPresignedURLForImageUpload` function to `s3.ts` for generating presigned POST URLs for image uploads
- Installed `@aws-sdk/s3-presigned-post@3.1039.0` dependency
- Updated `aws-sdk` dependencies and removed unneeded overrides

## v2.1.1
- Bump version to 2.1.1 to bring us back in line with rogue v2.1.0 which was deployed to test changes to MySQL RDS

## v2.0.4
- Removed `ts-node-dev`, `ts-node`, and `jest-expect-message` from `package.json` since they are not used in this app. Plus `ts-node-dev` and `jest-expect-message` have not been updated for over three years.
- Replaced `@aws-sdk/util-stream-node` with `@smithy/util-stream`. `@aws-sdk/util-stream-node` was deprecated as part of a move to decouple the core SDK components from AWS-specific namespace. These generic utilities now live under the @smithy namespace.
- Updated version of `fast-xml-builder` to `v1.2.0` to address security vulnerability

## v2.0.3
- Updated RDS query to accepted both positional and named parameters

## v2.0.2
- Updated dependencies

## v2.0.1
- Update `maDMP` to only include Related Works that have been `ACCEPTED`
- Updated RDS connection to allow for named parameters instead of just `?` placeholders

## v2.0.0
- Updated `loadNarrativeTemplateInfo` to return `customSections` and `customQuestions` data, and updated `DMPExtensionNarrativeQuestion` and `DMPExtensionNarrativeSection` types

## v1.0.43
- Update `aws-sdk` dependencies and add override for `fast-xml-parser`
- Remove outdated override for `minimatch`

## v1.0.42
- Updated override for `minimatch` and upgraded all dependencies
- Updated `renovate` config

## v1.0.41
- Added override for minimatch and upgrade all dependencies

## v1.0.6
- Removed all references to `process.env` and instead added those values as input arguments

## v1.0.0
- Ported over initial `cloudFormation` code from old `dmsp_api-_prototype` repo
- Ported over initial `dynamo` code from `dmsp_backend_prototype` repo's Dynamo datasource
- Ported over initial `general` code from old `dmsp_api-_prototype` and `dmsp_backend_prototype` repos
- Ported over initial `maDMP` code from `dmsp_backend_prototype` repo's token service
- Ported over initial `rds` code from old `dmsp_api-_prototype` and `dmsp_backend_prototype` repos
- Ported over initial `s3` code from old `dmsp_api-_prototype` repo
- Added new `eventBridge` file
- Ported over initial `ssm` code from old `dmsp_api-_prototype` repo
- Added unit tests, README and CHANGELOG documentation
