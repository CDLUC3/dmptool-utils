# dmptool-aws CHANGELOG

## v1.0.44
- Updated `loadNarrativeTemplateInfo` to return `customSections` and `customQuestions` data

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
