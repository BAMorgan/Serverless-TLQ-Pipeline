# Serverless TLQ Pipeline CDK

This CDK app provisions the AWS resources needed to rebuild and test the TLQ pipeline:

- One private S3 bucket for source CSV files, transformed CSV files, and SQLite databases.
- Java Lambda functions for Transform, Load, and Query.
- Python Lambda functions for Transform, Load, and Query.
- IAM permissions for each Lambda to read/write the pipeline bucket.
- One-week CloudWatch log retention for the Lambda log groups.

The Java Lambda asset is built during `cdk synth/deploy` with Docker and Maven using the Java 17 Lambda bundling image. The Python Lambda asset is packaged directly from `python/src`.

## Prerequisites

- AWS CLI configured with credentials for the target account.
- Node.js and npm.
- Docker running locally for Java Lambda bundling.
- AWS CDK bootstrap already completed in the target account/region.

If the account has not been bootstrapped:

```bash
npx cdk bootstrap
```

## Deploy

```bash
cd infrastructure/cdk
npm install
npm run build
npm run deploy
```

The stack outputs the generated S3 bucket and Lambda function names.

By default, Lambda functions are named with the `serverless-tlq` prefix. If those names already exist in the account/region, deploy with a different prefix:

```bash
npm run deploy -- -c functionNamePrefix=<your-prefix>
```

## Upload Test Data

Upload the dataset file expected by the benchmark scripts, for example:

```bash
aws s3 cp 10000SalesRecords.csv s3://<DataBucketName>/10000SalesRecords.csv
```

## Run Benchmarks

Export the CDK output values before running the existing scripts:

```bash
export BUCKET_NAME=<DataBucketName>
export JAVA_TRANSFORM_FUNCTION=<JavaTransformFunctionName>
export JAVA_LOAD_FUNCTION=<JavaLoadFunctionName>
export JAVA_QUERY_FUNCTION=<JavaQueryFunctionName>
./java_callservice.sh
```

For Python:

```bash
export BUCKET_NAME=<DataBucketName>
export PYTHON_TRANSFORM_FUNCTION=<PythonTransformFunctionName>
export PYTHON_LOAD_FUNCTION=<PythonLoadFunctionName>
export PYTHON_QUERY_FUNCTION=<PythonQueryFunctionName>
./python_callservice.sh
```

## Destroy

This stack is configured as disposable test infrastructure. Destroying it also deletes the pipeline bucket and objects.

```bash
npm run destroy
```
