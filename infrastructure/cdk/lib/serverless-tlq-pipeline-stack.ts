import * as path from 'node:path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';

export class ServerlessTlqPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const repoRoot = path.resolve(__dirname, '..', '..', '..');
    const memorySize = Number(this.node.tryGetContext('lambdaMemoryMb') ?? 256);
    const timeoutSeconds = Number(this.node.tryGetContext('lambdaTimeoutSeconds') ?? 900);
    const ephemeralStorageMb = Number(this.node.tryGetContext('lambdaEphemeralStorageMb') ?? 2048);
    const functionNamePrefix = String(
      this.node.tryGetContext('functionNamePrefix') ?? 'serverless-tlq',
    );

    const dataBucket = new s3.Bucket(this, 'DataBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const javaCode = lambda.Code.fromAsset(path.join(repoRoot, 'java'), {
      bundling: {
        image: lambda.Runtime.JAVA_17.bundlingImage,
        command: [
          'bash',
          '-lc',
          [
            'set -euo pipefail',
            'cp -R /asset-input/. /tmp/build',
            'cd /tmp/build',
            'mvn -q -DskipTests package',
            'cp target/tlq-pipeline-lambda-1.0-SNAPSHOT.jar /asset-output/tlq-pipeline-lambda-1.0-SNAPSHOT.jar',
          ].join(' && '),
        ],
        outputType: cdk.BundlingOutput.ARCHIVED,
      },
    });

    const pythonCode = lambda.Code.fromAsset(path.join(repoRoot, 'python', 'src'), {
      exclude: ['__pycache__', '*.pyc'],
    });

    const commonEnvironment = {
      TRANSFORMED_CSV_BUCKET_NAME: dataBucket.bucketName,
    };

    const createLogGroup = (id: string, functionName: string) =>
      new logs.LogGroup(this, `${id}LogGroup`, {
        logGroupName: `/aws/lambda/${functionName}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

    const javaTransformName = `${functionNamePrefix}-java-transform`;
    const javaLoadName = `${functionNamePrefix}-java-load`;
    const javaQueryName = `${functionNamePrefix}-java-query`;
    const pythonTransformName = `${functionNamePrefix}-python-transform`;
    const pythonLoadName = `${functionNamePrefix}-python-load`;
    const pythonQueryName = `${functionNamePrefix}-python-query`;

    const javaDefaults = {
      runtime: lambda.Runtime.JAVA_17,
      code: javaCode,
      memorySize,
      timeout: cdk.Duration.seconds(timeoutSeconds),
      ephemeralStorageSize: cdk.Size.mebibytes(ephemeralStorageMb),
      environment: commonEnvironment,
    };

    const pythonDefaults = {
      runtime: lambda.Runtime.PYTHON_3_12,
      code: pythonCode,
      memorySize,
      timeout: cdk.Duration.seconds(timeoutSeconds),
      ephemeralStorageSize: cdk.Size.mebibytes(ephemeralStorageMb),
      environment: commonEnvironment,
    };

    const javaTransform = new lambda.Function(this, 'JavaTransform', {
      ...javaDefaults,
      functionName: javaTransformName,
      handler: 'lambda.Transform::handleRequest',
      logGroup: createLogGroup('JavaTransform', javaTransformName),
    });

    const javaLoad = new lambda.Function(this, 'JavaLoad', {
      ...javaDefaults,
      functionName: javaLoadName,
      handler: 'lambda.Load::handleRequest',
      logGroup: createLogGroup('JavaLoad', javaLoadName),
    });

    const javaQuery = new lambda.Function(this, 'JavaQuery', {
      ...javaDefaults,
      functionName: javaQueryName,
      handler: 'lambda.Query::handleRequest',
      logGroup: createLogGroup('JavaQuery', javaQueryName),
    });

    const pythonTransform = new lambda.Function(this, 'PythonTransform', {
      ...pythonDefaults,
      functionName: pythonTransformName,
      handler: 'Transform.lambda_handler',
      logGroup: createLogGroup('PythonTransform', pythonTransformName),
    });

    const pythonLoad = new lambda.Function(this, 'PythonLoad', {
      ...pythonDefaults,
      functionName: pythonLoadName,
      handler: 'Load.lambda_handler',
      logGroup: createLogGroup('PythonLoad', pythonLoadName),
    });

    const pythonQuery = new lambda.Function(this, 'PythonQuery', {
      ...pythonDefaults,
      functionName: pythonQueryName,
      handler: 'Query.lambda_handler',
      logGroup: createLogGroup('PythonQuery', pythonQueryName),
    });

    [
      javaTransform,
      javaLoad,
      javaQuery,
      pythonTransform,
      pythonLoad,
      pythonQuery,
    ].forEach((fn) => dataBucket.grantReadWrite(fn));

    new cdk.CfnOutput(this, 'DataBucketName', {
      value: dataBucket.bucketName,
      description: 'Upload SalesRecords CSV files here before running benchmark scripts.',
    });
    new cdk.CfnOutput(this, 'JavaTransformFunctionName', { value: javaTransform.functionName });
    new cdk.CfnOutput(this, 'JavaLoadFunctionName', { value: javaLoad.functionName });
    new cdk.CfnOutput(this, 'JavaQueryFunctionName', { value: javaQuery.functionName });
    new cdk.CfnOutput(this, 'PythonTransformFunctionName', { value: pythonTransform.functionName });
    new cdk.CfnOutput(this, 'PythonLoadFunctionName', { value: pythonLoad.functionName });
    new cdk.CfnOutput(this, 'PythonQueryFunctionName', { value: pythonQuery.functionName });
  }
}
