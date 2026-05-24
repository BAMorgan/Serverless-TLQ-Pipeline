#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { ServerlessTlqPipelineStack } from '../lib/serverless-tlq-pipeline-stack';

const app = new cdk.App();

new ServerlessTlqPipelineStack(app, 'ServerlessTlqPipelineStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
