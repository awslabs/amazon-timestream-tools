#!/usr/bin/env node

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as cdk from 'aws-cdk-lib';
import { ManagedFlinkExampleStack } from '../lib/managed-flink-example-stack';

function stringContext(app: cdk.App, key: string, defaultValue: string): string {
  const value = app.node.tryGetContext(key);
  return value === undefined ? defaultValue : String(value);
}

function optionalStringContext(app: cdk.App, key: string): string | undefined {
  const value = app.node.tryGetContext(key);
  if (value === undefined) {
    return undefined;
  }
  const parsed = String(value).trim();
  if (parsed.length === 0) {
    throw new Error(`CDK context '${key}' must not be blank`);
  }
  return parsed;
}

function stringListContext(app: cdk.App, key: string): string[] | undefined {
  const value = app.node.tryGetContext(key);
  if (value === undefined) {
    return undefined;
  }

  const values = Array.isArray(value)
    ? value.map(String)
    : String(value).split(',');
  const parsed = values.map((item) => item.trim()).filter(Boolean);
  if (parsed.length === 0) {
    throw new Error(`CDK context '${key}' must contain at least one ID`);
  }
  return parsed;
}

function optionalNumberContext(app: cdk.App, key: string): number | undefined {
  const value = optionalStringContext(app, key);
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`CDK context '${key}' must be a number`);
  }
  return parsed;
}

function optionalBooleanContext(app: cdk.App, key: string): boolean | undefined {
  const value = optionalStringContext(app, key);
  if (value === undefined) {
    return undefined;
  }
  if (value !== 'true' && value !== 'false') {
    throw new Error(`CDK context '${key}' must be true or false`);
  }
  return value === 'true';
}

// Accepts a JSON object of string values, for example
// -c sinkProperties='{"sink.batch.max-size":"50000"}'.
function optionalPropertiesContext(
  app: cdk.App,
  key: string,
): Record<string, string> | undefined {
  const value = app.node.tryGetContext(key);
  if (value === undefined) {
    return undefined;
  }
  let parsed: unknown = value;
  if (typeof value === 'string') {
    try {
      parsed = JSON.parse(value);
    } catch {
      throw new Error(`CDK context '${key}' must be a JSON object`);
    }
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`CDK context '${key}' must be a JSON object`);
  }
  return Object.fromEntries(
    Object.entries(parsed as Record<string, unknown>).map(([name, entry]) => [
      name,
      String(entry),
    ]),
  );
}

function applicationJarPath(app: cdk.App): string {
  const configuredPath = optionalStringContext(app, 'applicationJarPath');
  if (configuredPath === undefined) {
    throw new Error(
      "CDK context 'applicationJarPath' is required; " +
        "pass '-c applicationJarPath=/absolute/path/to/application-all.jar'",
    );
  }

  const resolvedPath = path.resolve(__dirname, '..', configuredPath);
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`Application JAR does not exist: ${resolvedPath}`);
  }
  if (!fs.statSync(resolvedPath).isFile()) {
    throw new Error(`Application JAR path is not a file: ${resolvedPath}`);
  }
  if (path.extname(resolvedPath).toLowerCase() !== '.jar') {
    throw new Error(`Application artifact must be a .jar file: ${resolvedPath}`);
  }
  return resolvedPath;
}

const app = new cdk.App();

new ManagedFlinkExampleStack(app, 'ManagedFlinkInfluxDB3ExampleStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  applicationJarPath: applicationJarPath(app),
  lifecycleCodePath: path.resolve(
    __dirname,
    '../lambda/flink-application-lifecycle',
  ),
  applicationName: stringContext(app, 'applicationName', 'influxdb3-flink-example'),
  databaseName: stringContext(app, 'databaseName', 'sensors'),
  flinkRuntime: stringContext(app, 'flinkRuntime', 'FLINK-2_3'),
  pipeline: stringContext(app, 'pipeline', 'datastream'),
  influxDbEndpoint: optionalStringContext(app, 'influxDbEndpoint'),
  influxDbSecretArn: optionalStringContext(app, 'influxDbSecretArn'),
  influxDbTokenField: stringContext(app, 'influxDbTokenField', 'token'),
  influxDbSecretKmsKeyArn: optionalStringContext(
    app,
    'influxDbSecretKmsKeyArn',
  ),
  influxDbClusterName: optionalStringContext(app, 'influxDbClusterName'),
  parallelism: optionalNumberContext(app, 'parallelism'),
  parallelismPerKpu: optionalNumberContext(app, 'parallelismPerKpu'),
  checkpointingEnabled: optionalBooleanContext(app, 'checkpointingEnabled'),
  checkpointIntervalMillis: optionalNumberContext(app, 'checkpointIntervalMillis'),
  dataGenProperties: optionalPropertiesContext(app, 'dataGenProperties'),
  sinkProperties: optionalPropertiesContext(app, 'sinkProperties'),
  flinkSubnetIds: stringListContext(app, 'flinkSubnetIds'),
  flinkSecurityGroupIds: stringListContext(app, 'flinkSecurityGroupIds'),
});
