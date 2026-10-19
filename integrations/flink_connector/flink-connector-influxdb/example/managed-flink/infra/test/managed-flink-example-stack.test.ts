/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'node:assert/strict';
import * as path from 'node:path';
import { test } from 'node:test';
import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import {
  ManagedFlinkExampleStack,
  ManagedFlinkExampleStackProps,
} from '../lib/managed-flink-example-stack';

function synthesizeTemplate(
  overrides: Partial<ManagedFlinkExampleStackProps> = {},
): Template {
  const app = new cdk.App();
  const stack = new ManagedFlinkExampleStack(app, 'TestStack', {
    env: {
      account: '111111111111',
      region: 'us-east-1',
    },
    applicationJarPath: path.resolve(
      __dirname,
      '../../test/fixtures/application-asset.txt',
    ),
    lifecycleCodePath: path.resolve(
      __dirname,
      '../../lambda/flink-application-lifecycle',
    ),
    ...overrides,
  });
  return Template.fromStack(stack);
}

test('accepts an existing InfluxDB endpoint and secret as parameters', () => {
  const template = synthesizeTemplate();

  template.hasParameter('InfluxDbEndpoint', {
    Type: 'String',
    AllowedPattern:
      '^https?://(?:\\[[0-9A-Fa-f:]+\\]|[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*)' +
      '(?::[0-9]{1,5})?/?$',
  });
  template.hasParameter('InfluxDbSecretArn', {
    Type: 'String',
    AllowedPattern: 'arn:[^:]+:secretsmanager:[^:]+:[0-9]{12}:secret:.+',
  });
  template.resourceCountIs('AWS::Timestream::InfluxDBCluster', 0);
  template.resourceCountIs('AWS::EC2::VPC', 0);
  template.resourceCountIs('Custom::InfluxDBParameterGroup', 0);
  template.resourceCountIs('Custom::InfluxDB3Database', 0);
});

test('rejects an InfluxDB endpoint without a valid URI host', () => {
  assert.throws(
    () =>
      synthesizeTemplate({
        influxDbEndpoint: 'https://YOUR_ENDPOINT:8086',
      }),
    /influxDbEndpoint must be an HTTP\(S\) base URL with a valid host/,
  );
});

test('accepts a concrete InfluxDB endpoint with a valid host', () => {
  const endpoint =
    'https://0123456789-abcdefghijkl.us-east-1.timestream-influxdb.amazonaws.com:8086';
  const template = synthesizeTemplate({
    influxDbEndpoint: endpoint,
  });

  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      EnvironmentProperties: {
        PropertyGroups: Match.arrayWith([
          Match.objectLike({
            PropertyGroupId: 'InfluxDB3',
            PropertyMap: Match.objectLike({
              endpoint,
            }),
          }),
        ]),
      },
    }),
  });
});

test('accepts an InfluxDB database name containing underscores', () => {
  const databaseName = 'my_sensors';
  const template = synthesizeTemplate({ databaseName });

  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      EnvironmentProperties: {
        PropertyGroups: Match.arrayWith([
          Match.objectLike({
            PropertyGroupId: 'InfluxDB3',
            PropertyMap: Match.objectLike({
              database: databaseName,
            }),
          }),
        ]),
      },
    }),
  });
});

test('deploys the Maven bundle as a Managed Flink application', () => {
  const template = synthesizeTemplate();

  template.resourceCountIs('AWS::KinesisAnalyticsV2::Application', 1);
  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationMode: 'STREAMING',
    RuntimeEnvironment: 'FLINK-2_3',
    ApplicationConfiguration: Match.objectLike({
      ApplicationCodeConfiguration: Match.objectLike({
        CodeContentType: 'ZIPFILE',
        CodeContent: Match.objectLike({
          S3ContentLocation: Match.objectLike({
            BucketARN: Match.anyValue(),
            FileKey: Match.anyValue(),
          }),
        }),
      }),
      EnvironmentProperties: {
        PropertyGroups: Match.arrayWith([
          Match.objectLike({
            PropertyGroupId: 'KinesisSource',
            PropertyMap: Match.objectLike({
              initialPosition: 'LATEST',
            }),
          }),
          Match.objectLike({
            PropertyGroupId: 'InfluxDB3',
            PropertyMap: {
              endpoint: { Ref: 'InfluxDbEndpoint' },
              database: 'sensors',
              secretId: { Ref: 'InfluxDbSecretArn' },
              secretJsonField: 'token',
              region: {
                'Fn::Select': [
                  3,
                  {
                    'Fn::Split': [':', { Ref: 'InfluxDbSecretArn' }],
                  },
                ],
              },
            },
          }),
        ]),
      },
      FlinkApplicationConfiguration: Match.objectLike({
        ParallelismConfiguration: Match.objectLike({
          AutoScalingEnabled: false,
          Parallelism: 1,
        }),
      }),
    }),
  });

  const resources = template.findResources(
    'AWS::KinesisAnalyticsV2::Application',
  );
  const application = Object.values(resources)[0];
  assert.equal(
    application.Properties.ApplicationConfiguration.VpcConfigurations,
    undefined,
  );
});

test('grants access to only the configured token secret', () => {
  const template = synthesizeTemplate();
  const serializedTemplate = JSON.stringify(template.toJSON());

  assert.match(serializedTemplate, /secretsmanager:GetSecretValue/);
  assert.match(serializedTemplate, /InfluxDbSecretArn/);
  assert.doesNotMatch(serializedTemplate, /timestream-influxdb:/);
});

test('uses supplied networking for a private InfluxDB endpoint', () => {
  const template = synthesizeTemplate({
    flinkSubnetIds: ['subnet-11111111', 'subnet-22222222'],
    flinkSecurityGroupIds: ['sg-11111111'],
  });

  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      VpcConfigurations: [
        {
          SecurityGroupIds: ['sg-11111111'],
          SubnetIds: ['subnet-11111111', 'subnet-22222222'],
        },
      ],
    }),
  });
  assert.match(
    JSON.stringify(template.toJSON()),
    /ec2:CreateNetworkInterface/,
  );
  assert.match(
    JSON.stringify(template.toJSON()),
    /ec2:CreateNetworkInterfacePermission/,
  );
  assert.match(JSON.stringify(template.toJSON()), /ec2:DescribeDhcpOptions/);
});

test('grants KMS decrypt when a customer-managed secret key is supplied', () => {
  const template = synthesizeTemplate({
    influxDbSecretKmsKeyArn:
      'arn:aws:kms:us-east-1:111111111111:key/11111111-2222-3333-4444-555555555555',
  });

  const serializedTemplate = JSON.stringify(template.toJSON());
  assert.match(serializedTemplate, /kms:Decrypt/);
  assert.match(serializedTemplate, /11111111-2222-3333-4444-555555555555/);
});

test('starts and stops the Managed Flink application with a custom resource', () => {
  const template = synthesizeTemplate();

  template.resourceCountIs('Custom::FlinkApplicationLifecycle', 1);
  template.hasResourceProperties('Custom::FlinkApplicationLifecycle', {
    ServiceTimeout: '3300',
    ApplicationName: Match.anyValue(),
    DeploymentFingerprint: Match.anyValue(),
  });

  const serializedTemplate = JSON.stringify(template.toJSON());
  assert.match(serializedTemplate, /kinesisanalytics:StartApplication/);
  assert.match(serializedTemplate, /kinesisanalytics:StopApplication/);
  assert.match(serializedTemplate, /kinesisanalytics:DescribeApplication/);
  assert.match(serializedTemplate, /kinesisanalytics:ListApplicationOperations/);
  assert.match(
    serializedTemplate,
    /kinesisanalytics:DescribeApplicationOperation/,
  );
  assert.doesNotMatch(serializedTemplate, /apiv3_/);
});

function propertyGroups(template: Template): Record<string, Record<string, unknown>> {
  const resources = template.findResources(
    'AWS::KinesisAnalyticsV2::Application',
  );
  const application = Object.values(resources)[0];
  const groups =
    application.Properties.ApplicationConfiguration.EnvironmentProperties
      .PropertyGroups as { PropertyGroupId: string; PropertyMap: Record<string, unknown> }[];
  return Object.fromEntries(
    groups.map((group) => [group.PropertyGroupId, group.PropertyMap]),
  );
}

test('selects the DataStream pipeline by default', () => {
  const groups = propertyGroups(synthesizeTemplate());

  assert.deepEqual(groups.Application, { pipeline: 'datastream' });
});

test('selects the SQL pipeline', () => {
  const groups = propertyGroups(synthesizeTemplate({ pipeline: 'sql' }));

  assert.deepEqual(groups.Application, { pipeline: 'sql' });
});

test('accepts the pipeline in any case', () => {
  const groups = propertyGroups(synthesizeTemplate({ pipeline: 'SQL' }));

  assert.deepEqual(groups.Application, { pipeline: 'sql' });
});

test('rejects an unknown pipeline', () => {
  assert.throws(
    () => synthesizeTemplate({ pipeline: 'table' }),
    /pipeline must be one of datastream, sql, datagen/,
  );
});

test('runs the datagen pipeline without a Kinesis stream', () => {
  const template = synthesizeTemplate({
    pipeline: 'datagen',
    dataGenProperties: { ratePerSecond: '1000', runId: 'smoke' },
  });
  const groups = propertyGroups(template);

  template.resourceCountIs('AWS::Kinesis::Stream', 0);
  assert.deepEqual(groups.Application, { pipeline: 'datagen' });
  assert.equal(groups.KinesisSource, undefined);
  assert.deepEqual(groups.DataGen, { ratePerSecond: '1000', runId: 'smoke' });
  assert.ok(groups.InfluxDB3 !== undefined);
  assert.equal(template.findOutputs('KinesisStreamName').KinesisStreamName, undefined);
  assert.doesNotMatch(JSON.stringify(template.toJSON()), /kinesis:GetRecords/);
});

test('omits empty optional property groups', () => {
  const groups = propertyGroups(
    synthesizeTemplate({ pipeline: 'datagen', dataGenProperties: {}, sinkProperties: {} }),
  );

  assert.equal(groups.DataGen, undefined);
  assert.equal(groups.Sink, undefined);
});

test('passes Sink properties to the application', () => {
  const groups = propertyGroups(
    synthesizeTemplate({ sinkProperties: { 'sink.batch.max-size': '50000' } }),
  );

  assert.deepEqual(groups.Sink, { 'sink.batch.max-size': '50000' });
});

test('rejects DataGen properties for other pipelines', () => {
  assert.throws(
    () => synthesizeTemplate({ dataGenProperties: { ratePerSecond: '1' } }),
    /dataGenProperties requires the datagen pipeline/,
  );
});

test('configures parallelism and checkpointing', () => {
  const template = synthesizeTemplate({
    parallelism: 8,
    parallelismPerKpu: 2,
    checkpointingEnabled: true,
    checkpointIntervalMillis: 30_000,
  });

  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      FlinkApplicationConfiguration: Match.objectLike({
        ParallelismConfiguration: {
          ConfigurationType: 'CUSTOM',
          AutoScalingEnabled: false,
          Parallelism: 8,
          ParallelismPerKPU: 2,
        },
        CheckpointConfiguration: {
          ConfigurationType: 'CUSTOM',
          CheckpointingEnabled: true,
          CheckpointInterval: 30_000,
        },
      }),
    }),
  });
});

test('keeps checkpointing disabled by default', () => {
  synthesizeTemplate().hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      FlinkApplicationConfiguration: Match.objectLike({
        CheckpointConfiguration: {
          ConfigurationType: 'CUSTOM',
          CheckpointingEnabled: false,
        },
      }),
    }),
  });
});

test('rejects invalid parallelism settings', () => {
  assert.throws(
    () => synthesizeTemplate({ parallelism: 0 }),
    /parallelism must be an integer of at least 1/,
  );
  assert.throws(
    () => synthesizeTemplate({ parallelism: 2.5 }),
    /parallelism must be an integer of at least 1/,
  );
  assert.throws(
    () => synthesizeTemplate({ parallelismPerKpu: 9 }),
    /parallelismPerKpu must be an integer from 1 to 8/,
  );
  assert.throws(
    () => synthesizeTemplate({ checkpointIntervalMillis: 0 }),
    /checkpointIntervalMillis must be an integer of at least 1/,
  );
});

function deploymentFingerprint(template: Template): string {
  const resources = template.findResources('Custom::FlinkApplicationLifecycle');
  return JSON.stringify(
    Object.values(resources)[0].Properties.DeploymentFingerprint,
  );
}

test('changes the deployment fingerprint when the pipeline changes', () => {
  const datastream = deploymentFingerprint(synthesizeTemplate());
  const sql = deploymentFingerprint(synthesizeTemplate({ pipeline: 'sql' }));

  assert.notEqual(datastream, sql);
});

test('changes the deployment fingerprint when tuning settings change', () => {
  const baseline = deploymentFingerprint(synthesizeTemplate({ pipeline: 'datagen' }));

  for (const props of [
    { parallelism: 2 },
    { parallelismPerKpu: 2 },
    { checkpointingEnabled: true },
    { checkpointIntervalMillis: 30_000 },
    { dataGenProperties: { ratePerSecond: '1000' } },
    { sinkProperties: { 'sink.batch.max-size': '50000' } },
  ]) {
    assert.notEqual(
      deploymentFingerprint(synthesizeTemplate({ pipeline: 'datagen', ...props })),
      baseline,
      JSON.stringify(props),
    );
  }
});

test('alarms on dropped records from the published sink metrics', () => {
  const template = synthesizeTemplate();

  template.hasResourceProperties('AWS::KinesisAnalyticsV2::Application', {
    ApplicationConfiguration: Match.objectLike({
      FlinkApplicationConfiguration: Match.objectLike({
        MonitoringConfiguration: Match.objectLike({
          MetricsLevel: 'APPLICATION',
        }),
      }),
    }),
  });
  template.resourceCountIs('AWS::CloudWatch::Alarm', 1);
  template.hasResourceProperties('AWS::CloudWatch::Alarm', {
    Namespace: 'AWS/KinesisAnalytics',
    MetricName: 'numRecordsDroppedPerSecond',
    Dimensions: Match.arrayWith([
      { Name: 'Application', Value: { Ref: 'FlinkApplication' } },
      { Name: 'Connector', Value: 'TimestreamInfluxDB3' },
    ]),
    Statistic: 'Sum',
    Period: 60,
    Threshold: 0,
    ComparisonOperator: 'GreaterThanThreshold',
    EvaluationPeriods: 1,
    TreatMissingData: 'notBreaching',
  });
});

test('creates the sink dashboard panels', () => {
  const template = synthesizeTemplate();

  template.resourceCountIs('AWS::CloudWatch::Dashboard', 1);
  const dashboards = template.findResources('AWS::CloudWatch::Dashboard');
  const body = JSON.stringify(
    Object.values(dashboards)[0].Properties.DashboardBody,
  );
  for (const title of [
    'Throughput',
    'Write latency (ms)',
    'Dropped records and failures',
    'Throttling and retries',
    'Buffered bytes and in-flight requests',
    'Job health',
    'Bytes written',
    'Batch size',
    'Batch outcomes',
    'Write errors by type',
    'JVM memory (%)',
    'Threads and file descriptors',
    'Old-generation GC and checkpoints (ms)',
  ]) {
    assert.ok(body.includes(`\\"title\\":\\"${title}\\"`), title);
  }
  for (const metric of [
    'numRecordsWrittenPerSecond',
    'writeLatencyP99Millis',
    'numRecordsDroppedPerSecond',
    'numThrottledRequests',
    'numBufferedBytes',
    'heapMemoryUtilization',
    'busyTimeMsPerSecond',
    'numRestarts',
    'numBytesWrittenPerSecond',
    'numBytesOut',
    'numBatchesRetried',
    'numServerErrors',
    'numConnectionErrors',
    'numRetryDelayMillis',
    'maxRetryAfterMillis',
    'metaspaceMemoryUtilization',
    'threadsCount',
    'taskmanagerFileDescriptorsOpen',
    'oldGenerationGCTime',
    'lastCheckpointDuration',
  ]) {
    assert.ok(body.includes(metric), metric);
  }
  // Summed subtask reports are divided by the job-level report count.
  assert.ok(body.includes('numRecordsWrittenPerSecond / reports'));
  assert.ok(body.includes('uptime'));
  assert.ok(!body.includes('InfluxDB cluster'));
});

test('adds an InfluxDB cluster panel when the cluster name is supplied', () => {
  const template = synthesizeTemplate({ influxDbClusterName: 'sensors-cluster' });
  const dashboards = template.findResources('AWS::CloudWatch::Dashboard');
  const body = JSON.stringify(
    Object.values(dashboards)[0].Properties.DashboardBody,
  );

  assert.ok(body.includes('InfluxDB cluster'));
  assert.ok(body.includes('sensors-cluster'));
  assert.ok(body.includes('CPUUtilization'));
  assert.ok(body.includes('MemoryUtilization'));
});
