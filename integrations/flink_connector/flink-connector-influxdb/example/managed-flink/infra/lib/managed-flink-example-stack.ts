/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as kinesisanalytics from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import { Construct } from 'constructs';
import { FlinkApplicationLifecycle } from './flink-application-lifecycle';
import { FLINK_METRICS_LEVEL, SinkMonitoring } from './sink-monitoring';

const INFLUX_DB_ENDPOINT_PATTERN =
  '^https://(?:\\[[0-9A-Fa-f:]+\\]|[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*)' +
  '(?::[0-9]{1,5})?/?$';

export type ApplicationPipeline = 'datastream' | 'sql' | 'datagen';

const APPLICATION_PIPELINES: readonly ApplicationPipeline[] = [
  'datastream',
  'sql',
  'datagen',
];

// Managed Flink accepts 1 to 8 parallel tasks per KPU.
const MAX_PARALLELISM_PER_KPU = 8;
const DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 60_000;

export interface ManagedFlinkExampleStackProps extends cdk.StackProps {
  readonly applicationJarPath: string;
  readonly lifecycleCodePath: string;
  readonly applicationName?: string;
  readonly databaseName?: string;
  readonly flinkRuntime?: string;
  readonly pipeline?: string;
  readonly influxDbEndpoint?: string;
  readonly influxDbSecretArn?: string;
  readonly influxDbTokenField?: string;
  readonly influxDbSecretKmsKeyArn?: string;
  readonly influxDbClusterName?: string;
  /** Application parallelism; defaults to 1. */
  readonly parallelism?: number;
  /** Parallel tasks per KPU, from 1 to 8; defaults to 1. */
  readonly parallelismPerKpu?: number;
  /** Enables checkpointing; defaults to true. */
  readonly checkpointingEnabled?: boolean;
  /** Checkpoint interval when checkpointing is enabled; defaults to 60,000 ms. */
  readonly checkpointIntervalMillis?: number;
  /** `DataGen` runtime properties for the datagen pipeline. */
  readonly dataGenProperties?: Record<string, string>;
  /** `Sink` runtime properties that tune the connector. */
  readonly sinkProperties?: Record<string, string>;
  readonly flinkSubnetIds?: string[];
  readonly flinkSecurityGroupIds?: string[];
}

export class ManagedFlinkExampleStack extends cdk.Stack {
  public constructor(
    scope: Construct,
    id: string,
    props: ManagedFlinkExampleStackProps,
  ) {
    super(scope, id, props);

    const applicationName = props.applicationName ?? 'influxdb3-flink-example';
    const databaseName = props.databaseName ?? 'sensors';
    const flinkRuntime = props.flinkRuntime ?? 'FLINK-2_3';
    const influxDbTokenField = props.influxDbTokenField ?? 'token';
    // Matches the application, which parses the pipeline case-insensitively.
    const pipeline = (props.pipeline ?? 'datastream').toLowerCase();
    if (!APPLICATION_PIPELINES.includes(pipeline as ApplicationPipeline)) {
      throw new Error(
        `pipeline must be one of ${APPLICATION_PIPELINES.join(', ')}`,
      );
    }
    const parallelism = props.parallelism ?? 1;
    const parallelismPerKpu = props.parallelismPerKpu ?? 1;
    const checkpointingEnabled = props.checkpointingEnabled ?? true;
    const checkpointIntervalMillis =
      props.checkpointIntervalMillis ?? DEFAULT_CHECKPOINT_INTERVAL_MILLIS;
    requireInteger(parallelism, 'parallelism', 1);
    requireInteger(parallelismPerKpu, 'parallelismPerKpu', 1, MAX_PARALLELISM_PER_KPU);
    requireInteger(checkpointIntervalMillis, 'checkpointIntervalMillis', 1);
    if (props.dataGenProperties !== undefined && pipeline !== 'datagen') {
      throw new Error('dataGenProperties requires the datagen pipeline');
    }
    const readsKinesis = pipeline !== 'datagen';
    if (
      !cdk.Token.isUnresolved(databaseName) &&
      !/^[A-Za-z0-9_-]+$/.test(databaseName)
    ) {
      throw new Error(
        'databaseName must contain only letters, numbers, hyphens, and underscores',
      );
    }
    if (
      !cdk.Token.isUnresolved(influxDbTokenField) &&
      influxDbTokenField.trim().length === 0
    ) {
      throw new Error('influxDbTokenField must not be blank');
    }
    if (
      props.influxDbEndpoint !== undefined &&
      !cdk.Token.isUnresolved(props.influxDbEndpoint) &&
      !new RegExp(INFLUX_DB_ENDPOINT_PATTERN).test(props.influxDbEndpoint)
    ) {
      throw new Error(
        'influxDbEndpoint must be an HTTPS base URL with a valid host, ' +
          'an optional port, and no API path',
      );
    }

    const endpoint =
      props.influxDbEndpoint ??
      new cdk.CfnParameter(this, 'InfluxDbEndpoint', {
        type: 'String',
        description:
          'Existing InfluxDB base endpoint including scheme and port, without an API path',
        allowedPattern: INFLUX_DB_ENDPOINT_PATTERN,
        constraintDescription:
          'must be an HTTPS base URL with a valid host, an optional port, and no API path',
      }).valueAsString;
    const secretArnParameter =
      props.influxDbSecretArn === undefined
        ? new cdk.CfnParameter(this, 'InfluxDbSecretArn', {
            type: 'String',
            description:
              'Secrets Manager ARN containing the InfluxDB authentication token',
            allowedPattern:
              'arn:[^:]+:secretsmanager:[^:]+:[0-9]{12}:secret:.+',
          })
        : undefined;
    const secretArn =
      props.influxDbSecretArn ?? secretArnParameter!.valueAsString;
    const secretRegion = cdk.Fn.select(3, cdk.Fn.split(':', secretArn));

    const subnetIds = props.flinkSubnetIds;
    const securityGroupIds = props.flinkSecurityGroupIds;
    const hasSubnetIds = subnetIds !== undefined && subnetIds.length > 0;
    const hasSecurityGroupIds =
      securityGroupIds !== undefined && securityGroupIds.length > 0;
    if (hasSubnetIds !== hasSecurityGroupIds) {
      throw new Error(
        'flinkSubnetIds and flinkSecurityGroupIds must be provided together',
      );
    }

    // The datagen pipeline generates its input, so it has no stream.
    let inputStream: kinesis.Stream | undefined;
    if (readsKinesis) {
      inputStream = new kinesis.Stream(this, 'InputStream', {
        streamMode: kinesis.StreamMode.ON_DEMAND,
        encryption: kinesis.StreamEncryption.MANAGED,
      });
      inputStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }

    const applicationArtifact = new s3assets.Asset(this, 'ApplicationArtifact', {
      path: props.applicationJarPath,
    });

    const executionRole = new iam.Role(this, 'FlinkExecutionRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      description: 'Execution role for the InfluxDB 3 Managed Flink example application',
    });
    executionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['s3:GetObject', 's3:GetObjectVersion'],
        resources: [
          applicationArtifact.bucket.arnForObjects(applicationArtifact.s3ObjectKey),
        ],
      }),
    );
    inputStream?.grantRead(executionRole);
    executionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        // The second form matches the random suffix when secretArn omits it.
        resources: [secretArn, `${secretArn}-??????`],
      }),
    );
    if (props.influxDbSecretKmsKeyArn !== undefined) {
      executionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ['kms:Decrypt'],
          resources: [props.influxDbSecretKmsKeyArn],
        }),
      );
    }
    if (hasSubnetIds && hasSecurityGroupIds) {
      executionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            'ec2:CreateNetworkInterface',
            'ec2:CreateNetworkInterfacePermission',
            'ec2:DeleteNetworkInterface',
            'ec2:DescribeDhcpOptions',
            'ec2:DescribeNetworkInterfaces',
            'ec2:DescribeSecurityGroups',
            'ec2:DescribeSubnets',
            'ec2:DescribeVpcs',
          ],
          resources: ['*'],
        }),
      );
    }

    const applicationLogGroup = new logs.LogGroup(this, 'ApplicationLogGroup', {
      logGroupName: `/aws/kinesis-analytics/${applicationName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const applicationLogStream = new logs.LogStream(this, 'ApplicationLogStream', {
      logGroup: applicationLogGroup,
      logStreamName: 'application',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const applicationLogStreamArn = this.formatArn({
      service: 'logs',
      resource: 'log-group',
      resourceName:
        `${applicationLogGroup.logGroupName}:log-stream:` +
        applicationLogStream.logStreamName,
      arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME,
    });
    executionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['logs:DescribeLogGroups'],
        resources: ['*'],
      }),
    );
    executionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['logs:DescribeLogStreams'],
        resources: [applicationLogGroup.logGroupArn],
      }),
    );
    executionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['logs:PutLogEvents'],
        resources: [applicationLogStreamArn],
      }),
    );

    const application = new kinesisanalytics.CfnApplication(
      this,
      'FlinkApplication',
      {
        applicationName,
        applicationDescription:
          readsKinesis
            ? 'Kinesis to an existing InfluxDB 3 endpoint example application'
            : 'Generated load to an existing InfluxDB 3 endpoint',
        applicationMode: 'STREAMING',
        runtimeEnvironment: flinkRuntime,
        serviceExecutionRole: executionRole.roleArn,
        runConfiguration: {
          applicationRestoreConfiguration: {
            applicationRestoreType: 'SKIP_RESTORE_FROM_SNAPSHOT',
          },
          flinkRunConfiguration: {
            allowNonRestoredState: true,
          },
        },
        applicationConfiguration: {
          applicationCodeConfiguration: {
            codeContentType: 'ZIPFILE',
            codeContent: {
              s3ContentLocation: {
                bucketArn: applicationArtifact.bucket.bucketArn,
                fileKey: applicationArtifact.s3ObjectKey,
              },
            },
          },
          applicationSnapshotConfiguration: {
            snapshotsEnabled: false,
          },
          environmentProperties: {
            propertyGroups: [
              {
                propertyGroupId: 'Application',
                propertyMap: {
                  pipeline,
                },
              },
              ...(inputStream !== undefined
                ? [
                    {
                      propertyGroupId: 'KinesisSource',
                      propertyMap: {
                        streamArn: inputStream.streamArn,
                        region: this.region,
                        initialPosition: 'LATEST',
                      },
                    },
                  ]
                : []),
              {
                propertyGroupId: 'InfluxDB3',
                propertyMap: {
                  endpoint,
                  database: databaseName,
                  secretId: secretArn,
                  secretJsonField: influxDbTokenField,
                  region: secretRegion,
                },
              },
              ...optionalPropertyGroup('DataGen', props.dataGenProperties),
              ...optionalPropertyGroup('Sink', props.sinkProperties),
            ],
          },
          flinkApplicationConfiguration: {
            checkpointConfiguration: checkpointingEnabled
              ? {
                  configurationType: 'CUSTOM',
                  checkpointingEnabled: true,
                  checkpointInterval: checkpointIntervalMillis,
                }
              : {
                  configurationType: 'CUSTOM',
                  checkpointingEnabled: false,
                },
            monitoringConfiguration: {
              configurationType: 'CUSTOM',
              logLevel: 'INFO',
              metricsLevel: FLINK_METRICS_LEVEL,
            },
            parallelismConfiguration: {
              configurationType: 'CUSTOM',
              autoScalingEnabled: false,
              parallelism,
              parallelismPerKpu,
            },
          },
          ...(hasSubnetIds && hasSecurityGroupIds
            ? {
                vpcConfigurations: [
                  {
                    securityGroupIds,
                    subnetIds,
                  },
                ],
              }
            : {}),
        },
      },
    );
    application.node.addDependency(executionRole);

    const loggingOption =
      new kinesisanalytics.CfnApplicationCloudWatchLoggingOption(
        this,
        'FlinkApplicationLogging',
        {
          applicationName: application.ref,
          cloudWatchLoggingOption: {
            logStreamArn: applicationLogStreamArn,
          },
        },
      );

    const fingerprintParts = [
      applicationArtifact.assetHash,
      flinkRuntime,
      databaseName,
      endpoint,
      secretArn,
      ...(inputStream !== undefined ? [inputStream.streamArn] : []),
      pipeline,
      String(parallelism),
      String(parallelismPerKpu),
      String(checkpointingEnabled),
      String(checkpointIntervalMillis),
      JSON.stringify(props.dataGenProperties ?? {}),
      JSON.stringify(props.sinkProperties ?? {}),
      ...(subnetIds ?? []),
      ...(securityGroupIds ?? []),
    ];
    const lifecycle = new FlinkApplicationLifecycle(
      this,
      'FlinkApplicationLifecycle',
      {
        applicationArn: kinesisanalytics.CfnApplication.arnForApplication(
          application,
        ),
        applicationName: application.ref,
        codePath: props.lifecycleCodePath,
        deploymentFingerprint: cdk.Fn.join(':', fingerprintParts),
      },
    );
    lifecycle.resource.node.addDependency(loggingOption);

    const monitoring = new SinkMonitoring(this, 'SinkMonitoring', {
      applicationName: application.ref,
      influxDbClusterName: props.influxDbClusterName,
    });

    new cdk.CfnOutput(this, 'ApplicationName', {
      value: application.ref,
    });
    if (inputStream !== undefined) {
      new cdk.CfnOutput(this, 'KinesisStreamName', {
        value: inputStream.streamName,
      });
    }
    new cdk.CfnOutput(this, 'InfluxDBEndpoint', {
      value: endpoint,
    });
    new cdk.CfnOutput(this, 'InfluxDBAuthSecretArn', {
      value: secretArn,
    });
    new cdk.CfnOutput(this, 'InfluxDBDatabase', {
      value: databaseName,
    });
    new cdk.CfnOutput(this, 'SinkDashboardName', {
      value: monitoring.dashboard.dashboardName,
    });
    new cdk.CfnOutput(this, 'DroppedRecordsAlarmName', {
      value: monitoring.droppedRecordsAlarm.alarmName,
    });
  }
}

function requireInteger(
  value: number,
  name: string,
  minimum: number,
  maximum: number = Number.MAX_SAFE_INTEGER,
): void {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      maximum === Number.MAX_SAFE_INTEGER
        ? `${name} must be an integer of at least ${minimum}`
        : `${name} must be an integer from ${minimum} to ${maximum}`,
    );
  }
}

function optionalPropertyGroup(
  propertyGroupId: string,
  propertyMap: Record<string, string> | undefined,
): kinesisanalytics.CfnApplication.PropertyGroupProperty[] {
  if (propertyMap === undefined || Object.keys(propertyMap).length === 0) {
    return [];
  }
  return [{ propertyGroupId, propertyMap }];
}
