/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';

export interface FlinkApplicationLifecycleProps {
  readonly applicationArn: string;
  readonly applicationName: string;
  readonly codePath: string;
  readonly deploymentFingerprint: string;
}

/**
 * Starts a Managed Flink application after CloudFormation creates it and
 * attempts to stop it before deletion. Stop failures are logged without
 * blocking teardown because application deletion also halts execution.
 */
export class FlinkApplicationLifecycle extends Construct {
  public readonly resource: cdk.CustomResource;

  public constructor(
    scope: Construct,
    id: string,
    props: FlinkApplicationLifecycleProps,
  ) {
    super(scope, id);

    const onEventLogGroup = new logs.LogGroup(this, 'OnEventLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const onEvent = new lambda.Function(this, 'OnEvent', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.on_event',
      code: lambda.Code.fromAsset(props.codePath),
      timeout: cdk.Duration.minutes(2),
      logGroup: onEventLogGroup,
      description: 'Requests Managed Flink application start and stop transitions',
    });

    const isCompleteLogGroup = new logs.LogGroup(this, 'IsCompleteLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const isComplete = new lambda.Function(this, 'IsComplete', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.is_complete',
      code: lambda.Code.fromAsset(props.codePath),
      timeout: cdk.Duration.minutes(2),
      logGroup: isCompleteLogGroup,
      description: 'Waits for Managed Flink application lifecycle transitions',
    });

    const applicationLifecyclePolicy = new iam.PolicyStatement({
      actions: [
        'kinesisanalytics:DescribeApplication',
        'kinesisanalytics:DescribeApplicationOperation',
        'kinesisanalytics:ListApplicationOperations',
        'kinesisanalytics:StartApplication',
        'kinesisanalytics:StopApplication',
      ],
      resources: [props.applicationArn],
    });
    onEvent.addToRolePolicy(applicationLifecyclePolicy);
    isComplete.addToRolePolicy(applicationLifecyclePolicy);

    const providerLogGroup = new logs.LogGroup(this, 'ProviderLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const provider = new cr.Provider(this, 'Provider', {
      onEventHandler: onEvent,
      isCompleteHandler: isComplete,
      queryInterval: cdk.Duration.seconds(20),
      totalTimeout: cdk.Duration.minutes(45),
      logGroup: providerLogGroup,
    });

    this.resource = new cdk.CustomResource(this, 'Resource', {
      serviceToken: provider.serviceToken,
      serviceTimeout: cdk.Duration.minutes(55),
      resourceType: 'Custom::FlinkApplicationLifecycle',
      properties: {
        ApplicationName: props.applicationName,
        DeploymentFingerprint: props.deploymentFingerprint,
      },
    });
  }
}
