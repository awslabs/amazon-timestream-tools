/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';

/**
 * Managed Flink metrics level used by the application.
 *
 * The metrics level decides which dimensions Managed Flink adds to published
 * metrics. The dashboard and alarm below match the APPLICATION level only, so
 * the application configuration reads this value instead of repeating it.
 */
export const FLINK_METRICS_LEVEL = 'APPLICATION';

const NAMESPACE = 'AWS/KinesisAnalytics';
const CONNECTOR_DIMENSION = { Connector: 'TimestreamInfluxDB3' };
const PERIOD = cdk.Duration.minutes(1);

export interface SinkMonitoringProps {
  readonly applicationName: string;
  /** Timestream for InfluxDB 3 cluster name; adds a cluster health panel when set. */
  readonly influxDbClusterName?: string;
}

/** CloudWatch alarm and dashboard for the InfluxDB 3 sink of one application. */
export class SinkMonitoring extends Construct {
  public readonly droppedRecordsAlarm: cloudwatch.Alarm;
  public readonly dashboard: cloudwatch.Dashboard;

  public constructor(scope: Construct, id: string, props: SinkMonitoringProps) {
    super(scope, id);

    const application = { Application: props.applicationName };
    const sinkMetric = (
      metricName: string,
      statistic: string,
      label?: string,
    ): cloudwatch.Metric =>
      new cloudwatch.Metric({
        namespace: NAMESPACE,
        metricName,
        dimensionsMap: { ...application, ...CONNECTOR_DIMENSION },
        statistic,
        period: PERIOD,
        label,
      });
    const applicationMetric = (
      metricName: string,
      statistic: string,
      label?: string,
    ): cloudwatch.Metric =>
      new cloudwatch.Metric({
        namespace: NAMESPACE,
        metricName,
        dimensionsMap: application,
        statistic,
        period: PERIOD,
        label,
      });

    // Each subtask publishes its own data point every reporting interval, so a
    // Sum over one period adds every report from every subtask. Dividing by
    // the number of reports of the job-level uptime metric in the same period
    // gives the job-wide value regardless of parallelism or report interval.
    const reportsPerPeriod = applicationMetric('uptime', 'SampleCount');
    const jobTotal = (
      metric: cloudwatch.Metric,
      label: string,
    ): cloudwatch.MathExpression =>
      new cloudwatch.MathExpression({
        expression: `${metric.metricName} / reports`,
        usingMetrics: { [metric.metricName]: metric, reports: reportsPerPeriod },
        label,
        period: PERIOD,
      });

    // The alarm uses the raw Sum: its zero threshold does not depend on how
    // many reports the Sum covers.
    const droppedPerSecond = sinkMetric('numRecordsDroppedPerSecond', 'Sum');

    this.droppedRecordsAlarm = new cloudwatch.Alarm(
      this,
      'DroppedRecordsAlarm',
      {
        alarmDescription:
          'The InfluxDB 3 sink dropped records under its failure policy. ' +
          'Check the application log for the rejected lines.',
        metric: droppedPerSecond,
        threshold: 0,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        evaluationPeriods: 1,
        // No data means the application is stopped or has not written yet.
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );

    this.dashboard = new cloudwatch.Dashboard(this, 'Dashboard', {
      dashboardName: `${props.applicationName}-influxdb3-sink`,
      defaultInterval: cdk.Duration.hours(3),
    });
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Throughput',
        left: [
          jobTotal(
            sinkMetric('numRecordsWrittenPerSecond', 'Sum'),
            'Written records/s',
          ),
          jobTotal(
            applicationMetric('numRecordsInPerSecond', 'Sum'),
            'Application input records/s',
          ),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Write latency (ms)',
        left: [
          sinkMetric('writeLatencyP50Millis', 'Average', 'p50'),
          sinkMetric('writeLatencyP95Millis', 'Maximum', 'p95'),
          sinkMetric('writeLatencyP99Millis', 'Maximum', 'p99'),
        ],
        width: 12,
      }),
    );
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Dropped records and failures',
        left: [
          jobTotal(droppedPerSecond, 'Dropped records/s'),
          jobTotal(
            sinkMetric('numRecordsSendErrorsPerSecond', 'Sum'),
            'Send errors/s',
          ),
          jobTotal(
            sinkMetric('numBatchesFailedPerSecond', 'Sum'),
            'Failed batches/s',
          ),
        ],
        leftAnnotations: [
          { value: 0, label: 'Dropped records alarm threshold' },
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Throttling and retries',
        left: [
          jobTotal(
            sinkMetric('numThrottledRequests', 'Sum'),
            'Throttled requests since job start',
          ),
          jobTotal(
            sinkMetric('numRetries', 'Sum'),
            'Retried records since job start',
          ),
          jobTotal(
            sinkMetric('numWriteTimeouts', 'Sum'),
            'Write timeouts since job start',
          ),
        ],
        right: [
          jobTotal(
            sinkMetric('numRetryDelayMillis', 'Sum'),
            'Retry delay since job start (ms)',
          ),
          sinkMetric('maxRetryAfterMillis', 'Maximum', 'Largest Retry-After (ms)'),
        ],
        width: 12,
      }),
    );
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Buffered bytes and in-flight requests',
        left: [sinkMetric('numBufferedBytes', 'Maximum', 'Buffered bytes')],
        right: [
          sinkMetric(
            'numInFlightWriteRequests',
            'Maximum',
            'In-flight write requests',
          ),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Job health',
        left: [
          applicationMetric('heapMemoryUtilization', 'Maximum', 'Heap %'),
          applicationMetric('cpuUtilization', 'Maximum', 'CPU %'),
          // The source and sink run as one chained task, so a slow sink shows
          // as busy time rather than backpressure, which stays at zero.
          new cloudwatch.MathExpression({
            expression: 'busyTime / 10',
            usingMetrics: {
              busyTime: applicationMetric('busyTimeMsPerSecond', 'Maximum'),
            },
            label: 'Busiest task busy %',
            period: PERIOD,
          }),
        ],
        right: [
          applicationMetric('numRestarts', 'Maximum', 'Restarts'),
          applicationMetric(
            'numberOfFailedCheckpoints',
            'Maximum',
            'Failed checkpoints',
          ),
        ],
        width: 12,
      }),
    );
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Bytes written',
        left: [
          jobTotal(
            sinkMetric('numBytesWrittenPerSecond', 'Sum'),
            'Accepted bytes/s (uncompressed)',
          ),
        ],
        right: [
          jobTotal(
            sinkMetric('numBytesWritten', 'Sum'),
            'Accepted bytes since job start',
          ),
          jobTotal(
            sinkMetric('numBytesOut', 'Sum'),
            'Attempted bytes since job start, including retries',
          ),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Batch size',
        left: [sinkMetric('batchSizeLines', 'Average', 'Lines per batch')],
        right: [sinkMetric('batchSizeBytes', 'Average', 'Bytes per batch')],
        width: 12,
      }),
    );
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Batch outcomes',
        left: [
          jobTotal(sinkMetric('numBatchesSuccess', 'Sum'), 'Succeeded'),
          jobTotal(sinkMetric('numBatchesRetried', 'Sum'), 'Retried'),
          jobTotal(
            sinkMetric('numBatchesCompletedWithDrops', 'Sum'),
            'Completed with drops',
          ),
          jobTotal(sinkMetric('numBatchesFailed', 'Sum'), 'Failed'),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Write errors by type',
        left: [
          jobTotal(sinkMetric('numServerErrors', 'Sum'), 'HTTP 5xx'),
          jobTotal(sinkMetric('numClientErrors', 'Sum'), 'HTTP 4xx except 429'),
          jobTotal(sinkMetric('numThrottledRequests', 'Sum'), 'HTTP 429'),
          jobTotal(sinkMetric('numConnectionErrors', 'Sum'), 'Connection errors'),
          jobTotal(sinkMetric('numWriteTimeouts', 'Sum'), 'Write timeouts'),
        ],
        width: 12,
      }),
    );
    // Long-running health: a leak shows as heap after garbage collection,
    // threads, or file descriptors creeping up, or old-generation GC growing.
    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'JVM memory (%)',
        left: [
          applicationMetric('heapMemoryUtilization', 'Maximum', 'Heap (highest)'),
          applicationMetric(
            'heapMemoryUtilization',
            'Minimum',
            'Heap after garbage collection (lowest)',
          ),
          applicationMetric('metaspaceMemoryUtilization', 'Maximum', 'Metaspace'),
        ],
        width: 8,
      }),
      new cloudwatch.GraphWidget({
        title: 'Threads and file descriptors',
        left: [
          applicationMetric('threadsCount', 'Maximum', 'Threads'),
          applicationMetric(
            'taskmanagerFileDescriptorsOpen',
            'Maximum',
            'Open file descriptors',
          ),
        ],
        width: 8,
      }),
      new cloudwatch.GraphWidget({
        title: 'Old-generation GC and checkpoints (ms)',
        left: [
          applicationMetric('oldGenerationGCTime', 'Maximum', 'Old-generation GC time'),
          applicationMetric('lastCheckpointDuration', 'Maximum', 'Checkpoint duration'),
        ],
        width: 8,
      }),
    );
    if (props.influxDbClusterName !== undefined) {
      // SEARCH covers every node of the cluster without listing instance names.
      const clusterMetric = (metricName: string, label: string) =>
        new cloudwatch.MathExpression({
          expression:
            `SEARCH('{AWS/Timestream/InfluxDB,DbClusterName,DbInstanceName} ` +
            `DbClusterName="${props.influxDbClusterName}" ` +
            `MetricName="${metricName}"', 'Maximum', 60)`,
          label,
          period: PERIOD,
        });
      this.dashboard.addWidgets(
        new cloudwatch.GraphWidget({
          title: 'InfluxDB cluster',
          left: [
            clusterMetric('CPUUtilization', 'CPU %'),
            clusterMetric('MemoryUtilization', 'Memory %'),
          ],
          width: 24,
        }),
      );
    }
  }
}
