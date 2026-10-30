/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.failure;

import org.apache.flink.annotation.PublicEvolving;

/** Action to take for a failed InfluxDB write. */
@PublicEvolving
public enum FailureAction {
    /** Retry the failed record subject to the configured retry limit. */
    RETRY,

    /** Drop the failed record and allow processing to continue. */
    DROP,

    /** Fail the sink and the containing Flink job. */
    FAIL
}
