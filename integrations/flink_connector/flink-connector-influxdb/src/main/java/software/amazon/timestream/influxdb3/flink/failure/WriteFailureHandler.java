/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.failure;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Classifies a failed write as retryable, droppable, or fatal.
 *
 * <p>The connector serializes callbacks for each sink writer subtask. Implementations should
 * return promptly and must not perform long-running or blocking external I/O because callbacks
 * execute on connector write threads.
 */
@FunctionalInterface
@PublicEvolving
public interface WriteFailureHandler extends Serializable {

    /**
     * Classifies one failed line protocol record.
     *
     * <p>Calls are serialized for a given sink writer, but separate subtasks may invoke their own
     * deserialized handler instances concurrently.
     *
     * @param failure failure details
     * @return action for the failed record
     */
    FailureAction onFailure(WriteFailure failure);
}
