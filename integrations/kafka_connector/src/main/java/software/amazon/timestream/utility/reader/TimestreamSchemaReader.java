// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package software.amazon.timestream.utility.reader;

import software.amazon.awssdk.services.timestreamwrite.model.DataModel;

/**
 * Timestream table schema definition reader
 */
public interface TimestreamSchemaReader {

    /**
     * gets schema definition
     */
    DataModel getSchemaDefinition();
}
