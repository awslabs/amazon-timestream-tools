/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Note:
 * this code is available in Flink 1.15+, but as of now (12/2021) Amazon Kinesis supports Flink version 1.13.2.
 * Minimal changes were done to port the code to older Flink version. In future, the goal is to use the original source.
 * Original source link:
 * https://github.com/apache/flink/tree/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/sink
 */

package imported.vnext.org.apache.flink.connector.base.sink.sink;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

import imported.vnext.org.apache.flink.connector.base.sink.sink.writer.ElementConverter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A generic sink for destinations that provide an async client to persist data.
 *
 * <p>The design of the sink focuses on extensibility and a broad support of destinations. The core
 * of the sink is kept generic and free of any connector specific dependencies. The sink is designed
 * to participate in checkpointing to provide at-least once semantics, but it is limited to
 * destinations that provide a client that supports async requests.
 *
 * <p>Limitations:
 *
 * <ul>
 *   <li>The sink is designed for destinations that provide an async client. Destinations that
 *       cannot ingest events in an async fashion cannot be supported by the sink.
 *   <li>The sink usually persist InputTs in the order they are added to the sink, but reorderings
 *       may occur, eg, when RequestEntryTs need to be retried.
 *   <li>We are not considering support for exactly-once semantics at this point.
 * </ul>
 */
@PublicEvolving
public abstract class AsyncSinkBase<InputT, RequestEntryT extends Serializable>
        implements Sink<InputT, Void, Collection<RequestEntryT>, Void> {
    private final ElementConverter<InputT, RequestEntryT> elementConverter;
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long maxBatchSizeInBytes;
    private final long maxTimeInBufferMS;
    private final long maxRecordSizeInBytes;

    protected AsyncSinkBase(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        this.elementConverter = elementConverter;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    protected ElementConverter<InputT, RequestEntryT> getElementConverter() {
        return elementConverter;
    }

    protected int getMaxBatchSize() {
        return maxBatchSize;
    }

    protected int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    protected int getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    protected long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    protected long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }

    protected long getMaxRecordSizeInBytes() {
        return maxRecordSizeInBytes;
    }
}
