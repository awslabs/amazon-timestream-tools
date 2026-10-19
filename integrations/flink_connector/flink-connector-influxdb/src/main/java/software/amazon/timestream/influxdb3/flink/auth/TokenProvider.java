/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.auth;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;

/**
 * Supplies authentication tokens to a sink writer.
 *
 * <p>Implementations are serialized with the Flink job. They must not serialize a resolved token
 * unless explicitly intended, as with {@link StaticTokenProvider}.
 */
@PublicEvolving
public interface TokenProvider extends Serializable, AutoCloseable {

    /**
     * Initializes transient resources inside the sink subtask.
     *
     * @throws IOException when provider initialization fails
     */
    default void open() throws IOException {}

    /**
     * Returns the current token. The caller owns and clears the returned array.
     *
     * @return a new mutable token array
     * @throws IOException when the token cannot be resolved
     */
    char[] getToken() throws IOException;

    /**
     * Forces a token refresh after an authentication failure.
     *
     * @return a new mutable token array
     * @throws IOException when the token cannot be refreshed
     */
    default char[] refreshToken() throws IOException {
        return getToken();
    }

    /** Releases transient resources. */
    @Override
    default void close() throws IOException {}
}
