/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.auth;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Objects;

/**
 * A serializable, fixed token provider intended for local development and tests.
 *
 * <p>Production jobs should use a provider backed by a secret store.
 *
 * <p>The retained token copy is cleared when {@link #close()} is called.
 */
@PublicEvolving
public final class StaticTokenProvider implements TokenProvider {
    private static final long serialVersionUID = 1L;

    private final char[] token;

    /**
     * Creates a provider from a fixed token string.
     *
     * @param token non-blank InfluxDB API token
     */
    public StaticTokenProvider(String token) {
        this(Objects.requireNonNull(token, "token").toCharArray());
    }

    /**
     * Creates a provider by copying a fixed token array.
     *
     * @param token non-blank InfluxDB API token
     */
    public StaticTokenProvider(char[] token) {
        Objects.requireNonNull(token, "token");
        if (token.length == 0 || isBlank(token)) {
            throw new IllegalArgumentException("token must not be blank");
        }
        this.token = Arrays.copyOf(token, token.length);
    }

    @Override
    public synchronized char[] getToken() {
        return Arrays.copyOf(token, token.length);
    }

    @Override
    public synchronized void close() {
        Arrays.fill(token, '\0');
    }

    @Override
    public String toString() {
        return "StaticTokenProvider{token=[REDACTED]}";
    }

    private static boolean isBlank(char[] value) {
        for (char character : value) {
            if (!Character.isWhitespace(character)) {
                return false;
            }
        }
        return true;
    }
}
