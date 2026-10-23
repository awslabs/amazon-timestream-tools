/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Owns token lifecycle and reference-counted InfluxDB client generations. */
final class InfluxDBClientManager {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBClientManager.class);

    private final InfluxDB3SinkConfig config;
    private final InfluxDBClientFactory clientFactory;
    private final TokenProvider tokenProvider;
    private final InfluxDBSinkMetrics metrics;
    private final Consumer<Exception> asynchronousCloseFailureHandler;
    private final AtomicReference<ClientGeneration> activeClient = new AtomicReference<>();
    private final Set<ClientGeneration> clients = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    InfluxDBClientManager(
            InfluxDB3SinkConfig config,
            InfluxDBClientFactory clientFactory,
            InfluxDBSinkMetrics metrics,
            Consumer<Exception> asynchronousCloseFailureHandler) {
        this.config = Objects.requireNonNull(config, "config");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
        this.tokenProvider = config.getTokenProvider();
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.asynchronousCloseFailureHandler =
                Objects.requireNonNull(
                        asynchronousCloseFailureHandler,
                        "asynchronousCloseFailureHandler");
    }

    void initialize() throws IOException {
        try {
            tokenProvider.open();
            char[] token = null;
            try {
                token = requireToken(tokenProvider.getToken());
                activateGeneration(0, token);
            } finally {
                clearToken(token);
            }
        } catch (Exception error) {
            try {
                tokenProvider.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException("Unable to initialize the InfluxDB client", error);
        }
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean beginClose() {
        return closed.compareAndSet(false, true);
    }

    ClientLease acquire() {
        while (!closed.get()) {
            ClientGeneration generation = activeClient.get();
            if (generation == null) {
                throw new IllegalStateException("The InfluxDB client is not initialized");
            }
            ClientLease lease = generation.tryAcquire();
            if (lease != null) {
                return lease;
            }
        }
        throw new IllegalStateException("The sink writer is closed");
    }

    synchronized boolean refresh(ClientGeneration failedClient) throws IOException {
        if (closed.get()) {
            throw new IOException("Cannot refresh the InfluxDB client after the writer closed");
        }
        if (activeClient.get() != failedClient) {
            return false;
        }

        char[] token = null;
        try {
            token = requireToken(tokenProvider.refreshToken());
            activateGeneration(failedClient.generation + 1, token);
            Exception closeError = failedClient.retire();
            if (closeError != null) {
                reportClientCloseFailure(closeError);
            }
            return true;
        } finally {
            clearToken(token);
        }
    }

    Exception closeResources(Exception failure) {
        for (ClientGeneration client : List.copyOf(clients)) {
            Exception closeError = client.forceClose();
            if (closeError != null) {
                failure = appendFailure(failure, closeError);
            }
        }
        try {
            tokenProvider.close();
        } catch (Exception error) {
            failure = appendFailure(failure, error);
        }
        return failure;
    }

    int clientCount() {
        return clients.size();
    }

    private InfluxDBWriteClient createClient(char[] token) {
        return Objects.requireNonNull(
                clientFactory.create(config, token),
                "InfluxDBClientFactory returned null");
    }

    private void activateGeneration(int generationNumber, char[] token) {
        ClientGeneration generation =
                new ClientGeneration(generationNumber, createClient(token));
        clients.add(generation);
        metrics.clientGenerationOpened();
        activeClient.set(generation);
    }

    private void reportClientCloseFailure(Exception error) {
        if (closed.get()) {
            LOG.warn(
                    "Unable to close a retired InfluxDB client during shutdown; "
                            + "final cleanup will retry",
                    error);
            return;
        }
        // Continuing can leak one transport generation per token rotation, so this remains a
        // connector-owned fatal lifecycle failure while the writer is running.
        asynchronousCloseFailureHandler.accept(
                new InfluxDB3SinkException(
                        "Unable to close a retired InfluxDB client", error));
    }

    private static char[] requireToken(char[] token) throws IOException {
        if (token == null || token.length == 0) {
            throw new IOException("TokenProvider returned an empty InfluxDB token");
        }
        return token;
    }

    private static void clearToken(char[] token) {
        if (token != null) {
            Arrays.fill(token, '\0');
        }
    }

    private static Exception appendFailure(Exception existing, Exception additional) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    final class ClientLease {
        private final ClientGeneration generation;

        private ClientLease(ClientGeneration generation) {
            this.generation = generation;
        }

        InfluxDBWriteClient client() {
            return generation.client;
        }

        ClientGeneration generation() {
            return generation;
        }

        void release() {
            Exception closeError = generation.release();
            if (closeError != null) {
                reportClientCloseFailure(closeError);
            }
        }
    }

    final class ClientGeneration {
        private final int generation;
        private final InfluxDBWriteClient client;
        private int references;
        private boolean retired;
        private boolean clientClosed;

        private ClientGeneration(int generation, InfluxDBWriteClient client) {
            this.generation = generation;
            this.client = client;
        }

        private synchronized ClientLease tryAcquire() {
            if (retired || clientClosed) {
                return null;
            }
            references++;
            return new ClientLease(this);
        }

        private synchronized Exception release() {
            if (references <= 0) {
                return new IllegalStateException(
                        "InfluxDB client generation reference count underflow");
            }
            references--;
            return closeIfEligible();
        }

        private synchronized Exception retire() {
            retired = true;
            return closeIfEligible();
        }

        private synchronized Exception forceClose() {
            retired = true;
            return closeClient();
        }

        private Exception closeIfEligible() {
            return retired && references == 0 ? closeClient() : null;
        }

        private Exception closeClient() {
            if (clientClosed) {
                return null;
            }
            try {
                client.close();
                clientClosed = true;
                metrics.clientGenerationClosed();
                clients.remove(this);
                return null;
            } catch (Exception error) {
                return error;
            }
        }
    }
}
