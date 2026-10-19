/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.failure;

import com.influxdb.v3.client.InfluxDBApiException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultWriteFailureHandlerTest {

    @ParameterizedTest
    @ValueSource(ints = {408, 425, 429, 500, 502, 503, 504})
    void retriesTransientHttpStatuses(int status) {
        assertThat(handler().onFailure(failure(status, false)))
                .isEqualTo(FailureAction.RETRY);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 413, 422, 501, 505, 599})
    void failsPermanentHttpStatuses(int status) {
        assertThat(handler().onFailure(failure(status, false)))
                .isEqualTo(FailureAction.FAIL);
    }

    @Test
    void dropsLineLevelPartialWriteFailures() {
        assertThat(handler().onFailure(failure(400, true))).isEqualTo(FailureAction.DROP);
    }

    @Test
    void appliesConfiguredTerminalFailurePolicies() {
        DefaultWriteFailureHandler strictRejected =
                new DefaultWriteFailureHandler(true, true);
        DefaultWriteFailureHandler droppingErrors =
                new DefaultWriteFailureHandler(false, false);

        assertThat(strictRejected.onFailure(failure(400, true)))
                .isEqualTo(FailureAction.FAIL);
        assertThat(droppingErrors.onFailure(failure(400, false)))
                .isEqualTo(FailureAction.DROP);
        assertThat(droppingErrors.onFailure(failure(422, false)))
                .isEqualTo(FailureAction.DROP);
        assertThat(droppingErrors.onFailure(failure(404, false)))
                .isEqualTo(FailureAction.DROP);
        assertThat(droppingErrors.onFailure(failure(new IllegalStateException("client failed"))))
                .isEqualTo(FailureAction.DROP);
    }

    @Test
    void appliesConfiguredTerminalPolicyWhenCauseIsAbsent() {
        WriteFailure failure =
                new WriteFailure(
                        "cpu value=1i",
                        null,
                        0,
                        null,
                        false,
                        "client failed without a cause");

        assertThat(handler().onFailure(failure)).isEqualTo(FailureAction.FAIL);
        assertThat(new DefaultWriteFailureHandler(false, false).onFailure(failure))
                .isEqualTo(FailureAction.DROP);
    }

    @Test
    void retriesTransientFailuresWrappedByClient() {
        assertAction(
                new InfluxDBApiException(new SocketException("connection reset")),
                FailureAction.RETRY);
    }

    @Test
    void doesNotRetryUnclassifiedIoFailures() {
        assertAction(
                new InfluxDBApiException(new IOException("local encoding failed")),
                FailureAction.FAIL);
    }

    @Test
    void doesNotRetryClientFailuresWithWrappedNonIoCauses() {
        assertAction(
                new InfluxDBApiException(new IllegalStateException("client failed")),
                FailureAction.FAIL);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("retryableTransportFailures")
    void retriesSpecificTransportFailures(Throwable cause) {
        assertAction(cause, FailureAction.RETRY);
    }

    @Test
    void retriesRecognizedFailureAnywhereInTheCauseChain() {
        ConnectException connectionFailure = new ConnectException("connection refused");
        connectionFailure.initCause(new IllegalStateException("resolver detail"));
        assertAction(new InfluxDBApiException(connectionFailure), FailureAction.RETRY);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("permanentTransportFailures")
    void doesNotRetryPermanentTransportFailures(Throwable cause) {
        assertAction(new InfluxDBApiException(cause), FailureAction.FAIL);
    }

    @Test
    void permanentTransportFailureOverridesNestedTransientCause() {
        SSLHandshakeException handshakeFailure =
                new SSLHandshakeException("TLS handshake failed");
        handshakeFailure.initCause(new SocketException("connection reset"));
        assertAction(new InfluxDBApiException(handshakeFailure), FailureAction.FAIL);
    }

    @Test
    void doesNotRetryAnUnwrappedClientException() {
        assertAction(new InfluxDBApiException("client failed"), FailureAction.FAIL);
    }

    @Test
    void handlesSelfReferencingCauses() {
        RuntimeException selfReferencing =
                new RuntimeException("self-referencing") {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public synchronized Throwable getCause() {
                        return this;
                    }
                };
        assertAction(selfReferencing, FailureAction.FAIL);
    }

    @Test
    void failsUnknownRuntimeErrors() {
        assertAction(new IllegalStateException("bad client state"), FailureAction.FAIL);
    }

    @Test
    void exposesFailureDetails() {
        IllegalStateException cause = new IllegalStateException("write failed");
        WriteFailure failure =
                new WriteFailure(
                        "cpu,host=a value=1i",
                        cause,
                        3,
                        503,
                        false,
                        "service unavailable");

        assertThat(failure.getLineProtocol()).isEqualTo("cpu,host=a value=1i");
        assertThat(failure.getCause()).isSameAs(cause);
        assertThat(failure.getAttempt()).isEqualTo(3);
        assertThat(failure.getStatusCode()).hasValue(503);
        assertThat(failure.isPartialWrite()).isFalse();
        assertThat(failure.getServerMessage()).isEqualTo("service unavailable");
    }

    private static DefaultWriteFailureHandler handler() {
        return DefaultWriteFailureHandler.INSTANCE;
    }

    private static void assertAction(Throwable cause, FailureAction expectedAction) {
        assertThat(handler().onFailure(failure(cause))).isEqualTo(expectedAction);
    }

    private static Stream<Throwable> retryableTransportFailures() {
        return Stream.of(
                new ConnectException("connection refused"),
                new SocketException("connection reset"),
                new SocketTimeoutException("socket timed out"),
                new HttpTimeoutException("HTTP timed out"),
                new UnknownHostException("DNS unavailable"),
                new EOFException("connection closed early"));
    }

    private static Stream<Throwable> permanentTransportFailures() {
        return Stream.of(
                new SSLHandshakeException("certificate expired"),
                new SSLPeerUnverifiedException("hostname mismatch"),
                new ProtocolException("unsupported HTTP protocol"));
    }

    private static WriteFailure failure(int status, boolean partial) {
        return new WriteFailure(
                "cpu value=1i",
                new RuntimeException("write failed"),
                0,
                status,
                partial,
                "write failed");
    }

    private static WriteFailure failure(Throwable cause) {
        return new WriteFailure(
                "cpu value=1i",
                cause,
                0,
                null,
                false,
                cause.getMessage());
    }
}
