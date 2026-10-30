/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecretsManagerTokenProviderTest {

    @Test
    void loadsAndCachesPlainTextSecret() throws Exception {
        StubSecretsManagerClient client =
                new StubSecretsManagerClient(
                        responseWithString("  token-one  "), responseWithString("token-two"));
        SecretsManagerTokenProvider provider = provider(client.client(), null);

        char[] first = provider.getToken();
        first[0] = 'X';
        char[] cached = provider.getToken();
        char[] refreshed = provider.refreshToken();

        assertThat(cached).containsExactly("token-one".toCharArray());
        assertThat(refreshed).containsExactly("token-two".toCharArray());
        assertThat(client.requests)
                .hasSize(2)
                .allSatisfy(
                        value -> {
                            assertThat(value.secretId()).isEqualTo("my-token");
                            assertThat(value.versionStage()).isEqualTo("AWSCURRENT");
                        });

        Arrays.fill(first, '\0');
        Arrays.fill(cached, '\0');
        Arrays.fill(refreshed, '\0');
        provider.close();
        assertThat(client.closed).isTrue();
    }

    @Test
    void readsConfiguredJsonField() throws Exception {
        StubSecretsManagerClient client =
                new StubSecretsManagerClient(
                        responseWithString("{\"token\":\"json-token\",\"other\":1}"));
        SecretsManagerTokenProvider provider = provider(client.client(), "token");

        char[] token = provider.getToken();

        assertThat(token).containsExactly("json-token".toCharArray());
        Arrays.fill(token, '\0');
        provider.close();
    }

    @Test
    void readsUtf8BinarySecret() throws Exception {
        StubSecretsManagerClient client =
                new StubSecretsManagerClient(
                        GetSecretValueResponse.builder()
                                .secretBinary(SdkBytes.fromUtf8String("binary-token"))
                                .build());
        SecretsManagerTokenProvider provider = provider(client.client(), null);

        char[] token = provider.getToken();

        assertThat(token).containsExactly("binary-token".toCharArray());
        Arrays.fill(token, '\0');
        provider.close();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "\t"})
    void rejectsBlankSecretIds(String secretId) {
        assertThatThrownBy(() -> SecretsManagerTokenProvider.fromSecretId(secretId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secretId");
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void opensSdkClientsWithExplicitAndDefaultRegions() {
        String previousRegion = System.getProperty("aws.region");
        SecretsManagerTokenProvider explicitRegion =
                SecretsManagerTokenProvider.builder()
                        .setSecretId("my-token")
                        .setRegion("us-east-1")
                        .build();
        SecretsManagerTokenProvider defaultRegion =
                SecretsManagerTokenProvider.fromSecretId("my-token");
        try {
            System.setProperty("aws.region", "us-west-2");

            explicitRegion.open();
            defaultRegion.open();

            assertBoundedApiTimeouts(explicitRegion);
            assertBoundedApiTimeouts(defaultRegion);
        } finally {
            explicitRegion.close();
            defaultRegion.close();
            if (previousRegion == null) {
                System.clearProperty("aws.region");
            } else {
                System.setProperty("aws.region", previousRegion);
            }
        }
    }

    @Test
    void supportsConvenienceFactoryAndRedactedDescription() {
        SecretsManagerTokenProvider byId =
                SecretsManagerTokenProvider.fromSecretId("my-token");
        SecretsManagerTokenProvider byArn =
                SecretsManagerTokenProvider.fromSecretId(
                        "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-token");

        assertThat(byId.toString())
                .contains("secretId='my-token'", "versionStage='AWSCURRENT'")
                .doesNotContain("resolved-token");
        assertThat(byArn.toString()).contains("secretId='arn:aws:secretsmanager:");
    }

    @Test
    void refreshesTokenAndOmitsBlankOptionalSettings() throws Exception {
        StubSecretsManagerClient client =
                new StubSecretsManagerClient(
                        responseWithString("token-one"), responseWithString("token-two"));
        SecretsManagerTokenProvider provider =
                SecretsManagerTokenProvider.builder()
                        .setSecretId("my-token")
                        .setJsonField(" ")
                        .setRegion(" ")
                        .setVersionStage(" ")
                        .build();
        provider.setClientForTesting(client.client());

        provider.open();
        char[] first = provider.getToken();
        char[] firstCachedToken = cachedToken(provider);
        char[] second = provider.refreshToken();

        assertThat(first).containsExactly("token-one".toCharArray());
        assertThat(second).containsExactly("token-two".toCharArray());
        assertThat(firstCachedToken).containsOnly('\0');
        assertThat(client.requests)
                .hasSize(2)
                .allSatisfy(request -> assertThat(request.versionStage()).isNull());

        Arrays.fill(first, '\0');
        Arrays.fill(second, '\0');
        provider.close();
        provider.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("tokenFailureCases")
    void reportsRetrievalAndInvalidPayloadFailures(TokenFailureCase testCase) {
        SecretsManagerTokenProvider provider =
                provider(testCase.client().client(), testCase.jsonField());
        try {
            assertThatThrownBy(provider::getToken)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(testCase.expectedMessage());
        } finally {
            provider.close();
        }
    }

    private static SecretsManagerTokenProvider provider(
            SecretsManagerClient client, String jsonField) {
        SecretsManagerTokenProvider provider =
                SecretsManagerTokenProvider.builder()
                        .setSecretId("my-token")
                        .setJsonField(jsonField)
                        .setRegion("us-east-1")
                        .build();
        provider.setClientForTesting(client);
        return provider;
    }

    private static char[] cachedToken(SecretsManagerTokenProvider provider) {
        return fieldValue(provider, "cachedToken", char[].class);
    }

    private static void assertBoundedApiTimeouts(SecretsManagerTokenProvider provider) {
        ClientOverrideConfiguration configuration =
                fieldValue(provider, "client", SecretsManagerClient.class)
                        .serviceClientConfiguration()
                        .overrideConfiguration();
        assertThat(configuration.apiCallTimeout()).contains(Duration.ofSeconds(30));
        assertThat(configuration.apiCallAttemptTimeout()).contains(Duration.ofSeconds(10));
    }

    private static <T> T fieldValue(Object instance, String fieldName, Class<T> fieldType) {
        try {
            Field field = instance.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return fieldType.cast(field.get(instance));
        } catch (ReflectiveOperationException error) {
            throw new AssertionError("Unable to access " + fieldName, error);
        }
    }

    private static Stream<TokenFailureCase> tokenFailureCases() {
        return Stream.of(
                new TokenFailureCase(
                        "retrieval failure",
                        new StubSecretsManagerClient(),
                        null,
                        "Unable to retrieve the InfluxDB token"),
                new TokenFailureCase(
                        "missing secret payload",
                        new StubSecretsManagerClient(
                                GetSecretValueResponse.builder().build()),
                        null,
                        "no SecretString or SecretBinary"),
                new TokenFailureCase(
                        "blank secret payload",
                        new StubSecretsManagerClient(responseWithString(" \t ")),
                        null,
                        "resolved InfluxDB token is empty"),
                new TokenFailureCase(
                        "missing JSON field",
                        new StubSecretsManagerClient(
                                responseWithString("{\"other\":\"value\"}")),
                        "token",
                        "textual field 'token'"),
                new TokenFailureCase(
                        "non-textual JSON field",
                        new StubSecretsManagerClient(
                                responseWithString("{\"token\":42}")),
                        "token",
                        "textual field 'token'"),
                new TokenFailureCase(
                        "malformed JSON",
                        new StubSecretsManagerClient(responseWithString("{\"token\":")),
                        "token",
                        "Unable to parse the Secrets Manager secret as JSON"));
    }

    private static GetSecretValueResponse responseWithString(String value) {
        return GetSecretValueResponse.builder().secretString(value).build();
    }

    private record TokenFailureCase(
            String description,
            StubSecretsManagerClient client,
            String jsonField,
            String expectedMessage) {

        @Override
        public String toString() {
            return description;
        }
    }

    private static final class StubSecretsManagerClient {
        private final Deque<GetSecretValueResponse> responses = new ArrayDeque<>();
        private final List<GetSecretValueRequest> requests = new ArrayList<>();
        private boolean closed;

        private StubSecretsManagerClient(GetSecretValueResponse... responses) {
            this.responses.addAll(Arrays.asList(responses));
        }

        private SecretsManagerClient client() {
            return (SecretsManagerClient)
                    Proxy.newProxyInstance(
                            SecretsManagerClient.class.getClassLoader(),
                            new Class<?>[] {SecretsManagerClient.class},
                            (instance, method, arguments) -> {
                                switch (method.getName()) {
                                    case "getSecretValue":
                                        requests.add((GetSecretValueRequest) arguments[0]);
                                        return responses.removeFirst();
                                    case "close":
                                        closed = true;
                                        return null;
                                    case "serviceName":
                                        return "secretsmanager";
                                    case "toString":
                                        return "StubSecretsManagerClient";
                                    default:
                                        throw new UnsupportedOperationException(method.toString());
                                }
                            });
        }
    }
}
