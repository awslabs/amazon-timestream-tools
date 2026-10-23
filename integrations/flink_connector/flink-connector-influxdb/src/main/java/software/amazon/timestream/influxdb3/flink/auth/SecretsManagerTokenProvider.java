/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.auth;

import org.apache.flink.annotation.PublicEvolving;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

/**
 * Loads an InfluxDB token from AWS Secrets Manager using the default AWS credential provider
 * chain.
 *
 * <p>The secret can either be the token itself or a JSON object containing the configured field.
 * The AWS client and resolved token are transient and are created independently in each Flink sink
 * subtask.
 *
 * <p>The resolved token is retained until {@link #refreshToken()} or {@link #close()} is called.
 * The connector resolves it when the sink subtask starts and forces a refresh after an
 * authentication failure; it does not periodically poll Secrets Manager.
 */
@PublicEvolving
public final class SecretsManagerTokenProvider implements TokenProvider {
    private static final long serialVersionUID = 1L;
    private static final Duration API_CALL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String secretId;
    private final String jsonField;
    private final String region;
    private final String versionStage;

    private transient SecretsManagerClient client;
    private transient char[] cachedToken;

    private SecretsManagerTokenProvider(Builder builder) {
        this.secretId = requireNonBlank(builder.secretId, "secretId");
        this.jsonField = blankToNull(builder.jsonField);
        this.region = blankToNull(builder.region);
        this.versionStage = blankToNull(builder.versionStage);
    }

    /** @return a provider builder */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a provider for a plain-text secret using default settings.
     *
     * @param secretId secret name or ARN
     * @return a configured provider
     */
    public static SecretsManagerTokenProvider fromSecretId(String secretId) {
        return builder().setSecretId(secretId).build();
    }

    @Override
    public synchronized void open() {
        if (client != null) {
            return;
        }
        SecretsManagerClientBuilder builder = SecretsManagerClient.builder();
        builder.overrideConfiguration(
                override ->
                        override
                                .apiCallTimeout(API_CALL_TIMEOUT)
                                .apiCallAttemptTimeout(API_CALL_ATTEMPT_TIMEOUT));
        if (region != null) {
            builder.region(Region.of(region));
        }
        client = builder.build();
    }

    @Override
    public synchronized char[] getToken() throws IOException {
        if (cachedToken == null) {
            loadToken();
        }
        return Arrays.copyOf(cachedToken, cachedToken.length);
    }

    @Override
    public synchronized char[] refreshToken() throws IOException {
        loadToken();
        return Arrays.copyOf(cachedToken, cachedToken.length);
    }

    @Override
    public synchronized void close() {
        clearCachedToken();
        if (client != null) {
            client.close();
            client = null;
        }
    }

    void setClientForTesting(SecretsManagerClient client) {
        this.client = client;
    }

    private void loadToken() throws IOException {
        clearCachedToken();
        if (client == null) {
            open();
        }

        GetSecretValueRequest.Builder request =
                GetSecretValueRequest.builder().secretId(secretId);
        if (versionStage != null) {
            request.versionStage(versionStage);
        }

        final GetSecretValueResponse response;
        try {
            response = client.getSecretValue(request.build());
        } catch (RuntimeException e) {
            throw new IOException("Unable to retrieve the InfluxDB token from Secrets Manager", e);
        }

        String secret = response.secretString();
        if (secret == null) {
            SdkBytes binary = response.secretBinary();
            if (binary != null) {
                secret = binary.asUtf8String();
            }
        }
        if (secret == null) {
            throw new IOException("Secrets Manager returned no SecretString or SecretBinary");
        }

        String resolved = resolveToken(secret);
        if (resolved.isBlank()) {
            throw new IOException("The resolved InfluxDB token is empty");
        }
        cachedToken = resolved.toCharArray();
    }

    private String resolveToken(String secret) throws IOException {
        if (jsonField == null) {
            return secret.trim();
        }
        try {
            JsonNode value = OBJECT_MAPPER.readTree(secret).get(jsonField);
            if (value == null || !value.isTextual()) {
                throw new IOException(
                        "Secrets Manager secret does not contain textual field '" + jsonField + "'");
            }
            return value.textValue().trim();
        } catch (JsonProcessingException e) {
            throw new IOException("Unable to parse the Secrets Manager secret as JSON", e);
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("Unable to parse the Secrets Manager secret as JSON", e);
        }
    }

    private void clearCachedToken() {
        if (cachedToken != null) {
            Arrays.fill(cachedToken, '\0');
            cachedToken = null;
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return "SecretsManagerTokenProvider{"
                + "secretId='"
                + secretId
                + '\''
                + ", jsonField='"
                + jsonField
                + '\''
                + ", region='"
                + region
                + '\''
                + ", versionStage='"
                + versionStage
                + '\''
                + '}';
    }

    /** Builder for {@link SecretsManagerTokenProvider}. */
    @PublicEvolving
    public static final class Builder {
        private String secretId;
        private String jsonField;
        private String region;
        private String versionStage = "AWSCURRENT";

        private Builder() {}

        /**
         * Sets the secret name or ARN.
         *
         * @param secretId secret name or ARN
         * @return this builder
         */
        public Builder setSecretId(String secretId) {
            this.secretId = secretId;
            return this;
        }

        /**
         * Selects a top-level textual JSON field instead of using the full secret value.
         *
         * @param jsonField JSON field name, or {@code null} for a plain-text secret
         * @return this builder
         */
        public Builder setJsonField(String jsonField) {
            this.jsonField = jsonField;
            return this;
        }

        /**
         * Sets the Secrets Manager region.
         *
         * <p>When omitted, the AWS SDK default region provider chain is used.
         *
         * @param region AWS region identifier
         * @return this builder
         */
        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the Secrets Manager version stage.
         *
         * @param versionStage version stage, defaulting to {@code AWSCURRENT}
         * @return this builder
         */
        public Builder setVersionStage(String versionStage) {
            this.versionStage = versionStage;
            return this;
        }

        /** @return a validated token provider */
        public SecretsManagerTokenProvider build() {
            return new SecretsManagerTokenProvider(this);
        }
    }
}
