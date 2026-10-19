/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.util.Base64;

/**
 * Self-signed {@code localhost} TLS material for tests.
 *
 * <p>The build generates the key pair under {@code target/test-tls} and points the test JVM's
 * default truststore at its certificate, so the connector validates test servers like any other
 * HTTPS endpoint.
 */
public final class TestTls {
    private static final String KEYSTORE_PROPERTY = "test.tls.keystore";
    private static final String PASSWORD_PROPERTY = "test.tls.password";
    private static final String ALIAS = "test";

    private TestTls() {}

    /** @return WireMock configuration serving HTTPS only on a dynamic port */
    public static WireMockConfiguration wireMockHttps() {
        return WireMockConfiguration.wireMockConfig()
                .httpDisabled(true)
                .dynamicHttpsPort()
                .keystoreType("PKCS12")
                .keystorePath(keystorePath().toString())
                .keystorePassword(password())
                .keyManagerPassword(password());
    }

    /** @return the HTTPS base URL of a server created with {@link #wireMockHttps()} */
    public static String baseUrl(WireMockServer server) {
        return "https://localhost:" + server.httpsPort();
    }

    /** @return a server-side TLS context presenting the test certificate */
    public static SSLContext serverSslContext() {
        try {
            KeyManagerFactory keyManagers =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keyStore(), password().toCharArray());
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException | IOException error) {
            throw new IllegalStateException("Unable to load the test TLS key", error);
        }
    }

    /** @return the test certificate in PEM format */
    public static String certificatePem() {
        try {
            return pem("CERTIFICATE", keyStore().getCertificate(ALIAS).getEncoded());
        } catch (GeneralSecurityException | IOException error) {
            throw new IllegalStateException("Unable to export the test TLS certificate", error);
        }
    }

    /** @return the test private key in PKCS #8 PEM format */
    public static String privateKeyPem() {
        try {
            Key key = keyStore().getKey(ALIAS, password().toCharArray());
            return pem("PRIVATE KEY", key.getEncoded());
        } catch (GeneralSecurityException | IOException error) {
            throw new IllegalStateException("Unable to export the test TLS key", error);
        }
    }

    private static KeyStore keyStore() throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(keystorePath())) {
            keyStore.load(input, password().toCharArray());
        }
        return keyStore;
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN "
                + type
                + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(der)
                + "\n-----END "
                + type
                + "-----\n";
    }

    private static Path keystorePath() {
        return Path.of(requiredProperty(KEYSTORE_PROPERTY));
    }

    private static String password() {
        return requiredProperty(PASSWORD_PROPERTY);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException(
                    name + " is not set; run tests through Maven, which generates test TLS material");
        }
        return value;
    }
}
