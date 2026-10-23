/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.examples;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/** Post-package assertions for the deployable Managed Flink application JAR. */
public final class ManagedFlinkExampleShadedJarVerifier {
    private static final String MAIN_CLASS =
            "software.amazon.timestream.influxdb3.flink.examples.KinesisToInfluxDB3Application";
    private static final String SHADED_PREFIX =
            "software/amazon/timestream/influxdb3/flink/examples/shaded/";
    private static final String TABLE_FACTORY_SERVICES =
            "META-INF/services/org.apache.flink.table.factories.Factory";
    /** Factories the SQL pipeline discovers: Kinesis source, JSON format, and InfluxDB 3 sink. */
    private static final List<String> SQL_PIPELINE_FACTORIES =
            List.of(
                    "org.apache.flink.connector.kinesis.table.KinesisDynamicTableFactory",
                    "org.apache.flink.formats.json.JsonFormatFactory",
                    "software.amazon.timestream.influxdb3.flink.table.InfluxDB3DynamicTableSinkFactory");

    private ManagedFlinkExampleShadedJarVerifier() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("Expected the shaded JAR path");
        }
        Path shadedJar = Path.of(arguments[0]);
        if (!Files.isRegularFile(shadedJar)) {
            throw new IllegalStateException("Shaded JAR does not exist: " + shadedJar);
        }

        Set<String> classes = new HashSet<>();
        boolean hasLicense = false;
        boolean hasNotice = false;
        try (JarFile jar = new JarFile(shadedJar.toFile())) {
            Manifest manifest = jar.getManifest();
            String manifestMainClass =
                    manifest == null
                            ? null
                            : manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
            if (!MAIN_CLASS.equals(manifestMainClass)) {
                throw new IllegalStateException(
                        "Unexpected shaded JAR Main-Class: " + manifestMainClass);
            }

            var iterator = jar.entries();
            while (iterator.hasMoreElements()) {
                JarEntry entry = iterator.nextElement();
                if (entry.getName().endsWith(".class")
                        && !classes.add(entry.getName())) {
                    throw new IllegalStateException(
                            "Duplicate class in shaded JAR: " + entry.getName());
                }
                if (entry.getName().startsWith("META-INF/native/")
                        && entry.getName().contains("netty_tcnative")) {
                    throw new IllegalStateException(
                            "Relocation-incompatible tcnative resource: " + entry.getName());
                }
                hasLicense |= "META-INF/LICENSE.txt".equals(entry.getName());
                hasNotice |= "META-INF/NOTICE".equals(entry.getName());
            }
            requireTableFactories(jar);
        }
        if (!classes.contains(MAIN_CLASS.replace('.', '/') + ".class")) {
            throw new IllegalStateException("Shaded JAR is missing its application entry point");
        }
        if (!hasLicense || !hasNotice) {
            throw new IllegalStateException(
                    "Shaded JAR must contain META-INF/LICENSE.txt and META-INF/NOTICE");
        }

        requirePackage(classes, SHADED_PREFIX + "io/netty/");
        requirePackage(classes, SHADED_PREFIX + "io/grpc/");
        requirePackage(classes, SHADED_PREFIX + "com/google/common/");
        requirePackage(classes, SHADED_PREFIX + "com/google/protobuf/");
        rejectPackage(classes, "io/netty/");
        rejectPackage(classes, "io/grpc/");
        rejectPackage(classes, "com/google/common/");
        rejectPackage(classes, "com/google/protobuf/");

        try (URLClassLoader isolated =
                new URLClassLoader(
                        new URL[] {shadedJar.toUri().toURL()},
                        ClassLoader.getPlatformClassLoader())) {
            Class<?> openSsl =
                    isolated.loadClass(
                            "software.amazon.timestream.influxdb3.flink.examples.shaded.io.netty.handler.ssl.OpenSsl");
            Method isAvailable = openSsl.getMethod("isAvailable");
            if (Boolean.TRUE.equals(isAvailable.invoke(null))) {
                throw new IllegalStateException(
                        "Shaded Netty unexpectedly loaded an unverified native TLS provider");
            }
        }
    }

    private static void requireTableFactories(JarFile jar) throws IOException {
        JarEntry services = jar.getJarEntry(TABLE_FACTORY_SERVICES);
        if (services == null) {
            throw new IllegalStateException("Shaded JAR is missing " + TABLE_FACTORY_SERVICES);
        }
        String registered;
        try (InputStream input = jar.getInputStream(services)) {
            registered = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        Set<String> factories = new HashSet<>(registered.lines().map(String::trim).toList());
        for (String factory : SQL_PIPELINE_FACTORIES) {
            if (!factories.contains(factory)) {
                throw new IllegalStateException(
                        TABLE_FACTORY_SERVICES + " does not register " + factory);
            }
        }
    }

    private static void requirePackage(Set<String> entries, String prefix) {
        if (entries.stream().noneMatch(entry -> entry.startsWith(prefix))) {
            throw new IllegalStateException("Missing relocated package " + prefix);
        }
    }

    private static void rejectPackage(Set<String> entries, String prefix) {
        if (entries.stream().anyMatch(entry -> entry.startsWith(prefix))) {
            throw new IllegalStateException("Found unrelocated package " + prefix);
        }
    }
}
