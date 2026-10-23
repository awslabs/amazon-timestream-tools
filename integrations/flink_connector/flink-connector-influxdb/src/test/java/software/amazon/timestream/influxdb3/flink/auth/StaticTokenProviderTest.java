/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.auth;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticTokenProviderTest {

    @Test
    void returnsDefensiveCopiesForCurrentAndRefreshedTokens() throws Exception {
        char[] source = "fixed-token".toCharArray();
        StaticTokenProvider provider = new StaticTokenProvider(source);
        Arrays.fill(source, 'x');
        char[] retainedToken = retainedToken(provider);

        provider.open();
        char[] current = provider.getToken();
        current[0] = 'x';
        char[] refreshed = provider.refreshToken();
        provider.close();

        assertThat(refreshed).containsExactly("fixed-token".toCharArray());
        assertThat(retainedToken).containsOnly('\0');
        assertThat(provider.toString()).doesNotContain("fixed-token");

        Arrays.fill(current, '\0');
        Arrays.fill(refreshed, '\0');
    }

    @Test
    void rejectsNullEmptyAndBlankTokens() {
        assertThatThrownBy(() -> new StaticTokenProvider((String) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new StaticTokenProvider((char[]) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new StaticTokenProvider(new char[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StaticTokenProvider(new char[] {' ', '\t', '\n'}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static char[] retainedToken(StaticTokenProvider provider) {
        try {
            Field field = StaticTokenProvider.class.getDeclaredField("token");
            field.setAccessible(true);
            return (char[]) field.get(provider);
        } catch (ReflectiveOperationException error) {
            throw new AssertionError("Unable to access retained token", error);
        }
    }
}
