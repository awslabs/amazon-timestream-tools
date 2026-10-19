/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

/**
 * Escapes values for single-line logs and limits their size.
 *
 * <p>This utility does not redact secrets or other sensitive values.
 */
final class FailureLogUtils {
    static final int MAX_LOGGED_CHARACTERS = 4_096;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private FailureLogUtils() {}

    static String escapeAndTruncateForLog(String value) {
        if (value == null) {
            return "<null>";
        }
        StringBuilder escaped =
                new StringBuilder(Math.min(value.length(), MAX_LOGGED_CHARACTERS));
        int index = 0;
        int lastEllipsisSafeIndex = 0;
        int lastEllipsisSafeLength = 0;
        while (index < value.length()) {
            int codePoint = value.codePointAt(index);
            if (requiresEscaping(codePoint)) {
                escaped.append("\\u");
                escaped.append(HEX[(codePoint >>> 12) & 0xf]);
                escaped.append(HEX[(codePoint >>> 8) & 0xf]);
                escaped.append(HEX[(codePoint >>> 4) & 0xf]);
                escaped.append(HEX[codePoint & 0xf]);
            } else {
                escaped.appendCodePoint(codePoint);
            }
            index += Character.charCount(codePoint);
            if (escaped.length() <= MAX_LOGGED_CHARACTERS - 3) {
                lastEllipsisSafeIndex = index;
                lastEllipsisSafeLength = escaped.length();
            }
            if (escaped.length() > MAX_LOGGED_CHARACTERS) {
                escaped.setLength(lastEllipsisSafeLength);
                index = lastEllipsisSafeIndex;
                break;
            }
        }
        if (index < value.length()) {
            escaped.append("...");
        }
        return escaped.toString();
    }

    private static boolean requiresEscaping(int codePoint) {
        return Character.isISOControl(codePoint) || codePoint == 0x2028 || codePoint == 0x2029;
    }
}
