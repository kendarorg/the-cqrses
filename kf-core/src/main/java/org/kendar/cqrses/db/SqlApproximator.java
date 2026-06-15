package org.kendar.cqrses.db;

import java.util.UUID;

/**
 * Builds an <b>approximate</b>, human-readable rendering of a parameterised SQL statement by
 * substituting each positional {@code ?} placeholder with a literal form of its bound value.
 * <p>
 * <b>For logging / debugging only.</b> The result is <em>not</em> safe to execute — real statements
 * always go through {@link java.sql.PreparedStatement} parameter binding in {@link DefaultDb}. This
 * class never sees a database; it only formats values for a log line.
 * <p>
 * The {@code Db} layer uses positional {@code ?} placeholders exclusively (no named {@code :name}
 * parameters), so — unlike the broader SQL-rendering helpers in other stacks — only the positional
 * branch is implemented. Placeholders inside single-quoted string literals are skipped, and any
 * mismatch between placeholder and argument counts is tolerated (leftover {@code ?} are left intact,
 * extra arguments are ignored).
 * <p>
 * Value formatting follows the H2/MySQL dialect the framework targets:
 * <ul>
 *   <li>{@code null} &rarr; {@code NULL}</li>
 *   <li>{@code Boolean} &rarr; {@code 1} / {@code 0}</li>
 *   <li>{@code Number} &rarr; its decimal text</li>
 *   <li>{@code byte[]} &rarr; a {@code X'..'} hex literal, truncated past {@link #MAX_BYTES} bytes;
 *       a 16-byte array (the {@link UuidBytes} encoding) additionally carries a {@code /* uuid=.. *&#47;}
 *       comment</li>
 *   <li>everything else (String, Instant, UUID, ...) &rarr; single-quoted, with {@code '} doubled</li>
 * </ul>
 */
public final class SqlApproximator {

    /** Maximum number of {@code byte[]} bytes rendered as hex before truncation. */
    static final int MAX_BYTES = 64;

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private SqlApproximator() {
    }

    /**
     * Interpolate {@code args} into {@code sql}, replacing positional {@code ?} placeholders left to
     * right while skipping {@code ?} characters inside single-quoted literals.
     */
    public static String approximate(String sql, Object... args) {
        if (sql == null) return null;
        if (args == null || args.length == 0) return sql;

        StringBuilder out = new StringBuilder(sql.length() + 32);
        int argIndex = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // Toggle string state; a doubled '' (escaped quote) stays inside the literal.
                inString = !inString;
                out.append(c);
            } else if (c == '?' && !inString && argIndex < args.length) {
                out.append(formatValue(args[argIndex++]));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** Render a single bound value as a SQL literal. Package-private for testing. */
    static String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean b) {
            return b ? "1" : "0";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            return formatBytes(bytes);
        }
        return quote(value.toString());
    }

    private static String formatBytes(byte[] bytes) {
        int shown = Math.min(bytes.length, MAX_BYTES);
        StringBuilder sb = new StringBuilder(shown * 2 + 16);
        sb.append("X'");
        for (int i = 0; i < shown; i++) {
            int v = bytes[i] & 0xFF;
            sb.append(HEX[v >>> 4]).append(HEX[v & 0x0F]);
        }
        sb.append('\'');
        if (bytes.length > MAX_BYTES) {
            sb.append("...(len=").append(bytes.length).append(')');
        }
        if (bytes.length == 16) {
            sb.append(" /* uuid=").append(UuidBytes.fromBytes(bytes)).append(" */");
        }
        return sb.toString();
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
