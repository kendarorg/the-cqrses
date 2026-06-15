package org.kendar.cqrses.db;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlApproximatorTest {

    @Test
    void nullValueRendersAsSqlNull() {
        assertEquals("NULL", SqlApproximator.formatValue(null));
    }

    @Test
    void booleanRendersAsOneOrZero() {
        assertEquals("1", SqlApproximator.formatValue(Boolean.TRUE));
        assertEquals("0", SqlApproximator.formatValue(Boolean.FALSE));
    }

    @Test
    void numbersRenderUnquoted() {
        assertEquals("42", SqlApproximator.formatValue(42));
        assertEquals("42", SqlApproximator.formatValue(42L));
        assertEquals("3.5", SqlApproximator.formatValue(3.5d));
    }

    @Test
    void stringsAreSingleQuotedAndEscaped() {
        assertEquals("'alice'", SqlApproximator.formatValue("alice"));
        assertEquals("'O''Brien'", SqlApproximator.formatValue("O'Brien"));
    }

    @Test
    void instantAndUuidRenderAsQuotedText() {
        Instant t = Instant.parse("2026-06-09T10:15:30Z");
        assertEquals("'2026-06-09T10:15:30Z'", SqlApproximator.formatValue(t));

        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000ff");
        assertEquals("'00000000-0000-0000-0000-0000000000ff'", SqlApproximator.formatValue(id));
    }

    @Test
    void shortByteArrayRendersAsHexLiteral() {
        byte[] bytes = {0x00, 0x0F, (byte) 0xA0, (byte) 0xFF};
        assertEquals("X'000FA0FF'", SqlApproximator.formatValue(bytes));
    }

    @Test
    void sixteenByteArrayCarriesUuidComment() {
        UUID id = UUID.fromString("11111111-2222-3333-4444-555555555555");
        String rendered = SqlApproximator.formatValue(UuidBytes.toBytes(id));
        assertTrue(rendered.startsWith("X'11111111222233334444555555555555'"), rendered);
        assertTrue(rendered.endsWith("/* uuid=" + id + " */"), rendered);
    }

    @Test
    void longByteArrayIsTruncatedWithLengthSuffix() {
        byte[] bytes = new byte[SqlApproximator.MAX_BYTES + 10];
        String rendered = SqlApproximator.formatValue(bytes);
        // Two hex chars per shown byte, plus X'' wrapper.
        int hexChars = SqlApproximator.MAX_BYTES * 2;
        assertTrue(rendered.startsWith("X'"), rendered);
        assertTrue(rendered.contains("'...(len=" + bytes.length + ")"), rendered);
        assertEquals(hexChars, rendered.indexOf("'...") - 2);
    }

    @Test
    void positionalPlaceholdersAreReplacedInOrder() {
        String sql = "INSERT INTO t (a, b, c) VALUES (?, ?, ?)";
        String out = SqlApproximator.approximate(sql, 1, "x", null);
        assertEquals("INSERT INTO t (a, b, c) VALUES (1, 'x', NULL)", out);
    }

    @Test
    void questionMarkInsideStringLiteralIsNotReplaced() {
        String sql = "SELECT * FROM t WHERE label = 'is it? yes' AND id = ?";
        String out = SqlApproximator.approximate(sql, 7);
        assertEquals("SELECT * FROM t WHERE label = 'is it? yes' AND id = 7", out);
    }

    @Test
    void extraArgumentsAreIgnored() {
        String out = SqlApproximator.approximate("SELECT ?", 1, 2, 3);
        assertEquals("SELECT 1", out);
    }

    @Test
    void missingArgumentsLeavePlaceholdersIntact() {
        String out = SqlApproximator.approximate("SELECT ?, ?", 1);
        assertEquals("SELECT 1, ?", out);
    }

    @Test
    void noArgsReturnsSqlUnchanged() {
        String sql = "SELECT 1";
        assertEquals(sql, SqlApproximator.approximate(sql));
        assertNull(SqlApproximator.approximate(null));
    }
}
