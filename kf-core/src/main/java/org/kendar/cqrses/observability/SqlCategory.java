package org.kendar.cqrses.observability;

import java.util.Locale;

/**
 * Derives a low-cardinality {@code verb:table} category from a SQL statement so
 * the {@code Db} wrapper can time queries by category rather than per statement.
 *
 * <p>Examples: {@code SELECT ... FROM event_entry WHERE ...} &rarr;
 * {@code select:event_entry}; {@code INSERT INTO dlq_item (...)} &rarr;
 * {@code insert:dlq_item}; {@code UPDATE processor_checkpoint SET ...} &rarr;
 * {@code update:processor_checkpoint}.
 *
 * <p>The framework's SQL is a small, fixed, single-table-keyed set (the stores
 * and the cluster tables), so a cheap token scan for the table after
 * {@code FROM} / {@code INTO} / {@code UPDATE} / {@code DELETE FROM} /
 * {@code JOIN} is sufficient and keeps the resulting meter cardinality bounded
 * (~one category per table per verb). Anything unrecognised collapses to
 * {@code <verb>:other} (or {@code other:other}) rather than leaking a unique
 * label per statement.
 */
public final class SqlCategory {

    private SqlCategory() {
    }

    public static String of(String sql) {
        if (sql == null || sql.isBlank()) {
            return "other:other";
        }
        String[] tok = sql.trim().split("\\s+");
        String verb = tok[0].toLowerCase(Locale.ROOT);
        String table = switch (verb) {
            case "select", "delete" -> tableAfter(tok, "from");
            case "insert", "replace" -> tableAfter(tok, "into");
            case "update" -> (tok.length > 1) ? clean(tok[1]) : "other";
            default -> "other";
        };
        return verb + ":" + table;
    }

    /**
     * First token following any occurrence of {@code keyword} (case-insensitive) that
     * cleans to a real identifier. Scanning every occurrence (not just the first) lets
     * wrapped queries like {@code SELECT * FROM (SELECT ... FROM event_entry ...)}
     * resolve via the inner {@code FROM} instead of collapsing to {@code other}.
     */
    private static String tableAfter(String[] tok, String keyword) {
        for (int i = 0; i < tok.length - 1; i++) {
            if (tok[i].equalsIgnoreCase(keyword)) {
                String table = clean(tok[i + 1]);
                if (!"other".equals(table)) {
                    return table;
                }
            }
        }
        return "other";
    }

    /** Strip trailing punctuation / qualifiers and lower-case; bound the result. */
    private static String clean(String raw) {
        String s = raw.toLowerCase(Locale.ROOT);
        int cut = s.length();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean ident = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
            if (!ident) {
                cut = i;
                break;
            }
        }
        s = s.substring(0, cut);
        return s.isEmpty() ? "other" : s;
    }
}
