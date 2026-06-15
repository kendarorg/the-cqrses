package org.kendar.pfm.metrics;

import java.util.Locale;

/**
 * Verbatim copy of kf-core's {@code SqlCategory}: derives a low-cardinality {@code verb:table}
 * category from a SQL statement so the datasource-proxy hook can time queries by category rather
 * than per statement, exactly matching kf's {@code kf.sql.execute{category=...}} meter. Under Axon
 * the tables are different ({@code domain_event_entry}, {@code token_entry}, {@code dead_letter_entry},
 * …) but the category shape (and the meter) is identical, so the comparison report renders unchanged.
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

    private static String tableAfter(String[] tok, String keyword) {
        for (int i = 0; i < tok.length - 1; i++) {
            if (tok[i].equalsIgnoreCase(keyword)) {
                return clean(tok[i + 1]);
            }
        }
        return "other";
    }

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
