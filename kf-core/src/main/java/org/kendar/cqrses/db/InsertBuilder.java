package org.kendar.cqrses.db;

import java.util.LinkedHashMap;
import java.util.StringJoiner;

/**
 * Column-keyed INSERT builder. Names and values share an index inside a
 * {@link LinkedHashMap} so column-list / value-list drift is impossible by
 * construction. Positional {@code ?} parameters only — no named-parameter
 * dependency.
 * <p>
 * Dialect note: the PostgreSQL {@code ON CONFLICT DO NOTHING} of the original
 * has been replaced with MySQL's {@code INSERT IGNORE} (valid in MySQL and in
 * H2's MySQL compatibility mode), exposed via {@link #ignore()}.
 */
public class InsertBuilder {

    private final Db db;
    private final String table;
    private final LinkedHashMap<String, Object> cols = new LinkedHashMap<>();
    private boolean ignore;

    InsertBuilder(Db db, String table) {
        this.db = db;
        this.table = table;
    }

    public InsertBuilder set(String column, Object value) {
        cols.put(column, value);
        return this;
    }

    /**
     * Emits {@code INSERT IGNORE INTO ...} — a duplicate-key row is silently
     * dropped instead of throwing. Portable across MySQL and H2 MySQL-mode.
     */
    public InsertBuilder ignore() {
        this.ignore = true;
        return this;
    }

    public int execute() {
        if (cols.isEmpty()) throw new IllegalStateException("no columns set");
        StringJoiner names = new StringJoiner(", ", "(", ")");
        StringJoiner marks = new StringJoiner(", ", "(", ")");
        for (String c : cols.keySet()) {
            names.add(c);
            marks.add("?");
        }
        StringBuilder sql = new StringBuilder(ignore ? "INSERT IGNORE INTO " : "INSERT INTO ")
                .append(table).append(' ').append(names)
                .append(" VALUES ").append(marks);
        return db.update(sql.toString(), cols.values().toArray());
    }
}
