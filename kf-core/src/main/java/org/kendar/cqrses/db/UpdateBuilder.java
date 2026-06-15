package org.kendar.cqrses.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.StringJoiner;

/**
 * Column-keyed UPDATE builder. SET and WHERE clauses are keyed by column name
 * so the SQL fragment and its parameter share an index — no positional drift.
 * Refuses to execute without a WHERE clause; use {@link Db#update(String, Object...)}
 * for the (rare) legitimate table-wide update.
 */
public class UpdateBuilder {

    private final Db db;
    private final String table;
    private final LinkedHashMap<String, Object> sets = new LinkedHashMap<>();
    private final LinkedHashMap<String, Object> wheres = new LinkedHashMap<>();

    UpdateBuilder(Db db, String table) {
        this.db = db;
        this.table = table;
    }

    public UpdateBuilder set(String column, Object value) {
        sets.put(column, value);
        return this;
    }

    public UpdateBuilder where(String column, Object value) {
        wheres.put(column, value);
        return this;
    }

    public int execute() {
        if (sets.isEmpty()) throw new IllegalStateException("no columns to set");
        if (wheres.isEmpty()) throw new IllegalStateException("refusing UPDATE without WHERE");
        StringJoiner setJ = new StringJoiner(", ");
        for (String c : sets.keySet()) setJ.add(c + " = ?");
        StringJoiner whereJ = new StringJoiner(" AND ");
        for (String c : wheres.keySet()) whereJ.add(c + " = ?");
        String sql = "UPDATE " + table + " SET " + setJ + " WHERE " + whereJ;
        List<Object> args = new ArrayList<>(sets.size() + wheres.size());
        args.addAll(sets.values());
        args.addAll(wheres.values());
        return db.update(sql, args.toArray());
    }
}
