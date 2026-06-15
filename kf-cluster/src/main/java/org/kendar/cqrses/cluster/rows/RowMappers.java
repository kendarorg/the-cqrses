package org.kendar.cqrses.cluster.rows;

import org.kendar.cqrses.db.RowMapper;

/**
 * {@link RowMapper}s for the kf-cluster tables. Nullable {@code BIGINT} columns are read with
 * {@code getLong} + {@code wasNull} so a SQL {@code NULL} maps to a Java {@code null} rather than
 * a silent {@code 0}.
 */
public final class RowMappers {

    private RowMappers() {
    }

    public static final RowMapper<NodeRow> NODE = (rs, i) -> new NodeRow(
            rs.getString("node_id"),
            rs.getString("host"),
            rs.getInt("liveness_port"),
            rs.getLong("last_heartbeat"));

    public static final RowMapper<AssignmentRow> ASSIGNMENT = (rs, i) -> {
        long leaseUntil = rs.getLong("lease_until");
        Long leaseUntilOrNull = rs.wasNull() ? null : leaseUntil;
        return new AssignmentRow(
                rs.getInt("item_id"),
                rs.getString("owner_node"),
                rs.getLong("epoch"),
                rs.getString("lease_holder"),
                leaseUntilOrNull);
    };
}
