package org.kendar.cqrses.observability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlCategoryTest {

    @Test
    void plainSelect() {
        assertEquals("select:event_entry",
                SqlCategory.of("SELECT * FROM event_entry WHERE aggregate_id = ? ORDER BY sequence"));
    }

    @Test
    void plainInsert() {
        assertEquals("insert:dlq_item",
                SqlCategory.of("INSERT INTO dlq_item (id, sequence_id, ordinal) VALUES (?,?,?)"));
    }

    @Test
    void insertOnDuplicateKey() {
        assertEquals("insert:processor_checkpoint",
                SqlCategory.of("INSERT INTO processor_checkpoint(processing_group, segment, source_segment, last_seq, updated_at) "
                        + "VALUES (?,?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE last_seq = GREATEST(last_seq, VALUES(last_seq)), "
                        + "updated_at = VALUES(updated_at)"));
    }

    @Test
    void plainUpdate() {
        assertEquals("update:cluster_assignments",
                SqlCategory.of("UPDATE cluster_assignments SET lease_until = ? WHERE item_id = ? AND lease_holder = ?"));
    }

    @Test
    void plainDelete() {
        assertEquals("delete:cluster_nodes",
                SqlCategory.of("DELETE FROM cluster_nodes WHERE last_heartbeat < ?"));
    }

    @Test
    void replaceInto() {
        assertEquals("replace:snapshot_entry",
                SqlCategory.of("REPLACE INTO snapshot_entry (aggregate_id, sequence, payload) VALUES (?,?,?)"));
    }

    /** The verbatim MySQL fairness tail-read: the table must resolve via the inner FROM. */
    @Test
    void wrappedSubqueryResolvesInnerTable() {
        assertEquals("select:event_entry",
                SqlCategory.of("SELECT * FROM (" +
                        "  SELECT event_entry.*, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY segment_seq) AS rn " +
                        "  FROM event_entry WHERE (segment = ? AND segment_seq > ?)" +
                        ") ranked WHERE rn <= ? ORDER BY segment, segment_seq LIMIT ?"));
    }

    @Test
    void subqueryWithoutSpaceAfterParen() {
        assertEquals("select:event_entry",
                SqlCategory.of("SELECT * FROM (SELECT * FROM event_entry WHERE segment = ?) t LIMIT ?"));
    }

    @Test
    void nullAndBlank() {
        assertEquals("other:other", SqlCategory.of(null));
        assertEquals("other:other", SqlCategory.of("   "));
    }

    @Test
    void unknownVerb() {
        assertEquals("create:other", SqlCategory.of("CREATE TABLE foo (id INT)"));
        assertEquals("with:other", SqlCategory.of("WITH x AS (SELECT 1) SELECT * FROM x"));
    }

    @Test
    void selectWithNoResolvableTable() {
        assertEquals("select:other", SqlCategory.of("SELECT 1"));
    }
}
