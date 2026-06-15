-- Read-model tables for the finance manager. Identical to the kf sample's schema (H2 ∩ MySQL
-- intersection), so the same read model is built on both stacks and the cluster IT's row-count /
-- net-sum assertions are directly comparable. Axon's own tables (domain_event_entry, token_entry,
-- dead_letter_entry, association_value_entry, saga_entry, snapshot_event_entry) are created by
-- Hibernate (ddl-auto=update) on the same datasource.
--
-- The secondary index is declared INLINE in the CREATE TABLE rather than as a standalone
-- CREATE INDEX IF NOT EXISTS: MySQL supports IF NOT EXISTS on CREATE TABLE but NOT on CREATE INDEX,
-- and an inline index rides the table's own IF NOT EXISTS so it stays idempotent on both backends.

CREATE TABLE IF NOT EXISTS pfm_user (
    user_id  BINARY(16)   NOT NULL,
    username VARCHAR(255) NOT NULL,
    PRIMARY KEY (user_id),
    CONSTRAINT uq_pfm_user_username UNIQUE (username)
);

CREATE TABLE IF NOT EXISTS pfm_operation (
    op_id   BINARY(16)  NOT NULL,
    user_id BINARY(16)  NOT NULL,
    op_type VARCHAR(8)  NOT NULL,
    amount  BIGINT      NOT NULL,
    tag     VARCHAR(64) NOT NULL,
    ts      BIGINT      NOT NULL,
    PRIMARY KEY (op_id),
    INDEX ix_pfm_op_user (user_id)
);

-- Node-presence heartbeat for membership-aware segment balancing (cluster mode only). Server-less
-- Axon has no membership view: a node owning zero token_entry rows is invisible to its peers, so a
-- segment cap derived purely from ownership cannot bootstrap (the first node to boot grabs every
-- segment and nothing ever makes it let go — the others stay empty). This table is the independent
-- presence signal: each cluster member beats its row while "running", and HeartbeatService sizes
-- maxClaimedSegments to ceil(segments / liveNodes). It is the Axon-sample analog of kf's cluster_nodes
-- heartbeat — epoch millis in a BIGINT, matching the kf cluster convention. Left empty (and unread)
-- in the single-node demo.
CREATE TABLE IF NOT EXISTS cluster_presence (
    node_id   VARCHAR(255) NOT NULL,
    last_seen BIGINT       NOT NULL,
    PRIMARY KEY (node_id)
);
