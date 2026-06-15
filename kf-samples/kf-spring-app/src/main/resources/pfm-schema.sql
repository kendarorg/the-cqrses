-- Read-model tables for the finance manager. All SQL in the H2 ∩ MySQL intersection so the same
-- DDL runs on a real MySQL server (the kf-cluster-it run) and on H2 (MODE=MySQL) for the demo. The
-- framework's own tables are created by the starter.
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
