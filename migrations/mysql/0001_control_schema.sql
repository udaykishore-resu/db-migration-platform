-- Control schema for the migration platform (MySQL 8.0 / Aurora MySQL 3).
--
-- Mirrors the PostgreSQL schema. Where the two differ, the difference is noted
-- inline, because a silent divergence between the two control schemas is a very
-- efficient way to make a bug reproduce on only one engine.
--
-- Apply with:  mysql --database=migration_ctl < 0001_control_schema.sql

CREATE DATABASE IF NOT EXISTS migration_ctl
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs;
-- Note the case-sensitive collation. The default utf8mb4_0900_ai_ci is
-- accent- and case-insensitive, which would make 'A-1' and 'a-1' the same
-- primary key value in this schema while remaining distinct on the source. Every
-- row key in here is a hash or an identifier where that distinction matters.

USE migration_ctl;

CREATE TABLE IF NOT EXISTS migration_state (
    migration_id              VARCHAR(128) NOT NULL,
    phase                     VARCHAR(32)  NOT NULL DEFAULT 'planning',
    source_engine             VARCHAR(32),
    target_engine             VARCHAR(32),

    parts_total               INT          NOT NULL DEFAULT 0,
    parts_loaded              INT          NOT NULL DEFAULT 0,

    reconcile_ran_at          DATETIME(6),
    reconcile_findings        INT          NOT NULL DEFAULT 0,
    reconcile_complete        TINYINT(1)   NOT NULL DEFAULT 0,

    reverse_replication_armed TINYINT(1)   NOT NULL DEFAULT 0,

    started_at                DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    updated_at                DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),

    PRIMARY KEY (migration_id),
    CONSTRAINT migration_state_parts_sane CHECK (parts_loaded <= parts_total)
) ENGINE=InnoDB;

-- Written inside the same transaction as the data. See the PostgreSQL schema for
-- why that must never be relaxed.
CREATE TABLE IF NOT EXISTS applied_offset (
    migration_id VARCHAR(128) NOT NULL,
    topic        VARCHAR(255) NOT NULL,
    `partition`  INT          NOT NULL,
    `offset`     BIGINT       NOT NULL,
    last_lsn     BIGINT       NOT NULL DEFAULT 0,
    updated_at   DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    PRIMARY KEY (migration_id, topic, `partition`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dead_letter (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    migration_id      VARCHAR(128) NOT NULL,
    source_table      VARCHAR(512) NOT NULL,
    op                VARCHAR(8)   NOT NULL,

    row_key_hash      VARCHAR(64)  NOT NULL,
    row_key           TEXT         NOT NULL,

    payload           LONGTEXT,
    payload_encrypted TINYINT(1)   NOT NULL DEFAULT 0,

    error_class       VARCHAR(16)  NOT NULL,
    last_error        TEXT,
    attempts          INT          NOT NULL DEFAULT 0,
    status            VARCHAR(16)  NOT NULL DEFAULT 'pending',

    first_seen_at     DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    next_retry_at     DATETIME(6),
    claimed_at        DATETIME(6),
    claimed_by        VARCHAR(128),
    resolved_at       DATETIME(6),
    requeued_by       VARCHAR(128),

    source_lsn        BIGINT       NOT NULL DEFAULT 0,
    topic             VARCHAR(255),
    `partition`       INT          NOT NULL DEFAULT 0,
    `offset`          BIGINT       NOT NULL DEFAULT 0,

    PRIMARY KEY (id),
    CONSTRAINT dead_letter_status_known
        CHECK (status IN ('pending','retrying','quarantined','resolved','discarded'))
) ENGINE=InnoDB;

-- MySQL has no partial indexes, so status leads the index instead of filtering
-- it. The effect is the same for the claim query; the index is simply larger.
CREATE INDEX dead_letter_due_idx
    ON dead_letter (migration_id, status, next_retry_at);
CREATE INDEX dead_letter_table_idx
    ON dead_letter (migration_id, source_table(191), status);
CREATE INDEX dead_letter_key_idx
    ON dead_letter (migration_id, row_key_hash);

CREATE TABLE IF NOT EXISTS part_state (
    migration_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(191) NOT NULL,
    part_index   INT          NOT NULL,
    rows_loaded  BIGINT       NOT NULL DEFAULT 0,
    sha256       VARCHAR(64),
    state        VARCHAR(16)  NOT NULL DEFAULT 'sealed',
    extract_lsn  BIGINT       NOT NULL DEFAULT 0,
    attempts     INT          NOT NULL DEFAULT 0,
    last_error   TEXT,
    sealed_at    DATETIME(6),
    loaded_at    DATETIME(6),
    PRIMARY KEY (migration_id, source_table, part_index)
) ENGINE=InnoDB;
-- source_table is VARCHAR(191) rather than 512 because it participates in the
-- primary key and InnoDB's index key limit is 3072 bytes, which utf8mb4 spends
-- four bytes per character against.

CREATE INDEX part_state_pending_idx ON part_state (migration_id, state);

CREATE TABLE IF NOT EXISTS chunk_state (
    migration_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(191) NOT NULL,
    chunk_id     VARCHAR(128) NOT NULL,
    low_key      TEXT,
    high_key     TEXT,
    rows_read    BIGINT       NOT NULL DEFAULT 0,
    rows_emitted BIGINT       NOT NULL DEFAULT 0,
    rows_evicted BIGINT       NOT NULL DEFAULT 0,
    state        VARCHAR(16)  NOT NULL DEFAULT 'pending',
    started_at   DATETIME(6),
    completed_at DATETIME(6),
    PRIMARY KEY (migration_id, source_table, chunk_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS recon_run (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    migration_id   VARCHAR(128) NOT NULL,
    source_table   VARCHAR(512) NOT NULL,
    started_at     DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    finished_at    DATETIME(6),
    findings       INT          NOT NULL DEFAULT 0,
    digest_queries INT          NOT NULL DEFAULT 0,
    row_reads      INT          NOT NULL DEFAULT 0,
    ranges_visited INT          NOT NULL DEFAULT 0,
    deepest        INT          NOT NULL DEFAULT 0,
    complete       TINYINT(1)   NOT NULL DEFAULT 0,
    mode           VARCHAR(16)  NOT NULL DEFAULT 'continuous',
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE INDEX recon_run_recent_idx
    ON recon_run (migration_id, source_table(191), started_at DESC);

CREATE TABLE IF NOT EXISTS recon_finding (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    run_id       BIGINT       NOT NULL,
    migration_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(512) NOT NULL,
    kind         VARCHAR(32)  NOT NULL,
    row_key_hash VARCHAR(64),
    row_key      TEXT,
    range_low    TEXT,
    range_high   TEXT,
    source_rows  BIGINT,
    target_rows  BIGINT,
    detail       TEXT,
    found_at     DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    repaired_at  DATETIME(6),
    PRIMARY KEY (id),
    CONSTRAINT recon_finding_run_fk FOREIGN KEY (run_id)
        REFERENCES recon_run(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE INDEX recon_finding_open_idx
    ON recon_finding (migration_id, source_table(191), kind);

CREATE TABLE IF NOT EXISTS cutover_audit (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    migration_id  VARCHAR(128) NOT NULL,
    action        VARCHAR(32)  NOT NULL,
    performed_by  VARCHAR(128) NOT NULL,
    performed_at  DATETIME(6)  NOT NULL DEFAULT UTC_TIMESTAMP(6),
    gate_ready    TINYINT(1)   NOT NULL,
    blockers      TEXT,
    override      TINYINT(1)   NOT NULL DEFAULT 0,
    reason        TEXT,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE INDEX cutover_audit_migration_idx
    ON cutover_audit (migration_id, performed_at DESC);
