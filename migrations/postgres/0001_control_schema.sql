-- Control schema for the migration platform (PostgreSQL / Aurora PostgreSQL).
--
-- Everything the platform needs to know about its own progress lives here, in
-- the target database. That is a deliberate choice rather than a convenience:
-- putting the offset table in the same database as the data is what allows the
-- offset to be committed in the same transaction as the rows it accounts for,
-- which is the difference between at-least-once delivery and effectively-once
-- application.
--
-- Apply with:  psql -v ON_ERROR_STOP=1 -f 0001_control_schema.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS migration_ctl;

-- ---------------------------------------------------------------------------
-- migration_state: one row per migration, holding everything the cutover gate
-- reads. Keeping it in one row means the gate evaluates against a single
-- consistent snapshot rather than against five counters that were true at five
-- slightly different moments.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.migration_state (
    migration_id              VARCHAR(128) PRIMARY KEY,
    phase                     VARCHAR(32)  NOT NULL DEFAULT 'planning',
    source_engine             VARCHAR(32),
    target_engine             VARCHAR(32),

    parts_total               INTEGER      NOT NULL DEFAULT 0,
    parts_loaded              INTEGER      NOT NULL DEFAULT 0,

    reconcile_ran_at          TIMESTAMPTZ,
    reconcile_findings        INTEGER      NOT NULL DEFAULT 0,
    reconcile_complete        BOOLEAN      NOT NULL DEFAULT FALSE,

    reverse_replication_armed BOOLEAN      NOT NULL DEFAULT FALSE,

    started_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT migration_state_parts_sane CHECK (parts_loaded <= parts_total)
);

-- ---------------------------------------------------------------------------
-- applied_offset: committed stream progress.
--
-- This table is written inside the same transaction as the data. Never move it
-- to another database, and never commit it separately "for performance" — that
-- change silently reintroduces the possibility of applying a batch twice or
-- skipping one entirely, and nothing in the system will report it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.applied_offset (
    migration_id VARCHAR(128) NOT NULL,
    topic        VARCHAR(255) NOT NULL,
    partition    INTEGER      NOT NULL,
    "offset"     BIGINT       NOT NULL,
    last_lsn     BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (migration_id, topic, partition)
);

-- ---------------------------------------------------------------------------
-- dead_letter: records that could not be applied.
--
-- payload holds the original event bytes so a record can be replayed exactly,
-- not reconstructed from a partial parse. It is encrypted at rest because this
-- table is a durable copy of production data with a longer retention than the
-- pipeline itself.
--
-- row_key_hash, not the key, is what reporting and dashboards use: primary keys
-- in a consumer database are frequently PII, and this table is read by more
-- people than the data is.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.dead_letter (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    migration_id      VARCHAR(128) NOT NULL,
    source_table      VARCHAR(512) NOT NULL,
    op                VARCHAR(8)   NOT NULL,

    row_key_hash      VARCHAR(64)  NOT NULL,
    row_key           TEXT         NOT NULL,

    payload           TEXT,
    payload_encrypted BOOLEAN      NOT NULL DEFAULT FALSE,

    error_class       VARCHAR(16)  NOT NULL,
    last_error        TEXT,
    attempts          INTEGER      NOT NULL DEFAULT 0,
    status            VARCHAR(16)  NOT NULL DEFAULT 'pending',

    first_seen_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    next_retry_at     TIMESTAMPTZ,
    claimed_at        TIMESTAMPTZ,
    claimed_by        VARCHAR(128),
    resolved_at       TIMESTAMPTZ,
    requeued_by       VARCHAR(128),

    source_lsn        BIGINT       NOT NULL DEFAULT 0,
    topic             VARCHAR(255),
    partition         INTEGER      NOT NULL DEFAULT 0,
    "offset"          BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT dead_letter_status_known
        CHECK (status IN ('pending','retrying','quarantined','resolved','discarded'))
);

-- The claim query orders by next_retry_at within status, so this index is what
-- keeps the repair worker's SKIP LOCKED scan from degenerating into a table scan
-- once the resolved rows outnumber the pending ones by a thousand to one.
CREATE INDEX IF NOT EXISTS dead_letter_due_idx
    ON migration_ctl.dead_letter (migration_id, status, next_retry_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS dead_letter_table_idx
    ON migration_ctl.dead_letter (migration_id, source_table, status);

CREATE INDEX IF NOT EXISTS dead_letter_key_idx
    ON migration_ctl.dead_letter (migration_id, row_key_hash);

-- ---------------------------------------------------------------------------
-- part_state: which extracted parts have been loaded.
--
-- Recorded per part rather than per table so that a loader killed halfway
-- through a 400-part table resumes at part 217 instead of starting over.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.part_state (
    migration_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(512) NOT NULL,
    part_index   INTEGER      NOT NULL,
    rows_loaded  BIGINT       NOT NULL DEFAULT 0,
    sha256       VARCHAR(64),
    state        VARCHAR(16)  NOT NULL DEFAULT 'sealed',
    extract_lsn  BIGINT       NOT NULL DEFAULT 0,
    attempts     INTEGER      NOT NULL DEFAULT 0,
    last_error   TEXT,
    sealed_at    TIMESTAMPTZ,
    loaded_at    TIMESTAMPTZ,
    PRIMARY KEY (migration_id, source_table, part_index)
);

CREATE INDEX IF NOT EXISTS part_state_pending_idx
    ON migration_ctl.part_state (migration_id, state);

-- ---------------------------------------------------------------------------
-- chunk_state: snapshot chunk progress for the unified-stream strategy.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.chunk_state (
    migration_id VARCHAR(128) NOT NULL,
    source_table VARCHAR(512) NOT NULL,
    chunk_id     VARCHAR(128) NOT NULL,
    low_key      TEXT,
    high_key     TEXT,
    rows_read    BIGINT       NOT NULL DEFAULT 0,
    rows_emitted BIGINT       NOT NULL DEFAULT 0,
    rows_evicted BIGINT       NOT NULL DEFAULT 0,
    state        VARCHAR(16)  NOT NULL DEFAULT 'pending',
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (migration_id, source_table, chunk_id)
);

-- ---------------------------------------------------------------------------
-- recon_run and recon_finding: verification history.
--
-- History rather than a single current value: "it reconciled clean at 04:12 and
-- had four findings at 05:47" is the shape of question an incident actually
-- asks, and it cannot be answered by a table that only holds the latest result.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.recon_run (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    migration_id   VARCHAR(128) NOT NULL,
    source_table   VARCHAR(512) NOT NULL,
    started_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    findings       INTEGER      NOT NULL DEFAULT 0,
    digest_queries INTEGER      NOT NULL DEFAULT 0,
    row_reads      INTEGER      NOT NULL DEFAULT 0,
    ranges_visited INTEGER      NOT NULL DEFAULT 0,
    deepest        INTEGER      NOT NULL DEFAULT 0,
    complete       BOOLEAN      NOT NULL DEFAULT FALSE,
    mode           VARCHAR(16)  NOT NULL DEFAULT 'continuous'
);

CREATE INDEX IF NOT EXISTS recon_run_recent_idx
    ON migration_ctl.recon_run (migration_id, source_table, started_at DESC);

CREATE TABLE IF NOT EXISTS migration_ctl.recon_finding (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       BIGINT       NOT NULL REFERENCES migration_ctl.recon_run(id) ON DELETE CASCADE,
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
    found_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    repaired_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS recon_finding_open_idx
    ON migration_ctl.recon_finding (migration_id, source_table, kind)
    WHERE repaired_at IS NULL;

-- ---------------------------------------------------------------------------
-- cutover_audit: who cut over, when, and what they were told at the time.
--
-- An override that is not recorded is an override that will be argued about
-- later. Storing the blockers as they stood at the moment of the decision means
-- the record shows what was known then, not what is known now.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS migration_ctl.cutover_audit (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    migration_id  VARCHAR(128) NOT NULL,
    action        VARCHAR(32)  NOT NULL,
    performed_by  VARCHAR(128) NOT NULL,
    performed_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    gate_ready    BOOLEAN      NOT NULL,
    blockers      TEXT,
    override      BOOLEAN      NOT NULL DEFAULT FALSE,
    reason        TEXT
);

CREATE INDEX IF NOT EXISTS cutover_audit_migration_idx
    ON migration_ctl.cutover_audit (migration_id, performed_at DESC);

COMMIT;
