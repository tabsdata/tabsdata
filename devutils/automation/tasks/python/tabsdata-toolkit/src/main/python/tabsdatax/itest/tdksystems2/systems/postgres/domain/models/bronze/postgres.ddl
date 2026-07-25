-- =============================================================
-- PostgreSQL DDL — Bronze arrival-pattern simulator
--
-- One table per medallion-bronze arrival pattern. The only enforced
-- PK is sim_counter (a monotonic BIGINT) so a consumer can read the
-- next "batch" with:
--    WHERE sim_counter > :last ORDER BY sim_counter LIMIT :size.
-- (tenant_id, entity_id) is the composite "business" key — NOT
-- enforced by the schema, on purpose, so consumers exercise composite
-- dedup / upsert logic. sim_op (CDC only) carries I/U/D. The
-- soft-delete table communicates deletion via the `deleted` flag
-- column (one of the 10 payload columns). Booleans are stored as
-- SMALLINT(0,1) for cross-engine portability.
-- =============================================================

CREATE TABLE brz_full_snapshot (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMPTZ     NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INTEGER,
    score           DOUBLE PRECISION,
    is_active       SMALLINT        NOT NULL CHECK (is_active IN (0,1))
);

CREATE TABLE brz_insup_soft_delete (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMPTZ     NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INTEGER,
    score           DOUBLE PRECISION,
    deleted         SMALLINT        NOT NULL CHECK (deleted IN (0,1))
);

CREATE TABLE brz_insup_no_delete (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMPTZ     NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INTEGER,
    score           DOUBLE PRECISION,
    is_active       SMALLINT        NOT NULL CHECK (is_active IN (0,1))
);

-- @td.cdc.* contract: metadata cols ("@td.cdc.meta.*") describe the
-- operation; data cols are emitted as old + new pairs
-- ("@td.cdc.data.col.old.<COL>" / "@td.cdc.data.col.new.<COL>"). Per
-- spec: Insert → old NULL, new = inserted; Update → old = before, new
-- = after; Delete → old NULL, new = deleted row.
CREATE TABLE brz_cdc_stream (
    sim_counter                          BIGINT      NOT NULL PRIMARY KEY,
    "@td.cdc.meta.op"                    CHAR(1)     NOT NULL CHECK ("@td.cdc.meta.op" IN ('i','u','d')),
    "@td.cdc.meta.tx"                    VARCHAR(36) NOT NULL,
    "@td.cdc.meta.sq"                    INTEGER     NOT NULL,
    "@td.cdc.data.col.old.tenant_id"     VARCHAR(20),
    "@td.cdc.data.col.new.tenant_id"     VARCHAR(20),
    "@td.cdc.data.col.old.entity_id"     BIGINT,
    "@td.cdc.data.col.new.entity_id"     BIGINT,
    "@td.cdc.data.col.old.data_ts"       TIMESTAMPTZ,
    "@td.cdc.data.col.new.data_ts"       TIMESTAMPTZ,
    "@td.cdc.data.col.old.name"          VARCHAR(80),
    "@td.cdc.data.col.new.name"          VARCHAR(80),
    "@td.cdc.data.col.old.status"        VARCHAR(20),
    "@td.cdc.data.col.new.status"        VARCHAR(20),
    "@td.cdc.data.col.old.category"      VARCHAR(20),
    "@td.cdc.data.col.new.category"      VARCHAR(20),
    "@td.cdc.data.col.old.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.new.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.old.quantity"      INTEGER,
    "@td.cdc.data.col.new.quantity"      INTEGER,
    "@td.cdc.data.col.old.score"         DOUBLE PRECISION,
    "@td.cdc.data.col.new.score"         DOUBLE PRECISION,
    "@td.cdc.data.col.old.is_active"     SMALLINT    CHECK ("@td.cdc.data.col.old.is_active" IS NULL OR "@td.cdc.data.col.old.is_active" IN (0,1)),
    "@td.cdc.data.col.new.is_active"     SMALLINT    CHECK ("@td.cdc.data.col.new.is_active" IS NULL OR "@td.cdc.data.col.new.is_active" IN (0,1))
);

CREATE TABLE brz_append_events (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMPTZ     NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INTEGER,
    score           DOUBLE PRECISION,
    is_active       SMALLINT        NOT NULL CHECK (is_active IN (0,1))
);
