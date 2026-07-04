-- =============================================================
-- Snowflake SQL DDL — Bronze arrival-pattern simulator
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE brz_full_snapshot (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       STRING          NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMP       NOT NULL,
    name            STRING,
    status          STRING,
    category        STRING,
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       BOOLEAN         NOT NULL
);

CREATE TABLE brz_insup_soft_delete (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       STRING          NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMP       NOT NULL,
    name            STRING,
    status          STRING,
    category        STRING,
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    deleted         BOOLEAN         NOT NULL
);

CREATE TABLE brz_insup_no_delete (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       STRING          NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMP       NOT NULL,
    name            STRING,
    status          STRING,
    category        STRING,
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       BOOLEAN         NOT NULL
);

CREATE TABLE brz_cdc_stream (
    sim_counter                          BIGINT      NOT NULL,
    "@td.cdc.meta.op"                    STRING      NOT NULL,
    "@td.cdc.meta.tx"                    STRING      NOT NULL,
    "@td.cdc.meta.sq"                    INT         NOT NULL,
    "@td.cdc.data.col.old.tenant_id"     STRING,
    "@td.cdc.data.col.new.tenant_id"     STRING,
    "@td.cdc.data.col.old.entity_id"     BIGINT,
    "@td.cdc.data.col.new.entity_id"     BIGINT,
    "@td.cdc.data.col.old.data_ts"       TIMESTAMP,
    "@td.cdc.data.col.new.data_ts"       TIMESTAMP,
    "@td.cdc.data.col.old.name"          STRING,
    "@td.cdc.data.col.new.name"          STRING,
    "@td.cdc.data.col.old.status"        STRING,
    "@td.cdc.data.col.new.status"        STRING,
    "@td.cdc.data.col.old.category"      STRING,
    "@td.cdc.data.col.new.category"      STRING,
    "@td.cdc.data.col.old.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.new.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.old.quantity"      INT,
    "@td.cdc.data.col.new.quantity"      INT,
    "@td.cdc.data.col.old.score"         DOUBLE,
    "@td.cdc.data.col.new.score"         DOUBLE,
    "@td.cdc.data.col.old.is_active"     BOOLEAN,
    "@td.cdc.data.col.new.is_active"     BOOLEAN
);

CREATE TABLE brz_append_events (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       STRING          NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         TIMESTAMP       NOT NULL,
    name            STRING,
    status          STRING,
    category        STRING,
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       BOOLEAN         NOT NULL
);
