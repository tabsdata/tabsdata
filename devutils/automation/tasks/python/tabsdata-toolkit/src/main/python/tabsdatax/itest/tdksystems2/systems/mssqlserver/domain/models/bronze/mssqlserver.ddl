-- =============================================================
-- Microsoft SQL Server DDL — Bronze arrival-pattern simulator
--
-- sim_counter is the only enforced PK. (tenant_id, entity_id) is the
-- composite "business" key — intentionally NOT constrained.
-- =============================================================

CREATE TABLE brz_full_snapshot (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME2       NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           FLOAT,
    is_active       BIT             NOT NULL
);

CREATE TABLE brz_insup_soft_delete (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME2       NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           FLOAT,
    deleted         BIT             NOT NULL
);

CREATE TABLE brz_insup_no_delete (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME2       NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           FLOAT,
    is_active       BIT             NOT NULL
);

-- @td.cdc.* contract — see postgres.ddl header for the spec.
CREATE TABLE brz_cdc_stream (
    sim_counter                          BIGINT      NOT NULL PRIMARY KEY,
    "@td.cdc.meta.op"                    CHAR(1)     NOT NULL CHECK ("@td.cdc.meta.op" IN ('i','u','d')),
    "@td.cdc.meta.tx"                    VARCHAR(36) NOT NULL,
    "@td.cdc.meta.sq"                    INT         NOT NULL,
    "@td.cdc.data.col.old.tenant_id"     VARCHAR(20),
    "@td.cdc.data.col.new.tenant_id"     VARCHAR(20),
    "@td.cdc.data.col.old.entity_id"     BIGINT,
    "@td.cdc.data.col.new.entity_id"     BIGINT,
    "@td.cdc.data.col.old.data_ts"       DATETIME2,
    "@td.cdc.data.col.new.data_ts"       DATETIME2,
    "@td.cdc.data.col.old.name"          VARCHAR(80),
    "@td.cdc.data.col.new.name"          VARCHAR(80),
    "@td.cdc.data.col.old.status"        VARCHAR(20),
    "@td.cdc.data.col.new.status"        VARCHAR(20),
    "@td.cdc.data.col.old.category"      VARCHAR(20),
    "@td.cdc.data.col.new.category"      VARCHAR(20),
    "@td.cdc.data.col.old.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.new.amount"        DECIMAL(12,2),
    "@td.cdc.data.col.old.quantity"      INT,
    "@td.cdc.data.col.new.quantity"      INT,
    "@td.cdc.data.col.old.score"         FLOAT,
    "@td.cdc.data.col.new.score"         FLOAT,
    "@td.cdc.data.col.old.is_active"     BIT,
    "@td.cdc.data.col.new.is_active"     BIT
);

CREATE TABLE brz_append_events (
    sim_counter     BIGINT          NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME2       NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           FLOAT,
    is_active       BIT             NOT NULL
);
