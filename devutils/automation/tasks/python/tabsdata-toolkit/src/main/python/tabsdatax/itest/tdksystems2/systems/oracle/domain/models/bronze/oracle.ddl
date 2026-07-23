-- =============================================================
-- Oracle DDL — Bronze arrival-pattern simulator
--
-- sim_counter is the only enforced PK. (tenant_id, entity_id) is the
-- composite "business" key — intentionally NOT constrained. Booleans
-- stored as NUMBER(1) with CHECK IN (0,1) (Oracle pre-23ai has no
-- native BOOLEAN in tables).
-- =============================================================

CREATE TABLE full_snapshot (
    sim_counter     NUMBER(19)               NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR2(20)             NOT NULL,
    entity_id       NUMBER(19)               NOT NULL,
    data_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
    name            VARCHAR2(80),
    status          VARCHAR2(20),
    category        VARCHAR2(20),
    amount          NUMBER(12,2),
    quantity        NUMBER(10),
    score           BINARY_DOUBLE,
    is_active       NUMBER(1)                NOT NULL CHECK (is_active IN (0,1))
);

CREATE TABLE insup_soft_delete (
    sim_counter     NUMBER(19)               NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR2(20)             NOT NULL,
    entity_id       NUMBER(19)               NOT NULL,
    data_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
    name            VARCHAR2(80),
    status          VARCHAR2(20),
    category        VARCHAR2(20),
    amount          NUMBER(12,2),
    quantity        NUMBER(10),
    score           BINARY_DOUBLE,
    deleted         NUMBER(1)                NOT NULL CHECK (deleted IN (0,1))
);

CREATE TABLE insup_no_delete (
    sim_counter     NUMBER(19)               NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR2(20)             NOT NULL,
    entity_id       NUMBER(19)               NOT NULL,
    data_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
    name            VARCHAR2(80),
    status          VARCHAR2(20),
    category        VARCHAR2(20),
    amount          NUMBER(12,2),
    quantity        NUMBER(10),
    score           BINARY_DOUBLE,
    is_active       NUMBER(1)                NOT NULL CHECK (is_active IN (0,1))
);

-- @td.cdc.* contract — see postgres.ddl header for the spec. Booleans
-- (the is_active old/new pair) stay NUMBER(1) with NULL allowed.
CREATE TABLE cdc_stream (
    sim_counter                          NUMBER(19)               NOT NULL PRIMARY KEY,
    "@td.cdc.meta.op"                    CHAR(1)                  NOT NULL CHECK ("@td.cdc.meta.op" IN ('i','u','d')),
    "@td.cdc.meta.tx"                    VARCHAR2(36)             NOT NULL,
    "@td.cdc.meta.sq"                    NUMBER(10)               NOT NULL,
    "@td.cdc.data.col.old.tenant_id"     VARCHAR2(20),
    "@td.cdc.data.col.new.tenant_id"     VARCHAR2(20),
    "@td.cdc.data.col.old.entity_id"     NUMBER(19),
    "@td.cdc.data.col.new.entity_id"     NUMBER(19),
    "@td.cdc.data.col.old.data_ts"       TIMESTAMP WITH TIME ZONE,
    "@td.cdc.data.col.new.data_ts"       TIMESTAMP WITH TIME ZONE,
    "@td.cdc.data.col.old.name"          VARCHAR2(80),
    "@td.cdc.data.col.new.name"          VARCHAR2(80),
    "@td.cdc.data.col.old.status"        VARCHAR2(20),
    "@td.cdc.data.col.new.status"        VARCHAR2(20),
    "@td.cdc.data.col.old.category"      VARCHAR2(20),
    "@td.cdc.data.col.new.category"      VARCHAR2(20),
    "@td.cdc.data.col.old.amount"        NUMBER(12,2),
    "@td.cdc.data.col.new.amount"        NUMBER(12,2),
    "@td.cdc.data.col.old.quantity"      NUMBER(10),
    "@td.cdc.data.col.new.quantity"      NUMBER(10),
    "@td.cdc.data.col.old.score"         BINARY_DOUBLE,
    "@td.cdc.data.col.new.score"         BINARY_DOUBLE,
    "@td.cdc.data.col.old.is_active"     NUMBER(1)                CHECK ("@td.cdc.data.col.old.is_active" IS NULL OR "@td.cdc.data.col.old.is_active" IN (0,1)),
    "@td.cdc.data.col.new.is_active"     NUMBER(1)                CHECK ("@td.cdc.data.col.new.is_active" IS NULL OR "@td.cdc.data.col.new.is_active" IN (0,1))
);

CREATE TABLE append_events (
    sim_counter     NUMBER(19)               NOT NULL PRIMARY KEY,
    tenant_id       VARCHAR2(20)             NOT NULL,
    entity_id       NUMBER(19)               NOT NULL,
    data_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
    name            VARCHAR2(80),
    status          VARCHAR2(20),
    category        VARCHAR2(20),
    amount          NUMBER(12,2),
    quantity        NUMBER(10),
    score           BINARY_DOUBLE,
    is_active       NUMBER(1)                NOT NULL CHECK (is_active IN (0,1))
);
