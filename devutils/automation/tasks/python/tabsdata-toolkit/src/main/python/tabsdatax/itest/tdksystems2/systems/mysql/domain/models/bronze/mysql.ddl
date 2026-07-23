-- =============================================================
-- MySQL DDL — Bronze arrival-pattern simulator
--
-- sim_counter is the only enforced PK. (tenant_id, entity_id) is the
-- composite "business" key — intentionally NOT constrained.
-- =============================================================

CREATE TABLE full_snapshot (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME        NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       TINYINT(1)      NOT NULL,
    PRIMARY KEY (sim_counter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE insup_soft_delete (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME        NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    deleted         TINYINT(1)      NOT NULL,
    PRIMARY KEY (sim_counter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE insup_no_delete (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME        NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       TINYINT(1)      NOT NULL,
    PRIMARY KEY (sim_counter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- @td.cdc.* contract — see postgres.ddl header for the spec. MySQL
-- quotes the special identifiers with backticks.
CREATE TABLE cdc_stream (
    sim_counter                          BIGINT      NOT NULL,
    `@td.cdc.meta.op`                    CHAR(1)     NOT NULL,
    `@td.cdc.meta.tx`                    VARCHAR(36) NOT NULL,
    `@td.cdc.meta.sq`                    INT         NOT NULL,
    `@td.cdc.data.col.old.tenant_id`     VARCHAR(20),
    `@td.cdc.data.col.new.tenant_id`     VARCHAR(20),
    `@td.cdc.data.col.old.entity_id`     BIGINT,
    `@td.cdc.data.col.new.entity_id`     BIGINT,
    `@td.cdc.data.col.old.data_ts`       DATETIME,
    `@td.cdc.data.col.new.data_ts`       DATETIME,
    `@td.cdc.data.col.old.name`          VARCHAR(80),
    `@td.cdc.data.col.new.name`          VARCHAR(80),
    `@td.cdc.data.col.old.status`        VARCHAR(20),
    `@td.cdc.data.col.new.status`        VARCHAR(20),
    `@td.cdc.data.col.old.category`      VARCHAR(20),
    `@td.cdc.data.col.new.category`      VARCHAR(20),
    `@td.cdc.data.col.old.amount`        DECIMAL(12,2),
    `@td.cdc.data.col.new.amount`        DECIMAL(12,2),
    `@td.cdc.data.col.old.quantity`      INT,
    `@td.cdc.data.col.new.quantity`      INT,
    `@td.cdc.data.col.old.score`         DOUBLE,
    `@td.cdc.data.col.new.score`         DOUBLE,
    `@td.cdc.data.col.old.is_active`     TINYINT(1),
    `@td.cdc.data.col.new.is_active`     TINYINT(1),
    PRIMARY KEY (sim_counter),
    CONSTRAINT chk_cdc_op CHECK (`@td.cdc.meta.op` IN ('i','u','d'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE append_events (
    sim_counter     BIGINT          NOT NULL,
    tenant_id       VARCHAR(20)     NOT NULL,
    entity_id       BIGINT          NOT NULL,
    data_ts         DATETIME        NOT NULL,
    name            VARCHAR(80),
    status          VARCHAR(20),
    category        VARCHAR(20),
    amount          DECIMAL(12,2),
    quantity        INT,
    score           DOUBLE,
    is_active       TINYINT(1)      NOT NULL,
    PRIMARY KEY (sim_counter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
