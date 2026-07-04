-- =============================================================
-- Snowflake SQL DDL — Finance
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE customers (
    customer_id             BIGINT          AUTOINCREMENT,
    external_id             STRING          NOT NULL,
    salesforce_account_id   STRING,
    name                    STRING          NOT NULL,
    email                   STRING,
    phone                   STRING,
    country                 STRING,
    default_currency        STRING,
    is_active               BOOLEAN         NOT NULL,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL
);

CREATE TABLE payment_methods (
    payment_method_id   BIGINT          AUTOINCREMENT,
    customer_id         BIGINT          NOT NULL,
    method_type         STRING          NOT NULL,
    brand               STRING,
    last_four           STRING,
    exp_month           TINYINT,
    exp_year            SMALLINT,
    billing_country     STRING,
    is_default          BOOLEAN         NOT NULL,
    is_active           BOOLEAN         NOT NULL,
    created_at          TIMESTAMP       NOT NULL,
    updated_at          TIMESTAMP       NOT NULL
);

CREATE TABLE subscription_plans (
    plan_id                 BIGINT          AUTOINCREMENT,
    plan_code               STRING          NOT NULL,
    name                    STRING          NOT NULL,
    description             STRING,
    billing_interval        STRING          NOT NULL,
    billing_interval_count  SMALLINT        NOT NULL,
    trial_period_days       SMALLINT        NOT NULL,
    is_active               BOOLEAN         NOT NULL,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL
);

CREATE TABLE subscription_plan_prices (
    price_id            BIGINT          AUTOINCREMENT,
    plan_id             BIGINT          NOT NULL,
    currency            STRING          NOT NULL,
    amount              DECIMAL(19,4)   NOT NULL,
    effective_from      TIMESTAMP       NOT NULL,
    effective_to        TIMESTAMP,
    is_active           BOOLEAN         NOT NULL,
    created_at          TIMESTAMP       NOT NULL
);

CREATE TABLE customer_subscriptions (
    subscription_id         BIGINT          AUTOINCREMENT,
    external_id             STRING          NOT NULL,
    customer_id             BIGINT          NOT NULL,
    plan_id                 BIGINT          NOT NULL,
    price_id                BIGINT          NOT NULL,
    currency                STRING          NOT NULL,
    unit_amount             DECIMAL(19,4)   NOT NULL,
    quantity                INT             NOT NULL,
    discount_amount         DECIMAL(19,4)   NOT NULL,
    tax_amount              DECIMAL(19,4)   NOT NULL,
    total_amount            DECIMAL(19,4)   NOT NULL,
    status                  STRING          NOT NULL,
    started_at              TIMESTAMP       NOT NULL,
    trial_start             TIMESTAMP,
    trial_end               TIMESTAMP,
    current_period_start    TIMESTAMP       NOT NULL,
    current_period_end      TIMESTAMP       NOT NULL,
    next_billing_at         TIMESTAMP,
    cancel_at               TIMESTAMP,
    canceled_at             TIMESTAMP,
    ended_at                TIMESTAMP,
    cancel_at_period_end    BOOLEAN         NOT NULL,
    payment_method_id       BIGINT,
    metadata                STRING          NOT NULL,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    version                 INT             NOT NULL,
    active_key              STRING
);

CREATE TABLE customer_subscription_events (
    event_id            BIGINT          AUTOINCREMENT,
    subscription_id     BIGINT          NOT NULL,
    event_type          STRING          NOT NULL,
    from_status         STRING,
    to_status           STRING,
    from_plan_id        BIGINT,
    to_plan_id          BIGINT,
    amount              DECIMAL(19,4),
    currency            STRING,
    occurred_at         TIMESTAMP       NOT NULL,
    actor               STRING,
    reason              STRING,
    metadata            STRING          NOT NULL
);
