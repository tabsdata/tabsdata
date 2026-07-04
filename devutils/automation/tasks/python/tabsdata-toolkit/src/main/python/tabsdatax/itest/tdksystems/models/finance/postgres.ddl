-- =============================================================
-- PostgreSQL DDL — Finance (subscription billing)
-- =============================================================
--
-- Complementary to Salesforce: customers.salesforce_account_id joins
-- finance subscriptions to SF Account records.

CREATE TABLE customers (
    customer_id             BIGSERIAL       PRIMARY KEY,
    external_id             VARCHAR(36)     NOT NULL UNIQUE
                                            DEFAULT gen_random_uuid()::text,
    salesforce_account_id   VARCHAR(18)     UNIQUE,
    name                    VARCHAR(255)    NOT NULL,
    email                   VARCHAR(320),
    phone                   VARCHAR(64),
    country                 CHAR(2),
    default_currency        CHAR(3),
    is_active               SMALLINT        NOT NULL DEFAULT 1
                                            CHECK (is_active IN (0,1)),
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ix_customers_name ON customers (name);

CREATE TABLE payment_methods (
    payment_method_id   BIGSERIAL       PRIMARY KEY,
    customer_id         BIGINT          NOT NULL REFERENCES customers (customer_id),
    method_type         VARCHAR(32)     NOT NULL
                                        CHECK (method_type IN ('CARD','ACH','WIRE','SEPA')),
    brand               VARCHAR(32),
    last_four           CHAR(4),
    exp_month           SMALLINT,
    exp_year            SMALLINT,
    billing_country     CHAR(2),
    is_default          SMALLINT        NOT NULL DEFAULT 0
                                        CHECK (is_default IN (0,1)),
    is_active           SMALLINT        NOT NULL DEFAULT 1
                                        CHECK (is_active IN (0,1)),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ix_pm_customer ON payment_methods (customer_id);

CREATE TABLE subscription_plans (
    plan_id                 BIGSERIAL       PRIMARY KEY,
    plan_code               VARCHAR(64)     NOT NULL UNIQUE,
    name                    VARCHAR(255)    NOT NULL,
    description             TEXT,
    billing_interval        VARCHAR(16)     NOT NULL
                                            CHECK (billing_interval IN ('DAY','WEEK','MONTH','YEAR')),
    billing_interval_count  SMALLINT        NOT NULL DEFAULT 1
                                            CHECK (billing_interval_count > 0),
    trial_period_days       SMALLINT        NOT NULL DEFAULT 0
                                            CHECK (trial_period_days >= 0),
    is_active               SMALLINT        NOT NULL DEFAULT 1
                                            CHECK (is_active IN (0,1)),
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE subscription_plan_prices (
    price_id            BIGSERIAL       PRIMARY KEY,
    plan_id             BIGINT          NOT NULL REFERENCES subscription_plans (plan_id),
    currency            CHAR(3)         NOT NULL,
    amount              DECIMAL(19,4)   NOT NULL CHECK (amount >= 0),
    effective_from      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to        TIMESTAMP,
    is_active           SMALLINT        NOT NULL DEFAULT 1
                                        CHECK (is_active IN (0,1)),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (plan_id, currency, effective_from)
);

CREATE TABLE customer_subscriptions (
    subscription_id         BIGSERIAL       PRIMARY KEY,
    external_id             VARCHAR(36)     NOT NULL UNIQUE
                                            DEFAULT gen_random_uuid()::text,
    customer_id             BIGINT          NOT NULL REFERENCES customers (customer_id),
    plan_id                 BIGINT          NOT NULL REFERENCES subscription_plans (plan_id),
    price_id                BIGINT          NOT NULL REFERENCES subscription_plan_prices (price_id),

    currency                CHAR(3)         NOT NULL,
    unit_amount             DECIMAL(19,4)   NOT NULL CHECK (unit_amount >= 0),
    quantity                INT             NOT NULL DEFAULT 1 CHECK (quantity > 0),
    discount_amount         DECIMAL(19,4)   NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    tax_amount              DECIMAL(19,4)   NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    total_amount            DECIMAL(19,4)   NOT NULL CHECK (total_amount >= 0),

    status                  VARCHAR(32)     NOT NULL
                                            CHECK (status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED',
                                                              'CANCELED','EXPIRED','INCOMPLETE')),

    started_at              TIMESTAMP       NOT NULL,
    trial_start             TIMESTAMP,
    trial_end               TIMESTAMP,
    current_period_start    TIMESTAMP       NOT NULL,
    current_period_end      TIMESTAMP       NOT NULL,
    next_billing_at         TIMESTAMP,
    cancel_at               TIMESTAMP,
    canceled_at             TIMESTAMP,
    ended_at                TIMESTAMP,
    cancel_at_period_end    SMALLINT        NOT NULL DEFAULT 0
                                            CHECK (cancel_at_period_end IN (0,1)),

    payment_method_id       BIGINT          REFERENCES payment_methods (payment_method_id),

    metadata                JSONB           NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version                 INT             NOT NULL DEFAULT 1,

    CHECK (current_period_end > current_period_start),
    CHECK (trial_end IS NULL OR trial_start IS NULL OR trial_end >= trial_start),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- Partial unique index: at most one live subscription per (customer, plan).
CREATE UNIQUE INDEX ux_cust_sub_active ON customer_subscriptions (customer_id, plan_id)
    WHERE status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED');

CREATE INDEX ix_cust_sub_customer     ON customer_subscriptions (customer_id);
CREATE INDEX ix_cust_sub_status       ON customer_subscriptions (status);
CREATE INDEX ix_cust_sub_next_billing ON customer_subscriptions (status, next_billing_at);
CREATE INDEX ix_cust_sub_period_end   ON customer_subscriptions (current_period_end);

CREATE TABLE customer_subscription_events (
    event_id            BIGSERIAL       PRIMARY KEY,
    subscription_id     BIGINT          NOT NULL REFERENCES customer_subscriptions (subscription_id),
    event_type          VARCHAR(64)     NOT NULL,
    from_status         VARCHAR(32),
    to_status           VARCHAR(32),
    from_plan_id        BIGINT          REFERENCES subscription_plans (plan_id),
    to_plan_id          BIGINT          REFERENCES subscription_plans (plan_id),
    amount              DECIMAL(19,4),
    currency            CHAR(3),
    occurred_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor               VARCHAR(128),
    reason              TEXT,
    metadata            JSONB           NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ix_sub_event_subscription ON customer_subscription_events (subscription_id, occurred_at);
CREATE INDEX ix_sub_event_type         ON customer_subscription_events (event_type);
