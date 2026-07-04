-- =============================================================
-- Microsoft SQL Server DDL — Finance (subscription billing)
-- =============================================================

CREATE TABLE customers (
    customer_id             BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    external_id             VARCHAR(36)     NOT NULL UNIQUE
                                            DEFAULT CONVERT(VARCHAR(36), NEWID()),
    salesforce_account_id   VARCHAR(18)     UNIQUE,
    name                    VARCHAR(255)    NOT NULL,
    email                   VARCHAR(320),
    phone                   VARCHAR(64),
    country                 CHAR(2),
    default_currency        CHAR(3),
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX ix_customers_name ON customers (name);

CREATE TABLE payment_methods (
    payment_method_id   BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    customer_id         BIGINT          NOT NULL REFERENCES customers (customer_id),
    method_type         VARCHAR(32)     NOT NULL
                                        CHECK (method_type IN ('CARD','ACH','WIRE','SEPA')),
    brand               VARCHAR(32),
    last_four           CHAR(4),
    exp_month           SMALLINT,
    exp_year            SMALLINT,
    billing_country     CHAR(2),
    is_default          BIT             NOT NULL DEFAULT 0,
    is_active           BIT             NOT NULL DEFAULT 1,
    created_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX ix_pm_customer ON payment_methods (customer_id);

CREATE TABLE subscription_plans (
    plan_id                 BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    plan_code               VARCHAR(64)     NOT NULL UNIQUE,
    name                    VARCHAR(255)    NOT NULL,
    description             VARCHAR(MAX),
    billing_interval        VARCHAR(16)     NOT NULL
                                            CHECK (billing_interval IN ('DAY','WEEK','MONTH','YEAR')),
    billing_interval_count  SMALLINT        NOT NULL DEFAULT 1
                                            CHECK (billing_interval_count > 0),
    trial_period_days       SMALLINT        NOT NULL DEFAULT 0
                                            CHECK (trial_period_days >= 0),
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE subscription_plan_prices (
    price_id            BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    plan_id             BIGINT          NOT NULL REFERENCES subscription_plans (plan_id),
    currency            CHAR(3)         NOT NULL,
    amount              DECIMAL(19,4)   NOT NULL CHECK (amount >= 0),
    effective_from      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    effective_to        DATETIME2,
    is_active           BIT             NOT NULL DEFAULT 1,
    created_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT ux_price_plan_ccy_from UNIQUE (plan_id, currency, effective_from)
);

CREATE TABLE customer_subscriptions (
    subscription_id         BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    external_id             VARCHAR(36)     NOT NULL UNIQUE
                                            DEFAULT CONVERT(VARCHAR(36), NEWID()),
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

    started_at              DATETIME2       NOT NULL,
    trial_start             DATETIME2,
    trial_end               DATETIME2,
    current_period_start    DATETIME2       NOT NULL,
    current_period_end      DATETIME2       NOT NULL,
    next_billing_at         DATETIME2,
    cancel_at               DATETIME2,
    canceled_at             DATETIME2,
    ended_at                DATETIME2,
    cancel_at_period_end    BIT             NOT NULL DEFAULT 0,

    payment_method_id       BIGINT          REFERENCES payment_methods (payment_method_id),

    metadata                NVARCHAR(MAX)   NOT NULL DEFAULT N'{}',
    created_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at              DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    version                 INT             NOT NULL DEFAULT 1,

    CHECK (current_period_end > current_period_start),
    CHECK (trial_end IS NULL OR trial_start IS NULL OR trial_end >= trial_start),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- Filtered unique index: at most one live subscription per (customer, plan).
-- (MSSQL UNIQUE treats NULLs as equal, so a generated-key trick won't work;
-- a filtered index gives partial-uniqueness semantics directly.)
CREATE UNIQUE INDEX ux_cust_sub_active ON customer_subscriptions (customer_id, plan_id)
    WHERE status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED');

CREATE INDEX ix_cust_sub_customer     ON customer_subscriptions (customer_id);
CREATE INDEX ix_cust_sub_status       ON customer_subscriptions (status);
CREATE INDEX ix_cust_sub_next_billing ON customer_subscriptions (status, next_billing_at);
CREATE INDEX ix_cust_sub_period_end   ON customer_subscriptions (current_period_end);

CREATE TABLE customer_subscription_events (
    event_id            BIGINT          NOT NULL IDENTITY(1,1) PRIMARY KEY,
    subscription_id     BIGINT          NOT NULL REFERENCES customer_subscriptions (subscription_id),
    event_type          VARCHAR(64)     NOT NULL,
    from_status         VARCHAR(32),
    to_status           VARCHAR(32),
    from_plan_id        BIGINT          REFERENCES subscription_plans (plan_id),
    to_plan_id          BIGINT          REFERENCES subscription_plans (plan_id),
    amount              DECIMAL(19,4),
    currency            CHAR(3),
    occurred_at         DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    actor               VARCHAR(128),
    reason              VARCHAR(MAX),
    metadata            NVARCHAR(MAX)   NOT NULL DEFAULT N'{}'
);
CREATE INDEX ix_sub_event_subscription ON customer_subscription_events (subscription_id, occurred_at);
CREATE INDEX ix_sub_event_type         ON customer_subscription_events (event_type);
