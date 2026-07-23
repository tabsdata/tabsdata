-- Finance DB schema (MySQL 8.0+)
--
-- Complementary to Salesforce: `customers.salesforce_account_id` joins
-- finance subscriptions to SF Account records (and, transitively, to
-- Opportunity pipeline) for forecasting.

-- Customers billed by the finance system. Linked to Salesforce Account
-- via salesforce_account_id (15- or 18-char Salesforce Id).
CREATE TABLE customers (
    customer_id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    external_id             CHAR(36) NOT NULL UNIQUE DEFAULT (UUID()),
    salesforce_account_id   VARCHAR(18) UNIQUE,
    name                    VARCHAR(255) NOT NULL,
    email                   VARCHAR(320),
    phone                   VARCHAR(64),
    country                 CHAR(2),                   -- ISO 3166-1 alpha-2
    default_currency        CHAR(3),                   -- ISO 4217
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX ix_customers_name ON customers (name);

-- Payment methods on file for a customer
CREATE TABLE payment_methods (
    payment_method_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id         BIGINT NOT NULL,
    method_type         VARCHAR(32) NOT NULL,           -- 'CARD','ACH','WIRE','SEPA'
    brand               VARCHAR(32),                    -- 'VISA','MASTERCARD', etc.
    last_four           CHAR(4),
    exp_month           TINYINT UNSIGNED,
    exp_year            SMALLINT UNSIGNED,
    billing_country     CHAR(2),
    is_default          BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_pm_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT ck_pm_method_type
        CHECK (method_type IN ('CARD','ACH','WIRE','SEPA'))
);

CREATE INDEX ix_pm_customer ON payment_methods (customer_id);

-- Products/plans that customers can subscribe to
CREATE TABLE subscription_plans (
    plan_id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    plan_code               VARCHAR(64) NOT NULL UNIQUE,
    name                    VARCHAR(255) NOT NULL,
    description             TEXT,
    billing_interval        VARCHAR(16) NOT NULL,       -- 'DAY','WEEK','MONTH','YEAR'
    billing_interval_count  SMALLINT NOT NULL DEFAULT 1,
    trial_period_days       SMALLINT NOT NULL DEFAULT 0,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT ck_plan_interval
        CHECK (billing_interval IN ('DAY','WEEK','MONTH','YEAR')),
    CONSTRAINT ck_plan_interval_count CHECK (billing_interval_count > 0),
    CONSTRAINT ck_plan_trial_days     CHECK (trial_period_days >= 0)
);

-- Plan pricing, separated to support multi-currency and price history
CREATE TABLE subscription_plan_prices (
    price_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    plan_id             BIGINT NOT NULL,
    currency            CHAR(3) NOT NULL,               -- ISO 4217
    amount              DECIMAL(19,4) NOT NULL,
    effective_from      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to        TIMESTAMP NULL,                 -- NULL = current price
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_price_plan
        FOREIGN KEY (plan_id) REFERENCES subscription_plans(plan_id),
    CONSTRAINT ck_price_amount CHECK (amount >= 0),
    UNIQUE KEY ux_price_plan_ccy_from (plan_id, currency, effective_from)
);

-- The current subscription state for each customer
CREATE TABLE customer_subscriptions (
    subscription_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    external_id             CHAR(36) NOT NULL UNIQUE DEFAULT (UUID()),
    customer_id             BIGINT NOT NULL,
    plan_id                 BIGINT NOT NULL,
    price_id                BIGINT NOT NULL,

    -- Amounts (denormalized snapshot for point-in-time accuracy)
    currency                CHAR(3) NOT NULL,
    unit_amount             DECIMAL(19,4) NOT NULL,
    quantity                INT NOT NULL DEFAULT 1,
    discount_amount         DECIMAL(19,4) NOT NULL DEFAULT 0,
    tax_amount              DECIMAL(19,4) NOT NULL DEFAULT 0,
    total_amount            DECIMAL(19,4) NOT NULL,

    -- Status lifecycle:
    -- 'TRIALING','ACTIVE','PAST_DUE','PAUSED','CANCELED','EXPIRED','INCOMPLETE'
    status                  VARCHAR(32) NOT NULL,

    -- Validity / billing period
    started_at              TIMESTAMP NOT NULL,
    trial_start             TIMESTAMP NULL,
    trial_end               TIMESTAMP NULL,
    current_period_start    TIMESTAMP NOT NULL,
    current_period_end      TIMESTAMP NOT NULL,
    next_billing_at         TIMESTAMP NULL,
    cancel_at               TIMESTAMP NULL,             -- scheduled cancellation
    canceled_at             TIMESTAMP NULL,             -- when cancellation took effect
    ended_at                TIMESTAMP NULL,             -- when subscription actually terminated
    cancel_at_period_end    BOOLEAN NOT NULL DEFAULT FALSE,

    -- Payment linkage
    payment_method_id       BIGINT NULL,

    -- Metadata
    metadata                JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    version                 INT NOT NULL DEFAULT 1,     -- optimistic locking

    -- Emulates Postgres partial unique index: one active subscription
    -- per (customer, plan). NULL when not active => multiple NULLs allowed.
    active_key              VARCHAR(64)
        GENERATED ALWAYS AS (
            CASE WHEN status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED')
                 THEN CONCAT(customer_id, ':', plan_id)
            END
        ) STORED,

    CONSTRAINT fk_sub_customer
        FOREIGN KEY (customer_id)       REFERENCES customers(customer_id),
    CONSTRAINT fk_sub_plan
        FOREIGN KEY (plan_id)           REFERENCES subscription_plans(plan_id),
    CONSTRAINT fk_sub_price
        FOREIGN KEY (price_id)          REFERENCES subscription_plan_prices(price_id),
    CONSTRAINT fk_sub_payment_method
        FOREIGN KEY (payment_method_id) REFERENCES payment_methods(payment_method_id),

    CONSTRAINT ck_sub_unit_amount     CHECK (unit_amount >= 0),
    CONSTRAINT ck_sub_quantity        CHECK (quantity > 0),
    CONSTRAINT ck_sub_discount        CHECK (discount_amount >= 0),
    CONSTRAINT ck_sub_tax             CHECK (tax_amount >= 0),
    CONSTRAINT ck_sub_total           CHECK (total_amount >= 0),
    CONSTRAINT ck_sub_status
        CHECK (status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED',
                          'CANCELED','EXPIRED','INCOMPLETE')),
    CONSTRAINT ck_sub_period          CHECK (current_period_end > current_period_start),
    CONSTRAINT ck_sub_trial           CHECK (trial_end IS NULL OR trial_start IS NULL OR trial_end >= trial_start),
    CONSTRAINT ck_sub_ended           CHECK (ended_at IS NULL OR ended_at >= started_at),

    UNIQUE KEY ux_cust_sub_active (active_key)
);

CREATE INDEX ix_cust_sub_customer     ON customer_subscriptions (customer_id);
CREATE INDEX ix_cust_sub_status       ON customer_subscriptions (status);
CREATE INDEX ix_cust_sub_next_billing ON customer_subscriptions (status, next_billing_at);
CREATE INDEX ix_cust_sub_period_end   ON customer_subscriptions (current_period_end);

-- Full history of state changes (immutable, append-only)
CREATE TABLE customer_subscription_events (
    event_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    subscription_id     BIGINT NOT NULL,
    -- 'CREATED','ACTIVATED','RENEWED','UPGRADED','DOWNGRADED',
    -- 'PAUSED','RESUMED','CANCEL_SCHEDULED','CANCELED','EXPIRED','PAYMENT_FAILED'
    event_type          VARCHAR(64) NOT NULL,
    from_status         VARCHAR(32),
    to_status           VARCHAR(32),
    from_plan_id        BIGINT,
    to_plan_id          BIGINT,
    amount              DECIMAL(19,4),
    currency            CHAR(3),
    occurred_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor               VARCHAR(128),                   -- user id, 'system', 'webhook:stripe'
    reason              TEXT,
    metadata            JSON NOT NULL DEFAULT (JSON_OBJECT()),

    CONSTRAINT fk_evt_subscription
        FOREIGN KEY (subscription_id) REFERENCES customer_subscriptions(subscription_id),
    CONSTRAINT fk_evt_from_plan
        FOREIGN KEY (from_plan_id)    REFERENCES subscription_plans(plan_id),
    CONSTRAINT fk_evt_to_plan
        FOREIGN KEY (to_plan_id)      REFERENCES subscription_plans(plan_id)
);

CREATE INDEX ix_sub_event_subscription ON customer_subscription_events (subscription_id, occurred_at);
CREATE INDEX ix_sub_event_type         ON customer_subscription_events (event_type);
