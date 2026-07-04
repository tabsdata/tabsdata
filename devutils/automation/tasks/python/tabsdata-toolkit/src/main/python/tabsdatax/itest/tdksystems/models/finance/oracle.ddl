-- =============================================================
-- Oracle DDL — Finance (subscription billing)
-- =============================================================
--
-- Oracle treats '' as NULL (the SQL emitter normalises empty strings
-- to NULL for portability). Booleans are stored as NUMBER(1) with a
-- CHECK IN (0,1) since Oracle has no native BOOLEAN in tables prior
-- to 23c.

CREATE TABLE customers (
    customer_id             NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id             VARCHAR2(36)    DEFAULT SYS_GUID() NOT NULL UNIQUE,
    salesforce_account_id   VARCHAR2(18)    UNIQUE,
    name                    VARCHAR2(255)   NOT NULL,
    email                   VARCHAR2(320),
    phone                   VARCHAR2(64),
    country                 CHAR(2),
    default_currency        CHAR(3),
    is_active               NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_active IN (0,1)),
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX ix_customers_name ON customers (name);

CREATE TABLE payment_methods (
    payment_method_id   NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         NUMBER          NOT NULL REFERENCES customers (customer_id),
    method_type         VARCHAR2(32)    NOT NULL CHECK (method_type IN ('CARD','ACH','WIRE','SEPA')),
    brand               VARCHAR2(32),
    last_four           CHAR(4),
    exp_month           NUMBER(3),
    exp_year            NUMBER(5),
    billing_country     CHAR(2),
    is_default          NUMBER(1)       DEFAULT 0 NOT NULL CHECK (is_default IN (0,1)),
    is_active           NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_active IN (0,1)),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX ix_pm_customer ON payment_methods (customer_id);

CREATE TABLE subscription_plans (
    plan_id                 NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_code               VARCHAR2(64)    NOT NULL UNIQUE,
    name                    VARCHAR2(255)   NOT NULL,
    description             CLOB,
    billing_interval        VARCHAR2(16)    NOT NULL
                                            CHECK (billing_interval IN ('DAY','WEEK','MONTH','YEAR')),
    billing_interval_count  NUMBER(5)       DEFAULT 1 NOT NULL CHECK (billing_interval_count > 0),
    trial_period_days       NUMBER(5)       DEFAULT 0 NOT NULL CHECK (trial_period_days >= 0),
    is_active               NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_active IN (0,1)),
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE subscription_plan_prices (
    price_id            NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_id             NUMBER          NOT NULL REFERENCES subscription_plans (plan_id),
    currency            CHAR(3)         NOT NULL,
    amount              NUMBER(19,4)    NOT NULL CHECK (amount >= 0),
    effective_from      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    effective_to        TIMESTAMP,
    is_active           NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_active IN (0,1)),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ux_price_plan_ccy_from UNIQUE (plan_id, currency, effective_from)
);

CREATE TABLE customer_subscriptions (
    subscription_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id             VARCHAR2(36)    DEFAULT SYS_GUID() NOT NULL UNIQUE,
    customer_id             NUMBER          NOT NULL REFERENCES customers (customer_id),
    plan_id                 NUMBER          NOT NULL REFERENCES subscription_plans (plan_id),
    price_id                NUMBER          NOT NULL REFERENCES subscription_plan_prices (price_id),

    currency                CHAR(3)         NOT NULL,
    unit_amount             NUMBER(19,4)    NOT NULL CHECK (unit_amount >= 0),
    quantity                NUMBER(10)      DEFAULT 1 NOT NULL CHECK (quantity > 0),
    discount_amount         NUMBER(19,4)    DEFAULT 0 NOT NULL CHECK (discount_amount >= 0),
    tax_amount              NUMBER(19,4)    DEFAULT 0 NOT NULL CHECK (tax_amount >= 0),
    total_amount            NUMBER(19,4)    NOT NULL CHECK (total_amount >= 0),

    status                  VARCHAR2(32)    NOT NULL
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
    cancel_at_period_end    NUMBER(1)       DEFAULT 0 NOT NULL CHECK (cancel_at_period_end IN (0,1)),

    payment_method_id       NUMBER          REFERENCES payment_methods (payment_method_id),

    metadata                CLOB            DEFAULT '{}' NOT NULL,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    version                 NUMBER(10)      DEFAULT 1 NOT NULL,

    CHECK (current_period_end > current_period_start),
    CHECK (trial_end IS NULL OR trial_start IS NULL OR trial_end >= trial_start),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- Function-based unique index: at most one live subscription per (customer, plan).
-- Oracle treats NULLs as distinct in unique indexes, so the CASE → NULL trick
-- gives partial-uniqueness semantics directly.
CREATE UNIQUE INDEX ux_cust_sub_active ON customer_subscriptions (
    CASE WHEN status IN ('TRIALING','ACTIVE','PAST_DUE','PAUSED')
         THEN customer_id || '/' || plan_id
    END
);

CREATE INDEX ix_cust_sub_customer     ON customer_subscriptions (customer_id);
CREATE INDEX ix_cust_sub_status       ON customer_subscriptions (status);
CREATE INDEX ix_cust_sub_next_billing ON customer_subscriptions (status, next_billing_at);
CREATE INDEX ix_cust_sub_period_end   ON customer_subscriptions (current_period_end);

CREATE TABLE customer_subscription_events (
    event_id            NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id     NUMBER          NOT NULL REFERENCES customer_subscriptions (subscription_id),
    event_type          VARCHAR2(64)    NOT NULL,
    from_status         VARCHAR2(32),
    to_status           VARCHAR2(32),
    from_plan_id        NUMBER          REFERENCES subscription_plans (plan_id),
    to_plan_id          NUMBER          REFERENCES subscription_plans (plan_id),
    amount              NUMBER(19,4),
    currency            CHAR(3),
    occurred_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
    actor               VARCHAR2(128),
    reason              CLOB,
    metadata            CLOB            DEFAULT '{}' NOT NULL
);
CREATE INDEX ix_sub_event_subscription ON customer_subscription_events (subscription_id, occurred_at);
CREATE INDEX ix_sub_event_type         ON customer_subscription_events (event_type);
