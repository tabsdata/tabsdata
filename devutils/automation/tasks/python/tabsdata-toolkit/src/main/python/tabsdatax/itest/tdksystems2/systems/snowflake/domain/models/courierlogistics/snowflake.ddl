-- =============================================================
-- Snowflake SQL DDL — Courier Logistics
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE hubs (
    hub_id          BIGINT          AUTOINCREMENT,
    code            STRING          NOT NULL,
    name            STRING          NOT NULL,
    address         STRING          NOT NULL,
    city            STRING          NOT NULL,
    country         STRING          NOT NULL
);

CREATE TABLE couriers (
    courier_id      BIGINT          AUTOINCREMENT,
    employee_no     STRING          NOT NULL,
    name            STRING          NOT NULL,
    phone           STRING,
    vehicle_type    STRING          NOT NULL,
    vehicle_plate   STRING,
    home_hub_id     INT
);

CREATE TABLE customers (
    customer_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    phone           STRING,
    email           STRING,
    default_address STRING,
    account_type    STRING          NOT NULL
);

CREATE TABLE shipments (
    shipment_id         BIGINT          AUTOINCREMENT,
    tracking_no         STRING          NOT NULL,
    sender_id           INT             NOT NULL,
    recipient_name      STRING          NOT NULL,
    recipient_phone     STRING,
    recipient_addr      STRING          NOT NULL,
    origin_hub_id       INT             NOT NULL,
    dest_hub_id         INT             NOT NULL,
    weight_kg           DECIMAL(8,3)    NOT NULL,
    dimensions_cm       STRING,
    service_type        STRING          NOT NULL,
    declared_value      DECIMAL(10,2),
    status              STRING          NOT NULL,
    created_at          TIMESTAMP       NOT NULL,
    estimated_delivery  DATE
);

CREATE TABLE tracking_events (
    event_id        BIGINT          AUTOINCREMENT,
    shipment_id     INT             NOT NULL,
    hub_id          INT,
    courier_id      INT,
    event_type      STRING          NOT NULL,
    location_desc   STRING,
    notes           STRING,
    occurred_at     TIMESTAMP       NOT NULL
);
