-- =============================================================
-- Databricks SQL DDL — Courier Logistics
-- Targets a Unity-Catalog schema; tables default to Delta.
-- PK/FK constraints are informational on Databricks; IDENTITY
-- columns kept, no CREATE INDEX.
-- =============================================================

CREATE TABLE cou_hubs (
    hub_id          BIGINT          GENERATED ALWAYS AS IDENTITY,
    code            STRING          NOT NULL,
    name            STRING          NOT NULL,
    address         STRING          NOT NULL,
    city            STRING          NOT NULL,
    country         STRING          NOT NULL
);

CREATE TABLE cou_couriers (
    courier_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    employee_no     STRING          NOT NULL,
    name            STRING          NOT NULL,
    phone           STRING,
    vehicle_type    STRING          NOT NULL,
    vehicle_plate   STRING,
    home_hub_id     INT
);

CREATE TABLE cou_customers (
    customer_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    name            STRING          NOT NULL,
    phone           STRING,
    email           STRING,
    default_address STRING,
    account_type    STRING          NOT NULL
);

CREATE TABLE cou_shipments (
    shipment_id         BIGINT          GENERATED ALWAYS AS IDENTITY,
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

CREATE TABLE cou_tracking_events (
    event_id        BIGINT          GENERATED ALWAYS AS IDENTITY,
    shipment_id     INT             NOT NULL,
    hub_id          INT,
    courier_id      INT,
    event_type      STRING          NOT NULL,
    location_desc   STRING,
    notes           STRING,
    occurred_at     TIMESTAMP       NOT NULL
);
