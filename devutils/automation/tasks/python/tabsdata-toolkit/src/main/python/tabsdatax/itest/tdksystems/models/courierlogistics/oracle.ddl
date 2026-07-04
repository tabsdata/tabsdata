-- =============================================================
-- Oracle DDL — Real-World Domain Schemas
-- Domain: Courier Logistics
-- =============================================================

CREATE TABLE cou_hubs (
    hub_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        VARCHAR2(10)    NOT NULL UNIQUE,
    name        VARCHAR2(100)   NOT NULL,
    address     VARCHAR2(300)   NOT NULL,
    city        VARCHAR2(100)   NOT NULL,
    country     CHAR(2)         NOT NULL
);

CREATE TABLE cou_couriers (
    courier_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_no     VARCHAR2(20)    NOT NULL UNIQUE,
    name            VARCHAR2(100)   NOT NULL,
    phone           VARCHAR2(30),
    vehicle_type    VARCHAR2(20)    NOT NULL CHECK (vehicle_type IN ('bicycle','motorbike','van','truck')),
    vehicle_plate   VARCHAR2(20),
    home_hub_id     NUMBER          REFERENCES cou_hubs (hub_id) ON DELETE SET NULL
);

CREATE TABLE cou_customers (
    customer_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(150)   NOT NULL,
    phone           VARCHAR2(30),
    email           VARCHAR2(255),
    default_address VARCHAR2(300),
    account_type    VARCHAR2(20)    DEFAULT 'personal' NOT NULL CHECK (account_type IN ('personal','business'))
);

CREATE TABLE cou_shipments (
    shipment_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tracking_no     VARCHAR2(30)    NOT NULL UNIQUE,
    sender_id       NUMBER          NOT NULL REFERENCES cou_customers (customer_id),
    recipient_name  VARCHAR2(150)   NOT NULL,
    recipient_phone VARCHAR2(30),
    recipient_addr  VARCHAR2(300)   NOT NULL,
    origin_hub_id   NUMBER          NOT NULL REFERENCES cou_hubs (hub_id),
    dest_hub_id     NUMBER          NOT NULL REFERENCES cou_hubs (hub_id),
    weight_kg       NUMBER(8,3)     NOT NULL CHECK (weight_kg > 0),
    dimensions_cm   VARCHAR2(50),
    service_type    VARCHAR2(20)    DEFAULT 'standard' NOT NULL
                    CHECK (service_type IN ('standard','express','overnight','economy')),
    declared_value  NUMBER(10,2),
    status          VARCHAR2(20)    DEFAULT 'picked_up' NOT NULL
                    CHECK (status IN ('picked_up','in_transit','at_hub','out_for_delivery','delivered','failed','returned')),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    estimated_delivery DATE
);

CREATE TABLE cou_tracking_events (
    event_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id     NUMBER          NOT NULL REFERENCES cou_shipments (shipment_id) ON DELETE CASCADE,
    hub_id          NUMBER          REFERENCES cou_hubs     (hub_id)     ON DELETE SET NULL,
    courier_id      NUMBER          REFERENCES cou_couriers (courier_id) ON DELETE SET NULL,
    event_type      VARCHAR2(30)    NOT NULL CHECK (event_type IN ('picked_up','arrived_hub','departed_hub','out_for_delivery','delivery_attempted','delivered','exception','returned')),
    location_desc   VARCHAR2(200),
    notes           CLOB,
    occurred_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
