-- =============================================================
-- Microsoft SQL Server DDL — Real-World Domain Schemas
-- Domain: Courier Logistics
-- =============================================================

CREATE TABLE hubs (
    hub_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    code        VARCHAR(10)     NOT NULL UNIQUE,
    name        VARCHAR(100)    NOT NULL,
    address     VARCHAR(300)    NOT NULL,
    city        VARCHAR(100)    NOT NULL,
    country     CHAR(2)         NOT NULL
);

CREATE TABLE couriers (
    courier_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    employee_no     VARCHAR(20)     NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    phone           VARCHAR(30),
    vehicle_type    VARCHAR(20)     NOT NULL CHECK (vehicle_type IN ('bicycle','motorbike','van','truck')),
    vehicle_plate   VARCHAR(20),
    home_hub_id     INT             REFERENCES hubs (hub_id) ON DELETE SET NULL
);

CREATE TABLE customers (
    customer_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    phone           VARCHAR(30),
    email           VARCHAR(255),
    default_address VARCHAR(300),
    account_type    VARCHAR(20)     NOT NULL DEFAULT 'personal' CHECK (account_type IN ('personal','business'))
);

CREATE TABLE shipments (
    shipment_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    tracking_no     VARCHAR(30)     NOT NULL UNIQUE,
    sender_id       INT             NOT NULL REFERENCES customers (customer_id),
    recipient_name  VARCHAR(150)    NOT NULL,
    recipient_phone VARCHAR(30),
    recipient_addr  VARCHAR(300)    NOT NULL,
    origin_hub_id   INT             NOT NULL REFERENCES hubs (hub_id),
    dest_hub_id     INT             NOT NULL REFERENCES hubs (hub_id),
    weight_kg       DECIMAL(8,3)    NOT NULL CHECK (weight_kg > 0),
    dimensions_cm   VARCHAR(50),
    service_type    VARCHAR(20)     NOT NULL DEFAULT 'standard'
                    CHECK (service_type IN ('standard','express','overnight','economy')),
    declared_value  DECIMAL(10,2),
    status          VARCHAR(20)     NOT NULL DEFAULT 'picked_up'
                    CHECK (status IN ('picked_up','in_transit','at_hub','out_for_delivery','delivered','failed','returned')),
    created_at      DATETIMEOFFSET  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    estimated_delivery DATE
);

CREATE TABLE tracking_events (
    event_id        INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    shipment_id     INT             NOT NULL REFERENCES shipments (shipment_id) ON DELETE CASCADE,
    hub_id          INT             REFERENCES hubs     (hub_id)     ON DELETE SET NULL,
    courier_id      INT             REFERENCES couriers (courier_id) ON DELETE SET NULL,
    event_type      VARCHAR(30)     NOT NULL CHECK (event_type IN ('picked_up','arrived_hub','departed_hub','out_for_delivery','delivery_attempted','delivered','exception','returned')),
    location_desc   VARCHAR(200),
    notes           NVARCHAR(MAX),
    occurred_at     DATETIMEOFFSET  NOT NULL DEFAULT SYSDATETIMEOFFSET()
);
