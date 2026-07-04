-- =============================================================
-- MySQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 5. COURIER LOGISTICS SYSTEM
-- =============================================================

CREATE TABLE cou_hubs (
    hub_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    code        VARCHAR(10)     NOT NULL,
    name        VARCHAR(100)    NOT NULL,
    address     VARCHAR(300)    NOT NULL,
    city        VARCHAR(100)    NOT NULL,
    country     CHAR(2)         NOT NULL,   -- ISO 3166-1 alpha-2
    PRIMARY KEY (hub_id),
    UNIQUE KEY uq_cou_hub_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE cou_couriers (
    courier_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    employee_no     VARCHAR(20)     NOT NULL,
    name            VARCHAR(100)    NOT NULL,
    phone           VARCHAR(30),
    vehicle_type    ENUM('bicycle','motorbike','van','truck') NOT NULL,
    vehicle_plate   VARCHAR(20),
    home_hub_id     INT UNSIGNED,
    PRIMARY KEY (courier_id),
    UNIQUE KEY uq_cou_empno (employee_no),
    CONSTRAINT fk_coucour_hub FOREIGN KEY (home_hub_id) REFERENCES cou_hubs (hub_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE cou_customers (
    customer_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    phone           VARCHAR(30),
    email           VARCHAR(255),
    default_address VARCHAR(300),
    account_type    ENUM('personal','business') NOT NULL DEFAULT 'personal',
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE cou_shipments (
    shipment_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    tracking_no     VARCHAR(30)     NOT NULL,
    sender_id       INT UNSIGNED    NOT NULL,
    recipient_name  VARCHAR(150)    NOT NULL,
    recipient_phone VARCHAR(30),
    recipient_addr  VARCHAR(300)    NOT NULL,
    origin_hub_id   INT UNSIGNED    NOT NULL,
    dest_hub_id     INT UNSIGNED    NOT NULL,
    weight_kg       DECIMAL(8,3)    NOT NULL,
    dimensions_cm   VARCHAR(50),    -- LxWxH e.g. '30x20x15'
    service_type    ENUM('standard','express','overnight','economy') NOT NULL DEFAULT 'standard',
    declared_value  DECIMAL(10,2),
    status          ENUM('picked_up','in_transit','at_hub','out_for_delivery','delivered','failed','returned') NOT NULL DEFAULT 'picked_up',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    estimated_delivery DATE,
    PRIMARY KEY (shipment_id),
    UNIQUE KEY uq_cou_tracking (tracking_no),
    CONSTRAINT fk_coushi_sender FOREIGN KEY (sender_id)    REFERENCES cou_customers (customer_id),
    CONSTRAINT fk_coushi_ohub   FOREIGN KEY (origin_hub_id) REFERENCES cou_hubs     (hub_id),
    CONSTRAINT fk_coushi_dhub   FOREIGN KEY (dest_hub_id)   REFERENCES cou_hubs     (hub_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE cou_tracking_events (
    event_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    shipment_id     INT UNSIGNED    NOT NULL,
    hub_id          INT UNSIGNED,
    courier_id      INT UNSIGNED,
    event_type      ENUM('picked_up','arrived_hub','departed_hub','out_for_delivery','delivery_attempted','delivered','exception','returned') NOT NULL,
    location_desc   VARCHAR(200),
    notes           TEXT,
    occurred_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id),
    CONSTRAINT fk_couevt_shi FOREIGN KEY (shipment_id) REFERENCES cou_shipments (shipment_id) ON DELETE CASCADE,
    CONSTRAINT fk_couevt_hub FOREIGN KEY (hub_id)      REFERENCES cou_hubs      (hub_id)      ON DELETE SET NULL,
    CONSTRAINT fk_couevt_cur FOREIGN KEY (courier_id)  REFERENCES cou_couriers  (courier_id)  ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
