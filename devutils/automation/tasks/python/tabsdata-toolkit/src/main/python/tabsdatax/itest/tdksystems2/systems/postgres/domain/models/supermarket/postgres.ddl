-- =============================================================
-- PostgreSQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 1. SUPERMARKET INVENTORY SYSTEM
-- =============================================================

CREATE TABLE suppliers (
    supplier_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300)
);

CREATE TABLE categories (
    category_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    parent_id       INT             REFERENCES categories (category_id) ON DELETE SET NULL
);

CREATE TABLE products (
    product_id      SERIAL          PRIMARY KEY,
    sku             VARCHAR(50)     NOT NULL UNIQUE,
    name            VARCHAR(255)    NOT NULL,
    brand           VARCHAR(100),
    category_id     INT             REFERENCES categories (category_id) ON DELETE SET NULL,
    supplier_id     INT             REFERENCES suppliers  (supplier_id) ON DELETE SET NULL,
    unit            VARCHAR(20)     NOT NULL DEFAULT 'unit',
    cost_price      NUMERIC(10,2)   NOT NULL CHECK (cost_price >= 0),
    sell_price      NUMERIC(10,2)   NOT NULL CHECK (sell_price >= 0),
    vat_rate        NUMERIC(5,2)    NOT NULL DEFAULT 21.00,
    is_active       SMALLINT        NOT NULL DEFAULT 1 CHECK (is_active IN (0,1))
);

CREATE TABLE warehouses (
    warehouse_id    SERIAL          PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    location        VARCHAR(255)
);

CREATE TABLE stock (
    stock_id        SERIAL          PRIMARY KEY,
    product_id      INT             NOT NULL REFERENCES products   (product_id) ON DELETE CASCADE,
    warehouse_id    INT             NOT NULL REFERENCES warehouses (warehouse_id) ON DELETE CASCADE,
    quantity        NUMERIC(10,3)   NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    reorder_level   NUMERIC(10,3)   NOT NULL DEFAULT 0,
    reorder_qty     NUMERIC(10,3)   NOT NULL DEFAULT 0,
    last_updated    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (product_id, warehouse_id)
);

CREATE TABLE stock_movements (
    movement_id     SERIAL          PRIMARY KEY,
    product_id      INT             NOT NULL REFERENCES products   (product_id),
    warehouse_id    INT             NOT NULL REFERENCES warehouses (warehouse_id),
    movement_type   TEXT            NOT NULL CHECK (movement_type IN ('receipt','sale','adjustment','transfer','waste')),
    quantity        NUMERIC(10,3)   NOT NULL,
    reference       VARCHAR(100),
    moved_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
