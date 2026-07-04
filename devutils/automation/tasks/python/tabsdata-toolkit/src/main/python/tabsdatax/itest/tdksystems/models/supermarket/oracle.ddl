-- =============================================================
-- Oracle DDL — Real-World Domain Schemas
-- Domain: Supermarket Inventory
-- =============================================================

CREATE TABLE sm_suppliers (
    supplier_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(150)   NOT NULL,
    contact_name    VARCHAR2(100),
    phone           VARCHAR2(30),
    email           VARCHAR2(255),
    address         VARCHAR2(300)
);

CREATE TABLE sm_categories (
    category_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(100)   NOT NULL,
    parent_id       NUMBER          REFERENCES sm_categories (category_id) ON DELETE SET NULL
);

CREATE TABLE sm_products (
    product_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku             VARCHAR2(50)    NOT NULL UNIQUE,
    name            VARCHAR2(255)   NOT NULL,
    brand           VARCHAR2(100),
    category_id     NUMBER          REFERENCES sm_categories (category_id) ON DELETE SET NULL,
    supplier_id     NUMBER          REFERENCES sm_suppliers  (supplier_id) ON DELETE SET NULL,
    unit            VARCHAR2(20)    DEFAULT 'unit' NOT NULL,
    cost_price      NUMBER(10,2)    NOT NULL CHECK (cost_price >= 0),
    sell_price      NUMBER(10,2)    NOT NULL CHECK (sell_price >= 0),
    vat_rate        NUMBER(5,2)     DEFAULT 21.00 NOT NULL,
    is_active       NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_active IN (0,1))
);

CREATE TABLE sm_warehouses (
    warehouse_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(100)   NOT NULL,
    location        VARCHAR2(255)
);

CREATE TABLE sm_stock (
    stock_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      NUMBER          NOT NULL REFERENCES sm_products   (product_id)   ON DELETE CASCADE,
    warehouse_id    NUMBER          NOT NULL REFERENCES sm_warehouses (warehouse_id) ON DELETE CASCADE,
    quantity        NUMBER(10,3)    DEFAULT 0 NOT NULL CHECK (quantity >= 0),
    reorder_level   NUMBER(10,3)    DEFAULT 0 NOT NULL,
    reorder_qty     NUMBER(10,3)    DEFAULT 0 NOT NULL,
    last_updated    TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT uq_sm_stock UNIQUE (product_id, warehouse_id)
);

CREATE TABLE sm_stock_movements (
    movement_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      NUMBER          NOT NULL REFERENCES sm_products   (product_id),
    warehouse_id    NUMBER          NOT NULL REFERENCES sm_warehouses (warehouse_id),
    movement_type   VARCHAR2(20)    NOT NULL CHECK (movement_type IN ('receipt','sale','adjustment','transfer','waste')),
    quantity        NUMBER(10,3)    NOT NULL,
    reference       VARCHAR2(100),
    moved_at        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
