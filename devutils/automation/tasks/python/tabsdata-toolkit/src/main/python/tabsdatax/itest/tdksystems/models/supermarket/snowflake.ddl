-- =============================================================
-- Snowflake SQL DDL — Supermarket Inventory
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE sm_suppliers (
    supplier_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    contact_name    STRING,
    phone           STRING,
    email           STRING,
    address         STRING
);

CREATE TABLE sm_categories (
    category_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    parent_id       INT
);

CREATE TABLE sm_products (
    product_id      BIGINT          AUTOINCREMENT,
    sku             STRING          NOT NULL,
    name            STRING          NOT NULL,
    brand           STRING,
    category_id     INT,
    supplier_id     INT,
    unit            STRING          NOT NULL,
    cost_price      DECIMAL(10,2)   NOT NULL,
    sell_price      DECIMAL(10,2)   NOT NULL,
    vat_rate        DECIMAL(5,2)    NOT NULL,
    is_active       BOOLEAN         NOT NULL
);

CREATE TABLE sm_warehouses (
    warehouse_id    BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    location        STRING
);

CREATE TABLE sm_stock (
    stock_id        BIGINT          AUTOINCREMENT,
    product_id      INT             NOT NULL,
    warehouse_id    INT             NOT NULL,
    quantity        DECIMAL(10,3)   NOT NULL,
    reorder_level   DECIMAL(10,3)   NOT NULL,
    reorder_qty     DECIMAL(10,3)   NOT NULL,
    last_updated    TIMESTAMP       NOT NULL
);

CREATE TABLE sm_stock_movements (
    movement_id     BIGINT          AUTOINCREMENT,
    product_id      INT             NOT NULL,
    warehouse_id    INT             NOT NULL,
    movement_type   STRING          NOT NULL,
    quantity        DECIMAL(10,3)   NOT NULL,
    reference       STRING,
    moved_at        TIMESTAMP       NOT NULL
);
