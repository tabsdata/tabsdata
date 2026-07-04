-- =============================================================
-- Snowflake SQL DDL — Restaurant Menu & Provisioning System
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE rst_suppliers (
    supplier_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    contact_name    STRING,
    phone           STRING,
    email           STRING,
    address         STRING
);

CREATE TABLE rst_ingredients (
    ingredient_id   BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    unit            STRING          NOT NULL,
    unit_cost       DECIMAL(10,4)   NOT NULL,
    stock_qty       DECIMAL(10,3)   NOT NULL,
    reorder_level   DECIMAL(10,3)   NOT NULL,
    supplier_id     INT,
    allergen_flags  STRING
);

CREATE TABLE rst_menu_categories (
    category_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    display_order   TINYINT         NOT NULL
);

CREATE TABLE rst_menu_items (
    item_id         BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL,
    description     STRING,
    category_id     INT             NOT NULL,
    sell_price      DECIMAL(8,2)    NOT NULL,
    prep_time_min   TINYINT,
    is_available    BOOLEAN         NOT NULL,
    is_vegetarian   BOOLEAN         NOT NULL,
    is_vegan        BOOLEAN         NOT NULL
);

CREATE TABLE rst_recipes (
    recipe_id       BIGINT          AUTOINCREMENT,
    item_id         INT             NOT NULL,
    ingredient_id   INT             NOT NULL,
    quantity        DECIMAL(10,4)   NOT NULL
);

CREATE TABLE rst_purchase_orders (
    po_id           BIGINT          AUTOINCREMENT,
    supplier_id     INT             NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    expected_date   DATE,
    received_date   DATE,
    status          STRING          NOT NULL,
    total_cost      DECIMAL(10,2)
);

CREATE TABLE rst_po_lines (
    po_line_id      BIGINT          AUTOINCREMENT,
    po_id           INT             NOT NULL,
    ingredient_id   INT             NOT NULL,
    qty_ordered     DECIMAL(10,3)   NOT NULL,
    qty_received    DECIMAL(10,3)   NOT NULL,
    unit_cost       DECIMAL(10,4)   NOT NULL
);

CREATE TABLE rst_tables (
    table_id        BIGINT          AUTOINCREMENT,
    table_no        INT             NOT NULL,
    capacity        INT             NOT NULL,
    location        STRING          NOT NULL
);

CREATE TABLE rst_orders (
    order_id        BIGINT          AUTOINCREMENT,
    table_id        INT             NOT NULL,
    opened_at       TIMESTAMP       NOT NULL,
    closed_at       TIMESTAMP,
    covers          TINYINT         NOT NULL,
    status          STRING          NOT NULL,
    total_amount    DECIMAL(10,2),
    payment_method  STRING
);

CREATE TABLE rst_order_lines (
    line_id         BIGINT          AUTOINCREMENT,
    order_id        INT             NOT NULL,
    item_id         INT             NOT NULL,
    quantity        TINYINT         NOT NULL,
    unit_price      DECIMAL(8,2)    NOT NULL,
    notes           STRING
);
