-- =============================================================
-- IBM Db2 DDL — Real-World Domain Schemas
-- Domain: Restaurant Menu and Provisioning
-- =============================================================

CREATE TABLE rst_suppliers (
    supplier_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300)
);

CREATE TABLE rst_ingredients (
    ingredient_id   INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    unit            VARCHAR(20)     NOT NULL,
    unit_cost       DECIMAL(10,4)   NOT NULL CHECK (unit_cost >= 0),
    stock_qty       DECIMAL(10,3)   NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    reorder_level   DECIMAL(10,3)   NOT NULL DEFAULT 0,
    supplier_id     INTEGER         REFERENCES rst_suppliers (supplier_id) ON DELETE SET NULL,
    allergen_flags  CLOB
);

CREATE TABLE rst_menu_categories (
    category_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    display_order   SMALLINT        NOT NULL DEFAULT 0
);

CREATE TABLE rst_menu_items (
    item_id         INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    description     CLOB,
    category_id     INTEGER         NOT NULL REFERENCES rst_menu_categories (category_id),
    sell_price      DECIMAL(8,2)    NOT NULL CHECK (sell_price >= 0),
    prep_time_min   SMALLINT,
    is_available    SMALLINT        NOT NULL DEFAULT 1 CHECK (is_available IN (0,1)),
    is_vegetarian   SMALLINT        NOT NULL DEFAULT 0 CHECK (is_vegetarian IN (0,1)),
    is_vegan        SMALLINT        NOT NULL DEFAULT 0 CHECK (is_vegan IN (0,1))
);

CREATE TABLE rst_recipes (
    recipe_id       INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    item_id         INTEGER         NOT NULL REFERENCES rst_menu_items  (item_id) ON DELETE CASCADE,
    ingredient_id   INTEGER         NOT NULL REFERENCES rst_ingredients (ingredient_id),
    quantity        DECIMAL(10,4)   NOT NULL CHECK (quantity > 0),
    CONSTRAINT uq_rst_recipe UNIQUE (item_id, ingredient_id)
);

CREATE TABLE rst_purchase_orders (
    po_id           INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    supplier_id     INTEGER         NOT NULL REFERENCES rst_suppliers (supplier_id),
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT TIMESTAMP,
    expected_date   DATE,
    received_date   DATE,
    status          VARCHAR(20)     NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft','sent','partial','received','cancelled')),
    total_cost      DECIMAL(10,2)
);

CREATE TABLE rst_po_lines (
    po_line_id      INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    po_id           INTEGER         NOT NULL REFERENCES rst_purchase_orders (po_id) ON DELETE CASCADE,
    ingredient_id   INTEGER         NOT NULL REFERENCES rst_ingredients     (ingredient_id),
    qty_ordered     DECIMAL(10,3)   NOT NULL CHECK (qty_ordered > 0),
    qty_received    DECIMAL(10,3)   NOT NULL DEFAULT 0,
    unit_cost       DECIMAL(10,4)   NOT NULL CHECK (unit_cost >= 0)
);

CREATE TABLE rst_tables (
    table_id    INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    table_no    SMALLINT        NOT NULL UNIQUE,
    capacity    SMALLINT        NOT NULL CHECK (capacity > 0),
    location    VARCHAR(20)     NOT NULL DEFAULT 'indoor'
                CHECK (location IN ('indoor','terrace','private'))
);

CREATE TABLE rst_orders (
    order_id        INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    table_id        INTEGER         NOT NULL REFERENCES rst_tables (table_id),
    opened_at       TIMESTAMP       NOT NULL DEFAULT CURRENT TIMESTAMP,
    closed_at       TIMESTAMP,
    covers          SMALLINT        NOT NULL DEFAULT 1,
    status          VARCHAR(20)     NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open','billed','paid','void')),
    total_amount    DECIMAL(10,2),
    payment_method  VARCHAR(20)     CHECK (payment_method IN ('cash','card','voucher'))
);

CREATE TABLE rst_order_lines (
    line_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    order_id    INTEGER         NOT NULL REFERENCES rst_orders     (order_id) ON DELETE CASCADE,
    item_id     INTEGER         NOT NULL REFERENCES rst_menu_items (item_id),
    quantity    SMALLINT        NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price  DECIMAL(8,2)    NOT NULL CHECK (unit_price >= 0),
    notes       VARCHAR(200)
);
