-- =============================================================
-- PostgreSQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 7. RESTAURANT MENU AND PROVISIONING SYSTEM
-- =============================================================

CREATE TABLE suppliers (
    supplier_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300)
);

CREATE TABLE ingredients (
    ingredient_id   SERIAL          PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    unit            VARCHAR(20)     NOT NULL,
    unit_cost       NUMERIC(10,4)   NOT NULL CHECK (unit_cost >= 0),
    stock_qty       NUMERIC(10,3)   NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    reorder_level   NUMERIC(10,3)   NOT NULL DEFAULT 0,
    supplier_id     INT             REFERENCES suppliers (supplier_id) ON DELETE SET NULL,
    allergen_flags  TEXT            -- free-form, e.g. 'gluten,dairy'
);

CREATE TABLE menu_categories (
    category_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    display_order   SMALLINT        NOT NULL DEFAULT 0
);

CREATE TABLE menu_items (
    item_id         SERIAL          PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    description     TEXT,
    category_id     INT             NOT NULL REFERENCES menu_categories (category_id),
    sell_price      NUMERIC(8,2)    NOT NULL CHECK (sell_price >= 0),
    prep_time_min   SMALLINT,
    is_available    SMALLINT        NOT NULL DEFAULT 1 CHECK (is_available IN (0,1)),
    is_vegetarian   SMALLINT        NOT NULL DEFAULT 0 CHECK (is_vegetarian IN (0,1)),
    is_vegan        SMALLINT        NOT NULL DEFAULT 0 CHECK (is_vegan IN (0,1))
);

CREATE TABLE recipes (
    recipe_id       SERIAL          PRIMARY KEY,
    item_id         INT             NOT NULL REFERENCES menu_items  (item_id)       ON DELETE CASCADE,
    ingredient_id   INT             NOT NULL REFERENCES ingredients (ingredient_id) ON DELETE RESTRICT,
    quantity        NUMERIC(10,4)   NOT NULL CHECK (quantity > 0),
    UNIQUE (item_id, ingredient_id)
);

CREATE TABLE purchase_orders (
    po_id           SERIAL          PRIMARY KEY,
    supplier_id     INT             NOT NULL REFERENCES suppliers (supplier_id),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expected_date   DATE,
    received_date   DATE,
    status          TEXT            NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft','sent','partial','received','cancelled')),
    total_cost      NUMERIC(10,2)
);

CREATE TABLE po_lines (
    po_line_id      SERIAL          PRIMARY KEY,
    po_id           INT             NOT NULL REFERENCES purchase_orders (po_id)         ON DELETE CASCADE,
    ingredient_id   INT             NOT NULL REFERENCES ingredients     (ingredient_id),
    qty_ordered     NUMERIC(10,3)   NOT NULL CHECK (qty_ordered > 0),
    qty_received    NUMERIC(10,3)   NOT NULL DEFAULT 0,
    unit_cost       NUMERIC(10,4)   NOT NULL CHECK (unit_cost >= 0)
);

CREATE TABLE tables (
    table_id    SERIAL          PRIMARY KEY,
    table_no    SMALLINT        NOT NULL UNIQUE,
    capacity    SMALLINT        NOT NULL CHECK (capacity > 0),
    location    TEXT            NOT NULL DEFAULT 'indoor'
                CHECK (location IN ('indoor','terrace','private'))
);

CREATE TABLE orders (
    order_id        SERIAL          PRIMARY KEY,
    table_id        INT             NOT NULL REFERENCES tables (table_id),
    opened_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    covers          SMALLINT        NOT NULL DEFAULT 1,
    status          TEXT            NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open','billed','paid','void')),
    total_amount    NUMERIC(10,2),
    payment_method  TEXT            CHECK (payment_method IN ('cash','card','voucher'))
);

CREATE TABLE order_lines (
    line_id     SERIAL          PRIMARY KEY,
    order_id    INT             NOT NULL REFERENCES orders     (order_id) ON DELETE CASCADE,
    item_id     INT             NOT NULL REFERENCES menu_items (item_id),
    quantity    SMALLINT        NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price  NUMERIC(8,2)    NOT NULL CHECK (unit_price >= 0),
    notes       VARCHAR(200)
);
