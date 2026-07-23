-- =============================================================
-- Oracle DDL — Real-World Domain Schemas
-- Domain: Restaurant Menu and Provisioning
-- =============================================================

CREATE TABLE suppliers (
    supplier_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(150)   NOT NULL,
    contact_name    VARCHAR2(100),
    phone           VARCHAR2(30),
    email           VARCHAR2(255),
    address         VARCHAR2(300)
);

CREATE TABLE ingredients (
    ingredient_id   NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(150)   NOT NULL,
    unit            VARCHAR2(20)    NOT NULL,
    unit_cost       NUMBER(10,4)    NOT NULL CHECK (unit_cost >= 0),
    stock_qty       NUMBER(10,3)    DEFAULT 0 NOT NULL CHECK (stock_qty >= 0),
    reorder_level   NUMBER(10,3)    DEFAULT 0 NOT NULL,
    supplier_id     NUMBER          REFERENCES suppliers (supplier_id) ON DELETE SET NULL,
    allergen_flags  CLOB
);

CREATE TABLE menu_categories (
    category_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(100)   NOT NULL,
    display_order   NUMBER(5)       DEFAULT 0 NOT NULL
);

CREATE TABLE menu_items (
    item_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(150)   NOT NULL,
    description     CLOB,
    category_id     NUMBER          NOT NULL REFERENCES menu_categories (category_id),
    sell_price      NUMBER(8,2)     NOT NULL CHECK (sell_price >= 0),
    prep_time_min   NUMBER(5),
    is_available    NUMBER(1)       DEFAULT 1 NOT NULL CHECK (is_available IN (0,1)),
    is_vegetarian   NUMBER(1)       DEFAULT 0 NOT NULL CHECK (is_vegetarian IN (0,1)),
    is_vegan        NUMBER(1)       DEFAULT 0 NOT NULL CHECK (is_vegan IN (0,1))
);

CREATE TABLE recipes (
    recipe_id       NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_id         NUMBER          NOT NULL REFERENCES menu_items  (item_id) ON DELETE CASCADE,
    ingredient_id   NUMBER          NOT NULL REFERENCES ingredients (ingredient_id),
    quantity        NUMBER(10,4)    NOT NULL CHECK (quantity > 0),
    CONSTRAINT uq_rst_recipe UNIQUE (item_id, ingredient_id)
);

CREATE TABLE purchase_orders (
    po_id           NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_id     NUMBER          NOT NULL REFERENCES suppliers (supplier_id),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    expected_date   DATE,
    received_date   DATE,
    status          VARCHAR2(20)    DEFAULT 'draft' NOT NULL
                    CHECK (status IN ('draft','sent','partial','received','cancelled')),
    total_cost      NUMBER(10,2)
);

CREATE TABLE po_lines (
    po_line_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    po_id           NUMBER          NOT NULL REFERENCES purchase_orders (po_id) ON DELETE CASCADE,
    ingredient_id   NUMBER          NOT NULL REFERENCES ingredients     (ingredient_id),
    qty_ordered     NUMBER(10,3)    NOT NULL CHECK (qty_ordered > 0),
    qty_received    NUMBER(10,3)    DEFAULT 0 NOT NULL,
    unit_cost       NUMBER(10,4)    NOT NULL CHECK (unit_cost >= 0)
);

CREATE TABLE tables (
    table_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_no    NUMBER(5)       NOT NULL UNIQUE,
    capacity    NUMBER(5)       NOT NULL CHECK (capacity > 0),
    location    VARCHAR2(20)    DEFAULT 'indoor' NOT NULL
                CHECK (location IN ('indoor','terrace','private'))
);

CREATE TABLE orders (
    order_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_id        NUMBER          NOT NULL REFERENCES tables (table_id),
    opened_at       TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    closed_at       TIMESTAMP WITH TIME ZONE,
    covers          NUMBER(5)       DEFAULT 1 NOT NULL,
    status          VARCHAR2(20)    DEFAULT 'open' NOT NULL
                    CHECK (status IN ('open','billed','paid','void')),
    total_amount    NUMBER(10,2),
    payment_method  VARCHAR2(20)    CHECK (payment_method IN ('cash','card','voucher'))
);

CREATE TABLE order_lines (
    line_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id    NUMBER          NOT NULL REFERENCES orders     (order_id) ON DELETE CASCADE,
    item_id     NUMBER          NOT NULL REFERENCES menu_items (item_id),
    quantity    NUMBER(5)       DEFAULT 1 NOT NULL CHECK (quantity > 0),
    unit_price  NUMBER(8,2)     NOT NULL CHECK (unit_price >= 0),
    notes       VARCHAR2(200)
);
