-- =============================================================
-- MySQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 7. RESTAURANT MENU AND PROVISIONING SYSTEM
-- =============================================================

CREATE TABLE rst_suppliers (
    supplier_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300),
    PRIMARY KEY (supplier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_ingredients (
    ingredient_id   INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    unit            VARCHAR(20)     NOT NULL,   -- kg, litre, unit, g, ml
    unit_cost       DECIMAL(10,4)   NOT NULL,
    stock_qty       DECIMAL(10,3)   NOT NULL DEFAULT 0,
    reorder_level   DECIMAL(10,3)   NOT NULL DEFAULT 0,
    supplier_id     INT UNSIGNED,
    allergen_flags  SET('gluten','dairy','nuts','eggs','soy','shellfish','fish') DEFAULT NULL,
    PRIMARY KEY (ingredient_id),
    CONSTRAINT fk_rsting_sup FOREIGN KEY (supplier_id) REFERENCES rst_suppliers (supplier_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_menu_categories (
    category_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(100)    NOT NULL,
    display_order   TINYINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_menu_items (
    item_id         INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    description     TEXT,
    category_id     INT UNSIGNED    NOT NULL,
    sell_price      DECIMAL(8,2)    NOT NULL,
    prep_time_min   TINYINT UNSIGNED,
    is_available    TINYINT(1)      NOT NULL DEFAULT 1,
    is_vegetarian   TINYINT(1)      NOT NULL DEFAULT 0,
    is_vegan        TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id),
    CONSTRAINT fk_rstitem_cat FOREIGN KEY (category_id) REFERENCES rst_menu_categories (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_recipes (
    recipe_id       INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    item_id         INT UNSIGNED    NOT NULL,
    ingredient_id   INT UNSIGNED    NOT NULL,
    quantity        DECIMAL(10,4)   NOT NULL,  -- in ingredient's unit, per 1 portion
    PRIMARY KEY (recipe_id),
    UNIQUE KEY uq_rst_recipe (item_id, ingredient_id),
    CONSTRAINT fk_rstrec_item FOREIGN KEY (item_id)       REFERENCES rst_menu_items   (item_id)       ON DELETE CASCADE,
    CONSTRAINT fk_rstrec_ing  FOREIGN KEY (ingredient_id) REFERENCES rst_ingredients  (ingredient_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_purchase_orders (
    po_id           INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    supplier_id     INT UNSIGNED    NOT NULL,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expected_date   DATE,
    received_date   DATE,
    status          ENUM('draft','sent','partial','received','cancelled') NOT NULL DEFAULT 'draft',
    total_cost      DECIMAL(10,2),
    PRIMARY KEY (po_id),
    CONSTRAINT fk_rstpo_sup FOREIGN KEY (supplier_id) REFERENCES rst_suppliers (supplier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_po_lines (
    po_line_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    po_id           INT UNSIGNED    NOT NULL,
    ingredient_id   INT UNSIGNED    NOT NULL,
    qty_ordered     DECIMAL(10,3)   NOT NULL,
    qty_received    DECIMAL(10,3)   NOT NULL DEFAULT 0,
    unit_cost       DECIMAL(10,4)   NOT NULL,
    PRIMARY KEY (po_line_id),
    CONSTRAINT fk_rstpol_po  FOREIGN KEY (po_id)          REFERENCES rst_purchase_orders (po_id)          ON DELETE CASCADE,
    CONSTRAINT fk_rstpol_ing FOREIGN KEY (ingredient_id)  REFERENCES rst_ingredients     (ingredient_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_tables (
    table_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    table_no    INT UNSIGNED NOT NULL,
    capacity    INT UNSIGNED NOT NULL,
    location    ENUM('indoor','terrace','private') NOT NULL DEFAULT 'indoor',
    PRIMARY KEY (table_id),
    UNIQUE KEY uq_rst_tableno (table_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_orders (
    order_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    table_id        INT UNSIGNED    NOT NULL,
    opened_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at       DATETIME,
    covers          TINYINT UNSIGNED NOT NULL DEFAULT 1,
    status          ENUM('open','billed','paid','void') NOT NULL DEFAULT 'open',
    total_amount    DECIMAL(10,2),
    payment_method  ENUM('cash','card','voucher'),
    PRIMARY KEY (order_id),
    CONSTRAINT fk_rstord_tbl FOREIGN KEY (table_id) REFERENCES rst_tables (table_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rst_order_lines (
    line_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    order_id    INT UNSIGNED    NOT NULL,
    item_id     INT UNSIGNED    NOT NULL,
    quantity    TINYINT UNSIGNED NOT NULL DEFAULT 1,
    unit_price  DECIMAL(8,2)    NOT NULL,
    notes       VARCHAR(200),
    PRIMARY KEY (line_id),
    CONSTRAINT fk_rstol_ord  FOREIGN KEY (order_id) REFERENCES rst_orders    (order_id) ON DELETE CASCADE,
    CONSTRAINT fk_rstol_item FOREIGN KEY (item_id)  REFERENCES rst_menu_items (item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
