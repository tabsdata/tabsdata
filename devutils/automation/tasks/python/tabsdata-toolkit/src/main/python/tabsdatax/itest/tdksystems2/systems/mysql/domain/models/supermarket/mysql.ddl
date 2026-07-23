-- =============================================================
-- MySQL DDL — Real-World Domain Schemas
-- Domains: Supermarket Inventory, Hospital Patient, Insurance
--          Management, Insurance Claims, Courier Logistics,
--          School Grades, Restaurant Menu & Provisioning
-- =============================================================

-- =============================================================
-- 1. SUPERMARKET INVENTORY SYSTEM
-- =============================================================

CREATE TABLE suppliers (
    supplier_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300),
    PRIMARY KEY (supplier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE categories (
    category_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(100)    NOT NULL,
    parent_id       INT UNSIGNED    NULL,
    PRIMARY KEY (category_id),
    CONSTRAINT fk_smcat_parent FOREIGN KEY (parent_id)
        REFERENCES categories (category_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE products (
    product_id      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    sku             VARCHAR(50)     NOT NULL,
    name            VARCHAR(255)    NOT NULL,
    brand           VARCHAR(100),
    category_id     INT UNSIGNED,
    supplier_id     INT UNSIGNED,
    unit            VARCHAR(20)     NOT NULL DEFAULT 'unit',  -- kg, litre, unit, pack
    cost_price      DECIMAL(10,2)   NOT NULL,
    sell_price      DECIMAL(10,2)   NOT NULL,
    vat_rate        DECIMAL(5,2)    NOT NULL DEFAULT 21.00,
    is_active       TINYINT(1)      NOT NULL DEFAULT 1,
    PRIMARY KEY (product_id),
    UNIQUE KEY uq_sm_sku (sku),
    CONSTRAINT fk_smprod_cat  FOREIGN KEY (category_id) REFERENCES categories (category_id) ON DELETE SET NULL,
    CONSTRAINT fk_smprod_sup  FOREIGN KEY (supplier_id) REFERENCES suppliers  (supplier_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE warehouses (
    warehouse_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name            VARCHAR(100)    NOT NULL,
    location        VARCHAR(255),
    PRIMARY KEY (warehouse_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE stock (
    stock_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    product_id      INT UNSIGNED    NOT NULL,
    warehouse_id    INT UNSIGNED    NOT NULL,
    quantity        DECIMAL(10,3)   NOT NULL DEFAULT 0,
    reorder_level   DECIMAL(10,3)   NOT NULL DEFAULT 0,
    reorder_qty     DECIMAL(10,3)   NOT NULL DEFAULT 0,
    last_updated    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_id),
    UNIQUE KEY uq_sm_stock (product_id, warehouse_id),
    CONSTRAINT fk_smstk_prod FOREIGN KEY (product_id)  REFERENCES products   (product_id)  ON DELETE CASCADE,
    CONSTRAINT fk_smstk_wh   FOREIGN KEY (warehouse_id) REFERENCES warehouses (warehouse_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE stock_movements (
    movement_id     INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    product_id      INT UNSIGNED    NOT NULL,
    warehouse_id    INT UNSIGNED    NOT NULL,
    movement_type   ENUM('receipt','sale','adjustment','transfer','waste') NOT NULL,
    quantity        DECIMAL(10,3)   NOT NULL,   -- positive = in, negative = out
    reference       VARCHAR(100),               -- PO number, invoice, etc.
    moved_at        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (movement_id),
    CONSTRAINT fk_smmov_prod FOREIGN KEY (product_id)  REFERENCES products   (product_id),
    CONSTRAINT fk_smmov_wh   FOREIGN KEY (warehouse_id) REFERENCES warehouses (warehouse_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
