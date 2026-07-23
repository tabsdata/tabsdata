-- =============================================================
-- Microsoft SQL Server DDL — Real-World Domain Schemas
-- Domain: Supermarket Inventory
-- =============================================================

CREATE TABLE suppliers (
    supplier_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name            VARCHAR(150)    NOT NULL,
    contact_name    VARCHAR(100),
    phone           VARCHAR(30),
    email           VARCHAR(255),
    address         VARCHAR(300)
);

CREATE TABLE categories (
    category_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    parent_id       INT             REFERENCES categories (category_id)
                                    -- self-FK; SQL Server forbids ON DELETE SET NULL on a self-reference
);

CREATE TABLE products (
    product_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    sku             VARCHAR(50)     NOT NULL UNIQUE,
    name            VARCHAR(255)    NOT NULL,
    brand           VARCHAR(100),
    category_id     INT             REFERENCES categories (category_id) ON DELETE SET NULL,
    supplier_id     INT             REFERENCES suppliers  (supplier_id) ON DELETE SET NULL,
    unit            VARCHAR(20)     NOT NULL DEFAULT 'unit',
    cost_price      DECIMAL(10,2)   NOT NULL CHECK (cost_price >= 0),
    sell_price      DECIMAL(10,2)   NOT NULL CHECK (sell_price >= 0),
    vat_rate        DECIMAL(5,2)    NOT NULL DEFAULT 21.00,
    is_active       BIT             NOT NULL DEFAULT 1
);

CREATE TABLE warehouses (
    warehouse_id    INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name            VARCHAR(100)    NOT NULL,
    location        VARCHAR(255)
);

CREATE TABLE stock (
    stock_id        INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    product_id      INT             NOT NULL REFERENCES products   (product_id)   ON DELETE CASCADE,
    warehouse_id    INT             NOT NULL REFERENCES warehouses (warehouse_id) ON DELETE CASCADE,
    quantity        DECIMAL(10,3)   NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    reorder_level   DECIMAL(10,3)   NOT NULL DEFAULT 0,
    reorder_qty     DECIMAL(10,3)   NOT NULL DEFAULT 0,
    last_updated    DATETIMEOFFSET  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    CONSTRAINT uq_sm_stock UNIQUE (product_id, warehouse_id)
);

CREATE TABLE stock_movements (
    movement_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    product_id      INT             NOT NULL REFERENCES products   (product_id),
    warehouse_id    INT             NOT NULL REFERENCES warehouses (warehouse_id),
    movement_type   VARCHAR(20)     NOT NULL CHECK (movement_type IN ('receipt','sale','adjustment','transfer','waste')),
    quantity        DECIMAL(10,3)   NOT NULL,
    reference       VARCHAR(100),
    moved_at        DATETIMEOFFSET  NOT NULL DEFAULT SYSDATETIMEOFFSET()
);
