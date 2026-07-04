-- =============================================================
-- Snowflake SQL DDL — Sakila (DVD rental store)
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE sk_language (
    language_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL
);

CREATE TABLE sk_actor (
    actor_id        BIGINT          AUTOINCREMENT,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL
);

CREATE TABLE sk_country (
    country_id      BIGINT          AUTOINCREMENT,
    country         STRING          NOT NULL
);

CREATE TABLE sk_city (
    city_id         BIGINT          AUTOINCREMENT,
    city            STRING          NOT NULL,
    country_id      INT             NOT NULL
);

CREATE TABLE sk_address (
    address_id      BIGINT          AUTOINCREMENT,
    address         STRING          NOT NULL,
    address2        STRING,
    district        STRING,
    city_id         INT             NOT NULL,
    postal_code     STRING,
    phone           STRING
);

CREATE TABLE sk_category (
    category_id     BIGINT          AUTOINCREMENT,
    name            STRING          NOT NULL
);

CREATE TABLE sk_film (
    film_id              BIGINT         AUTOINCREMENT,
    title                STRING         NOT NULL,
    description          STRING,
    release_year         SMALLINT,
    language_id          INT            NOT NULL,
    original_language_id INT,
    rental_duration      SMALLINT       NOT NULL,
    rental_rate          DECIMAL(4,2)   NOT NULL,
    length               SMALLINT,
    replacement_cost     DECIMAL(5,2)   NOT NULL,
    rating               STRING,
    special_features     STRING
);

CREATE TABLE sk_film_actor (
    actor_id        INT             NOT NULL,
    film_id         INT             NOT NULL
);

CREATE TABLE sk_film_category (
    film_id         INT             NOT NULL,
    category_id     INT             NOT NULL
);

CREATE TABLE sk_store (
    store_id         BIGINT         AUTOINCREMENT,
    manager_staff_id INT,
    address_id       INT            NOT NULL
);

CREATE TABLE sk_staff (
    staff_id        BIGINT          AUTOINCREMENT,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL,
    address_id      INT             NOT NULL,
    email           STRING,
    store_id        INT             NOT NULL,
    active          SMALLINT        NOT NULL,
    username        STRING          NOT NULL,
    password        STRING
);

CREATE TABLE sk_customer (
    customer_id     BIGINT          AUTOINCREMENT,
    store_id        INT             NOT NULL,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL,
    email           STRING,
    address_id      INT             NOT NULL,
    active          SMALLINT        NOT NULL,
    create_date     TIMESTAMP       NOT NULL
);

CREATE TABLE sk_inventory (
    inventory_id    BIGINT          AUTOINCREMENT,
    film_id         INT             NOT NULL,
    store_id        INT             NOT NULL
);

CREATE TABLE sk_rental (
    rental_id       BIGINT          AUTOINCREMENT,
    rental_date     TIMESTAMP       NOT NULL,
    inventory_id    INT             NOT NULL,
    customer_id     INT             NOT NULL,
    return_date     TIMESTAMP,
    staff_id        INT             NOT NULL
);

CREATE TABLE sk_payment (
    payment_id      BIGINT          AUTOINCREMENT,
    customer_id     INT             NOT NULL,
    staff_id        INT             NOT NULL,
    rental_id       INT,
    amount          DECIMAL(7,2)    NOT NULL,
    payment_date    TIMESTAMP       NOT NULL
);
