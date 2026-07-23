-- =============================================================
-- Databricks SQL DDL — Sakila (DVD rental store)
-- Targets a Unity-Catalog schema; tables default to Delta.
-- PK/FK constraints are informational on Databricks; we keep
-- IDENTITY columns and indexes-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE language (
    language_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    name            STRING          NOT NULL
);

CREATE TABLE actor (
    actor_id        BIGINT          GENERATED ALWAYS AS IDENTITY,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL
);

CREATE TABLE country (
    country_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    country         STRING          NOT NULL
);

CREATE TABLE city (
    city_id         BIGINT          GENERATED ALWAYS AS IDENTITY,
    city            STRING          NOT NULL,
    country_id      INT             NOT NULL
);

CREATE TABLE address (
    address_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    address         STRING          NOT NULL,
    address2        STRING,
    district        STRING,
    city_id         INT             NOT NULL,
    postal_code     STRING,
    phone           STRING
);

CREATE TABLE category (
    category_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    name            STRING          NOT NULL
);

CREATE TABLE film (
    film_id              BIGINT         GENERATED ALWAYS AS IDENTITY,
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

CREATE TABLE film_actor (
    actor_id        INT             NOT NULL,
    film_id         INT             NOT NULL
);

CREATE TABLE film_category (
    film_id         INT             NOT NULL,
    category_id     INT             NOT NULL
);

CREATE TABLE store (
    store_id         BIGINT         GENERATED ALWAYS AS IDENTITY,
    manager_staff_id INT,
    address_id       INT            NOT NULL
);

CREATE TABLE staff (
    staff_id        BIGINT          GENERATED ALWAYS AS IDENTITY,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL,
    address_id      INT             NOT NULL,
    email           STRING,
    store_id        INT             NOT NULL,
    active          SMALLINT        NOT NULL,
    username        STRING          NOT NULL,
    password        STRING
);

CREATE TABLE customer (
    customer_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    store_id        INT             NOT NULL,
    first_name      STRING          NOT NULL,
    last_name       STRING          NOT NULL,
    email           STRING,
    address_id      INT             NOT NULL,
    active          SMALLINT        NOT NULL,
    create_date     TIMESTAMP       NOT NULL
);

CREATE TABLE inventory (
    inventory_id    BIGINT          GENERATED ALWAYS AS IDENTITY,
    film_id         INT             NOT NULL,
    store_id        INT             NOT NULL
);

CREATE TABLE rental (
    rental_id       BIGINT          GENERATED ALWAYS AS IDENTITY,
    rental_date     TIMESTAMP       NOT NULL,
    inventory_id    INT             NOT NULL,
    customer_id     INT             NOT NULL,
    return_date     TIMESTAMP,
    staff_id        INT             NOT NULL
);

CREATE TABLE payment (
    payment_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    customer_id     INT             NOT NULL,
    staff_id        INT             NOT NULL,
    rental_id       INT,
    amount          DECIMAL(7,2)    NOT NULL,
    payment_date    TIMESTAMP       NOT NULL
);
