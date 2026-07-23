-- =============================================================
-- IBM Db2 DDL — Sakila (DVD rental store)
-- =============================================================

CREATE TABLE language (
    language_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(20)     NOT NULL
);

CREATE TABLE actor (
    actor_id        INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL
);
CREATE INDEX idx_sk_actor_lname ON actor (last_name);

CREATE TABLE country (
    country_id      INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    country         VARCHAR(50)     NOT NULL
);

CREATE TABLE city (
    city_id         INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    city            VARCHAR(50)     NOT NULL,
    country_id      INTEGER         NOT NULL REFERENCES country (country_id)
);
CREATE INDEX idx_sk_city_country ON city (country_id);

CREATE TABLE address (
    address_id      INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    address         VARCHAR(50)     NOT NULL,
    address2        VARCHAR(50),
    district        VARCHAR(20),
    city_id         INTEGER         NOT NULL REFERENCES city (city_id),
    postal_code     VARCHAR(10),
    phone           VARCHAR(20)
);

CREATE TABLE category (
    category_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    name            VARCHAR(25)     NOT NULL
);

CREATE TABLE film (
    film_id              INTEGER        NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    title                VARCHAR(128)   NOT NULL,
    description          CLOB,
    release_year         SMALLINT,
    language_id          INTEGER        NOT NULL REFERENCES language (language_id),
    original_language_id INTEGER        REFERENCES language (language_id),
    rental_duration      SMALLINT       NOT NULL DEFAULT 3,
    rental_rate          DECIMAL(4,2)   NOT NULL DEFAULT 4.99,
    length               SMALLINT,
    replacement_cost     DECIMAL(5,2)   NOT NULL DEFAULT 19.99,
    rating               VARCHAR(5)     DEFAULT 'G' CHECK (rating IN ('G','PG','PG-13','R','NC-17')),
    special_features     VARCHAR(100)
);
CREATE INDEX idx_sk_film_title ON film (title);

CREATE TABLE film_actor (
    actor_id        INTEGER         NOT NULL REFERENCES actor (actor_id),
    film_id         INTEGER         NOT NULL REFERENCES film (film_id),
    PRIMARY KEY (actor_id, film_id)
);
CREATE INDEX idx_sk_fa_film ON film_actor (film_id);

CREATE TABLE film_category (
    film_id         INTEGER         NOT NULL REFERENCES film (film_id),
    category_id     INTEGER         NOT NULL REFERENCES category (category_id),
    PRIMARY KEY (film_id, category_id)
);

CREATE TABLE store (
    store_id         INTEGER        NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    manager_staff_id INTEGER,
    address_id       INTEGER        NOT NULL REFERENCES address (address_id)
);

CREATE TABLE staff (
    staff_id        INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    address_id      INTEGER         NOT NULL REFERENCES address (address_id),
    email           VARCHAR(50),
    store_id        INTEGER         NOT NULL REFERENCES store (store_id),
    active          SMALLINT        NOT NULL DEFAULT 1,
    username        VARCHAR(16)     NOT NULL,
    password        VARCHAR(40)
);

CREATE TABLE customer (
    customer_id     INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    store_id        INTEGER         NOT NULL REFERENCES store (store_id),
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    email           VARCHAR(50),
    address_id      INTEGER         NOT NULL REFERENCES address (address_id),
    active          SMALLINT        NOT NULL DEFAULT 1,
    create_date     TIMESTAMP       NOT NULL
);
CREATE INDEX idx_sk_cust_lname ON customer (last_name);

CREATE TABLE inventory (
    inventory_id    INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    film_id         INTEGER         NOT NULL REFERENCES film (film_id),
    store_id        INTEGER         NOT NULL REFERENCES store (store_id)
);
CREATE INDEX idx_sk_inv_film ON inventory (film_id);
CREATE INDEX idx_sk_inv_store_film ON inventory (store_id, film_id);

CREATE TABLE rental (
    rental_id       INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    rental_date     TIMESTAMP       NOT NULL,
    inventory_id    INTEGER         NOT NULL REFERENCES inventory (inventory_id),
    customer_id     INTEGER         NOT NULL REFERENCES customer (customer_id),
    return_date     TIMESTAMP,
    staff_id        INTEGER         NOT NULL REFERENCES staff (staff_id)
);
CREATE INDEX idx_sk_rental_inv ON rental (inventory_id);
CREATE INDEX idx_sk_rental_cust ON rental (customer_id);

CREATE TABLE payment (
    payment_id      INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    customer_id     INTEGER         NOT NULL REFERENCES customer (customer_id),
    staff_id        INTEGER         NOT NULL REFERENCES staff (staff_id),
    rental_id       INTEGER         REFERENCES rental (rental_id) ON DELETE SET NULL,
    amount          DECIMAL(7,2)    NOT NULL,
    payment_date    TIMESTAMP       NOT NULL
);
