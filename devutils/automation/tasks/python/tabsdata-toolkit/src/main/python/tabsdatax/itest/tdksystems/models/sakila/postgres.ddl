-- =============================================================
-- PostgreSQL DDL — Sakila (DVD rental store)
-- =============================================================

CREATE TABLE sk_language (
    language_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(20)     NOT NULL
);

CREATE TABLE sk_actor (
    actor_id        SERIAL          PRIMARY KEY,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL
);
CREATE INDEX idx_sk_actor_lname ON sk_actor (last_name);

CREATE TABLE sk_country (
    country_id      SERIAL          PRIMARY KEY,
    country         VARCHAR(50)     NOT NULL
);

CREATE TABLE sk_city (
    city_id         SERIAL          PRIMARY KEY,
    city            VARCHAR(50)     NOT NULL,
    country_id      INT             NOT NULL REFERENCES sk_country (country_id)
);
CREATE INDEX idx_sk_city_country ON sk_city (country_id);

CREATE TABLE sk_address (
    address_id      SERIAL          PRIMARY KEY,
    address         VARCHAR(50)     NOT NULL,
    address2        VARCHAR(50),
    district        VARCHAR(20),
    city_id         INT             NOT NULL REFERENCES sk_city (city_id),
    postal_code     VARCHAR(10),
    phone           VARCHAR(20)
);

CREATE TABLE sk_category (
    category_id     SERIAL          PRIMARY KEY,
    name            VARCHAR(25)     NOT NULL
);

CREATE TABLE sk_film (
    film_id              SERIAL         PRIMARY KEY,
    title                VARCHAR(128)   NOT NULL,
    description          TEXT,
    release_year         SMALLINT,
    language_id          INT            NOT NULL REFERENCES sk_language (language_id),
    original_language_id INT            REFERENCES sk_language (language_id),
    rental_duration      SMALLINT       NOT NULL DEFAULT 3,
    rental_rate          NUMERIC(4,2)   NOT NULL DEFAULT 4.99,
    length               SMALLINT,
    replacement_cost     NUMERIC(5,2)   NOT NULL DEFAULT 19.99,
    rating               TEXT           DEFAULT 'G' CHECK (rating IN ('G','PG','PG-13','R','NC-17')),
    special_features     VARCHAR(100)
);
CREATE INDEX idx_sk_film_title ON sk_film (title);

CREATE TABLE sk_film_actor (
    actor_id        INT             NOT NULL REFERENCES sk_actor (actor_id),
    film_id         INT             NOT NULL REFERENCES sk_film (film_id),
    PRIMARY KEY (actor_id, film_id)
);
CREATE INDEX idx_sk_fa_film ON sk_film_actor (film_id);

CREATE TABLE sk_film_category (
    film_id         INT             NOT NULL REFERENCES sk_film (film_id),
    category_id     INT             NOT NULL REFERENCES sk_category (category_id),
    PRIMARY KEY (film_id, category_id)
);

CREATE TABLE sk_store (
    store_id         SERIAL         PRIMARY KEY,
    manager_staff_id INT,
    address_id       INT            NOT NULL REFERENCES sk_address (address_id)
);

CREATE TABLE sk_staff (
    staff_id        SERIAL          PRIMARY KEY,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    address_id      INT             NOT NULL REFERENCES sk_address (address_id),
    email           VARCHAR(50),
    store_id        INT             NOT NULL REFERENCES sk_store (store_id),
    active          SMALLINT        NOT NULL DEFAULT 1,
    username        VARCHAR(16)     NOT NULL,
    password        VARCHAR(40)
);

CREATE TABLE sk_customer (
    customer_id     SERIAL          PRIMARY KEY,
    store_id        INT             NOT NULL REFERENCES sk_store (store_id),
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    email           VARCHAR(50),
    address_id      INT             NOT NULL REFERENCES sk_address (address_id),
    active          SMALLINT        NOT NULL DEFAULT 1,
    create_date     TIMESTAMP       NOT NULL
);
CREATE INDEX idx_sk_cust_lname ON sk_customer (last_name);

CREATE TABLE sk_inventory (
    inventory_id    SERIAL          PRIMARY KEY,
    film_id         INT             NOT NULL REFERENCES sk_film (film_id),
    store_id        INT             NOT NULL REFERENCES sk_store (store_id)
);
CREATE INDEX idx_sk_inv_film ON sk_inventory (film_id);
CREATE INDEX idx_sk_inv_store_film ON sk_inventory (store_id, film_id);

CREATE TABLE sk_rental (
    rental_id       SERIAL          PRIMARY KEY,
    rental_date     TIMESTAMP       NOT NULL,
    inventory_id    INT             NOT NULL REFERENCES sk_inventory (inventory_id),
    customer_id     INT             NOT NULL REFERENCES sk_customer (customer_id),
    return_date     TIMESTAMP,
    staff_id        INT             NOT NULL REFERENCES sk_staff (staff_id)
);
CREATE INDEX idx_sk_rental_inv ON sk_rental (inventory_id);
CREATE INDEX idx_sk_rental_cust ON sk_rental (customer_id);

CREATE TABLE sk_payment (
    payment_id      SERIAL          PRIMARY KEY,
    customer_id     INT             NOT NULL REFERENCES sk_customer (customer_id),
    staff_id        INT             NOT NULL REFERENCES sk_staff (staff_id),
    rental_id       INT             REFERENCES sk_rental (rental_id) ON DELETE SET NULL,
    amount          NUMERIC(7,2)    NOT NULL,
    payment_date    TIMESTAMP       NOT NULL
);
