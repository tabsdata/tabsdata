-- =============================================================
-- Oracle DDL — Sakila (DVD rental store)
-- =============================================================

CREATE TABLE language (
    language_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(20)    NOT NULL
);

CREATE TABLE actor (
    actor_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name      VARCHAR2(45)    NOT NULL,
    last_name       VARCHAR2(45)    NOT NULL
);
CREATE INDEX idx_sk_actor_lname ON actor (last_name);

CREATE TABLE country (
    country_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country         VARCHAR2(50)    NOT NULL
);

CREATE TABLE city (
    city_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city            VARCHAR2(50)    NOT NULL,
    country_id      NUMBER          NOT NULL REFERENCES country (country_id)
);
CREATE INDEX idx_sk_city_country ON city (country_id);

CREATE TABLE address (
    address_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    address         VARCHAR2(50)    NOT NULL,
    address2        VARCHAR2(50),
    district        VARCHAR2(20),
    city_id         NUMBER          NOT NULL REFERENCES city (city_id),
    postal_code     VARCHAR2(10),
    phone           VARCHAR2(20)
);

CREATE TABLE category (
    category_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            VARCHAR2(25)    NOT NULL
);

CREATE TABLE film (
    film_id              NUMBER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title                VARCHAR2(128)  NOT NULL,
    description          CLOB,
    release_year         NUMBER(5),
    language_id          NUMBER         NOT NULL REFERENCES language (language_id),
    original_language_id NUMBER         REFERENCES language (language_id),
    rental_duration      NUMBER(5)      DEFAULT 3 NOT NULL,
    rental_rate          NUMBER(4,2)    DEFAULT 4.99 NOT NULL,
    length               NUMBER(5),
    replacement_cost     NUMBER(5,2)    DEFAULT 19.99 NOT NULL,
    rating               VARCHAR2(5)    DEFAULT 'G' CHECK (rating IN ('G','PG','PG-13','R','NC-17')),
    special_features     VARCHAR2(100)
);
CREATE INDEX idx_sk_film_title ON film (title);

CREATE TABLE film_actor (
    actor_id        NUMBER          NOT NULL REFERENCES actor (actor_id),
    film_id         NUMBER          NOT NULL REFERENCES film (film_id),
    PRIMARY KEY (actor_id, film_id)
);
CREATE INDEX idx_sk_fa_film ON film_actor (film_id);

CREATE TABLE film_category (
    film_id         NUMBER          NOT NULL REFERENCES film (film_id),
    category_id     NUMBER          NOT NULL REFERENCES category (category_id),
    PRIMARY KEY (film_id, category_id)
);

CREATE TABLE store (
    store_id         NUMBER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manager_staff_id NUMBER,
    address_id       NUMBER         NOT NULL REFERENCES address (address_id)
);

CREATE TABLE staff (
    staff_id        NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name      VARCHAR2(45)    NOT NULL,
    last_name       VARCHAR2(45)    NOT NULL,
    address_id      NUMBER          NOT NULL REFERENCES address (address_id),
    email           VARCHAR2(50),
    store_id        NUMBER          NOT NULL REFERENCES store (store_id),
    active          NUMBER(1)       DEFAULT 1 NOT NULL,
    username        VARCHAR2(16)    NOT NULL,
    password        VARCHAR2(40)
);

CREATE TABLE customer (
    customer_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id        NUMBER          NOT NULL REFERENCES store (store_id),
    first_name      VARCHAR2(45)    NOT NULL,
    last_name       VARCHAR2(45)    NOT NULL,
    email           VARCHAR2(50),
    address_id      NUMBER          NOT NULL REFERENCES address (address_id),
    active          NUMBER(1)       DEFAULT 1 NOT NULL,
    create_date     TIMESTAMP       NOT NULL
);
CREATE INDEX idx_sk_cust_lname ON customer (last_name);

CREATE TABLE inventory (
    inventory_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    film_id         NUMBER          NOT NULL REFERENCES film (film_id),
    store_id        NUMBER          NOT NULL REFERENCES store (store_id)
);
CREATE INDEX idx_sk_inv_film ON inventory (film_id);
CREATE INDEX idx_sk_inv_store_film ON inventory (store_id, film_id);

CREATE TABLE rental (
    rental_id       NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rental_date     TIMESTAMP       NOT NULL,
    inventory_id    NUMBER          NOT NULL REFERENCES inventory (inventory_id),
    customer_id     NUMBER          NOT NULL REFERENCES customer (customer_id),
    return_date     TIMESTAMP,
    staff_id        NUMBER          NOT NULL REFERENCES staff (staff_id)
);
CREATE INDEX idx_sk_rental_inv ON rental (inventory_id);
CREATE INDEX idx_sk_rental_cust ON rental (customer_id);

CREATE TABLE payment (
    payment_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     NUMBER          NOT NULL REFERENCES customer (customer_id),
    staff_id        NUMBER          NOT NULL REFERENCES staff (staff_id),
    rental_id       NUMBER          REFERENCES rental (rental_id) ON DELETE SET NULL,
    amount          NUMBER(7,2)     NOT NULL,
    payment_date    TIMESTAMP       NOT NULL
);
