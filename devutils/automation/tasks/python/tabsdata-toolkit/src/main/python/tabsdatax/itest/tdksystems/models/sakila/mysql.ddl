-- =============================================================
-- MySQL DDL — Sakila (DVD rental store)
-- =============================================================

CREATE TABLE sk_language (
    language_id     INT             NOT NULL AUTO_INCREMENT,
    name            VARCHAR(20)     NOT NULL,
    PRIMARY KEY (language_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_actor (
    actor_id        INT             NOT NULL AUTO_INCREMENT,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    PRIMARY KEY (actor_id),
    KEY idx_sk_actor_lname (last_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_country (
    country_id      INT             NOT NULL AUTO_INCREMENT,
    country         VARCHAR(50)     NOT NULL,
    PRIMARY KEY (country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_city (
    city_id         INT             NOT NULL AUTO_INCREMENT,
    city            VARCHAR(50)     NOT NULL,
    country_id      INT             NOT NULL,
    PRIMARY KEY (city_id),
    KEY idx_sk_city_country (country_id),
    CONSTRAINT fk_skcity_country FOREIGN KEY (country_id) REFERENCES sk_country (country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_address (
    address_id      INT             NOT NULL AUTO_INCREMENT,
    address         VARCHAR(50)     NOT NULL,
    address2        VARCHAR(50),
    district        VARCHAR(20),
    city_id         INT             NOT NULL,
    postal_code     VARCHAR(10),
    phone           VARCHAR(20),
    PRIMARY KEY (address_id),
    CONSTRAINT fk_skaddr_city FOREIGN KEY (city_id) REFERENCES sk_city (city_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_category (
    category_id     INT             NOT NULL AUTO_INCREMENT,
    name            VARCHAR(25)     NOT NULL,
    PRIMARY KEY (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_film (
    film_id              INT            NOT NULL AUTO_INCREMENT,
    title                VARCHAR(128)   NOT NULL,
    description          TEXT,
    release_year         SMALLINT,
    language_id          INT            NOT NULL,
    original_language_id INT,
    rental_duration      SMALLINT       NOT NULL DEFAULT 3,
    rental_rate          DECIMAL(4,2)   NOT NULL DEFAULT 4.99,
    length               SMALLINT,
    replacement_cost     DECIMAL(5,2)   NOT NULL DEFAULT 19.99,
    rating               ENUM('G','PG','PG-13','R','NC-17') DEFAULT 'G',
    special_features     VARCHAR(100),
    PRIMARY KEY (film_id),
    KEY idx_sk_film_title (title),
    CONSTRAINT fk_skfilm_lang FOREIGN KEY (language_id) REFERENCES sk_language (language_id),
    CONSTRAINT fk_skfilm_olang FOREIGN KEY (original_language_id) REFERENCES sk_language (language_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_film_actor (
    actor_id        INT             NOT NULL,
    film_id         INT             NOT NULL,
    PRIMARY KEY (actor_id, film_id),
    KEY idx_sk_fa_film (film_id),
    CONSTRAINT fk_skfa_actor FOREIGN KEY (actor_id) REFERENCES sk_actor (actor_id),
    CONSTRAINT fk_skfa_film FOREIGN KEY (film_id) REFERENCES sk_film (film_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_film_category (
    film_id         INT             NOT NULL,
    category_id     INT             NOT NULL,
    PRIMARY KEY (film_id, category_id),
    CONSTRAINT fk_skfc_film FOREIGN KEY (film_id) REFERENCES sk_film (film_id),
    CONSTRAINT fk_skfc_cat FOREIGN KEY (category_id) REFERENCES sk_category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_store (
    store_id         INT            NOT NULL AUTO_INCREMENT,
    manager_staff_id INT,
    address_id       INT            NOT NULL,
    PRIMARY KEY (store_id),
    CONSTRAINT fk_skstore_addr FOREIGN KEY (address_id) REFERENCES sk_address (address_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_staff (
    staff_id        INT             NOT NULL AUTO_INCREMENT,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    address_id      INT             NOT NULL,
    email           VARCHAR(50),
    store_id        INT             NOT NULL,
    active          SMALLINT        NOT NULL DEFAULT 1,
    username        VARCHAR(16)     NOT NULL,
    password        VARCHAR(40),
    PRIMARY KEY (staff_id),
    CONSTRAINT fk_skstaff_addr FOREIGN KEY (address_id) REFERENCES sk_address (address_id),
    CONSTRAINT fk_skstaff_store FOREIGN KEY (store_id) REFERENCES sk_store (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_customer (
    customer_id     INT             NOT NULL AUTO_INCREMENT,
    store_id        INT             NOT NULL,
    first_name      VARCHAR(45)     NOT NULL,
    last_name       VARCHAR(45)     NOT NULL,
    email           VARCHAR(50),
    address_id      INT             NOT NULL,
    active          SMALLINT        NOT NULL DEFAULT 1,
    create_date     DATETIME        NOT NULL,
    PRIMARY KEY (customer_id),
    KEY idx_sk_cust_lname (last_name),
    CONSTRAINT fk_skcust_store FOREIGN KEY (store_id) REFERENCES sk_store (store_id),
    CONSTRAINT fk_skcust_addr FOREIGN KEY (address_id) REFERENCES sk_address (address_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_inventory (
    inventory_id    INT             NOT NULL AUTO_INCREMENT,
    film_id         INT             NOT NULL,
    store_id        INT             NOT NULL,
    PRIMARY KEY (inventory_id),
    KEY idx_sk_inv_film (film_id),
    KEY idx_sk_inv_store_film (store_id, film_id),
    CONSTRAINT fk_skinv_film FOREIGN KEY (film_id) REFERENCES sk_film (film_id),
    CONSTRAINT fk_skinv_store FOREIGN KEY (store_id) REFERENCES sk_store (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_rental (
    rental_id       INT             NOT NULL AUTO_INCREMENT,
    rental_date     DATETIME        NOT NULL,
    inventory_id    INT             NOT NULL,
    customer_id     INT             NOT NULL,
    return_date     DATETIME,
    staff_id        INT             NOT NULL,
    PRIMARY KEY (rental_id),
    KEY idx_sk_rental_inv (inventory_id),
    KEY idx_sk_rental_cust (customer_id),
    CONSTRAINT fk_skrent_inv FOREIGN KEY (inventory_id) REFERENCES sk_inventory (inventory_id),
    CONSTRAINT fk_skrent_cust FOREIGN KEY (customer_id) REFERENCES sk_customer (customer_id),
    CONSTRAINT fk_skrent_staff FOREIGN KEY (staff_id) REFERENCES sk_staff (staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sk_payment (
    payment_id      INT             NOT NULL AUTO_INCREMENT,
    customer_id     INT             NOT NULL,
    staff_id        INT             NOT NULL,
    rental_id       INT,
    amount          DECIMAL(7,2)    NOT NULL,
    payment_date    DATETIME        NOT NULL,
    PRIMARY KEY (payment_id),
    CONSTRAINT fk_skpay_cust FOREIGN KEY (customer_id) REFERENCES sk_customer (customer_id),
    CONSTRAINT fk_skpay_staff FOREIGN KEY (staff_id) REFERENCES sk_staff (staff_id),
    CONSTRAINT fk_skpay_rental FOREIGN KEY (rental_id) REFERENCES sk_rental (rental_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
