-- =============================================================
-- PostgreSQL DDL — Airport DB
-- =============================================================

CREATE TABLE airport (
    airport_id      SERIAL          PRIMARY KEY,
    iata            CHAR(3),
    icao            CHAR(4)         NOT NULL UNIQUE,
    name            VARCHAR(80)     NOT NULL
);
CREATE INDEX ix_ap_airport_iata ON airport (iata);
CREATE INDEX ix_ap_airport_name ON airport (name);

CREATE TABLE airport_geo (
    airport_id      INT             PRIMARY KEY REFERENCES airport (airport_id),
    name            VARCHAR(80)     NOT NULL,
    city            VARCHAR(80),
    country         VARCHAR(80),
    latitude        NUMERIC(11,8)   NOT NULL,
    longitude       NUMERIC(11,8)   NOT NULL
);

CREATE TABLE airline (
    airline_id      SERIAL          PRIMARY KEY,
    iata            CHAR(2)         NOT NULL UNIQUE,
    airline_name    VARCHAR(50),
    base_airport    INT             NOT NULL REFERENCES airport (airport_id)
);
CREATE INDEX ix_ap_airline_base ON airline (base_airport);

CREATE TABLE airplane_type (
    type_id         SERIAL          PRIMARY KEY,
    identifier      VARCHAR(50),
    description     TEXT
);

CREATE TABLE airplane (
    airplane_id     SERIAL          PRIMARY KEY,
    capacity        INT             NOT NULL,
    type_id         INT             NOT NULL REFERENCES airplane_type (type_id),
    airline_id      INT             NOT NULL REFERENCES airline (airline_id)
);
CREATE INDEX ix_ap_airplane_type    ON airplane (type_id);
CREATE INDEX ix_ap_airplane_airline ON airplane (airline_id);

CREATE TABLE employee (
    employee_id     SERIAL          PRIMARY KEY,
    firstname       VARCHAR(100)    NOT NULL,
    lastname        VARCHAR(100)    NOT NULL,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR(100)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    zip             INT             NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    email           VARCHAR(120),
    phone           VARCHAR(30),
    salary          NUMERIC(8,2),
    department      VARCHAR(20),
    username        VARCHAR(20)     UNIQUE,
    password        CHAR(32)
);

CREATE TABLE passenger (
    passenger_id    SERIAL          PRIMARY KEY,
    passportno      CHAR(9)         NOT NULL UNIQUE,
    firstname       VARCHAR(100)    NOT NULL,
    lastname        VARCHAR(100)    NOT NULL
);

CREATE TABLE passenger_details (
    passenger_id    INT             PRIMARY KEY REFERENCES passenger (passenger_id) ON DELETE CASCADE,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR(100)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    zip             INT             NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    email           VARCHAR(120),
    phone           VARCHAR(30)
);

CREATE TABLE flight_schedule (
    flightno        CHAR(8)         PRIMARY KEY,
    from_airport    INT             NOT NULL REFERENCES airport (airport_id),
    to_airport      INT             NOT NULL REFERENCES airport (airport_id),
    departure       TIME            NOT NULL,
    arrival         TIME            NOT NULL,
    airline_id      INT             NOT NULL REFERENCES airline (airline_id),
    monday          SMALLINT        DEFAULT 0,
    tuesday         SMALLINT        DEFAULT 0,
    wednesday       SMALLINT        DEFAULT 0,
    thursday        SMALLINT        DEFAULT 0,
    friday          SMALLINT        DEFAULT 0,
    saturday        SMALLINT        DEFAULT 0,
    sunday          SMALLINT        DEFAULT 0
);
CREATE INDEX ix_apfs_from    ON flight_schedule (from_airport);
CREATE INDEX ix_apfs_to      ON flight_schedule (to_airport);
CREATE INDEX ix_apfs_airline ON flight_schedule (airline_id);

CREATE TABLE flight (
    flight_id       SERIAL          PRIMARY KEY,
    flightno        CHAR(8)         NOT NULL REFERENCES flight_schedule (flightno),
    from_airport    INT             NOT NULL REFERENCES airport (airport_id),
    to_airport      INT             NOT NULL REFERENCES airport (airport_id),
    departure       TIMESTAMP       NOT NULL,
    arrival         TIMESTAMP       NOT NULL,
    airline_id      INT             NOT NULL REFERENCES airline (airline_id),
    airplane_id     INT             NOT NULL REFERENCES airplane (airplane_id)
);
CREATE INDEX ix_apfl_from     ON flight (from_airport);
CREATE INDEX ix_apfl_to       ON flight (to_airport);
CREATE INDEX ix_apfl_dep      ON flight (departure);
CREATE INDEX ix_apfl_arr      ON flight (arrival);
CREATE INDEX ix_apfl_airline  ON flight (airline_id);
CREATE INDEX ix_apfl_airplane ON flight (airplane_id);
CREATE INDEX ix_apfl_flightno ON flight (flightno);

CREATE TABLE booking (
    booking_id      SERIAL          PRIMARY KEY,
    flight_id       INT             NOT NULL REFERENCES flight (flight_id),
    seat            CHAR(4),
    passenger_id    INT             NOT NULL REFERENCES passenger (passenger_id),
    price           NUMERIC(10,2)   NOT NULL,
    UNIQUE (flight_id, seat)
);
CREATE INDEX ix_apbk_flight ON booking (flight_id);
CREATE INDEX ix_apbk_pass   ON booking (passenger_id);

CREATE TABLE weather (
    log_date        DATE            NOT NULL,
    log_time        TIME            NOT NULL,
    station         INT             NOT NULL,
    temp            NUMERIC(4,1)    NOT NULL,
    humidity        NUMERIC(4,1)    NOT NULL,
    airpressure     NUMERIC(10,2)   NOT NULL,
    wind            NUMERIC(5,2)    NOT NULL,
    weather         VARCHAR(30),
    winddirection   SMALLINT        NOT NULL,
    PRIMARY KEY (log_date, log_time, station)
);
