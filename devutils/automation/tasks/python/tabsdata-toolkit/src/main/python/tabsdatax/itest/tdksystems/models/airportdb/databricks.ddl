-- =============================================================
-- Databricks SQL DDL — Airport DB
-- Targets a Unity-Catalog schema; tables default to Delta.
-- PK/FK constraints are informational on Databricks; we keep
-- IDENTITY columns and indexes-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE ap_airport (
    airport_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    iata            STRING,
    icao            STRING          NOT NULL,
    name            STRING          NOT NULL
);

CREATE TABLE ap_airport_geo (
    airport_id      INT             NOT NULL,
    name            STRING          NOT NULL,
    city            STRING,
    country         STRING,
    latitude        DECIMAL(11,8)   NOT NULL,
    longitude       DECIMAL(11,8)   NOT NULL
);

CREATE TABLE ap_airline (
    airline_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    iata            STRING          NOT NULL,
    airline_name    STRING,
    base_airport    INT             NOT NULL
);

CREATE TABLE ap_airplane_type (
    type_id         BIGINT          GENERATED ALWAYS AS IDENTITY,
    identifier      STRING,
    description     STRING
);

CREATE TABLE ap_airplane (
    airplane_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    capacity        INT             NOT NULL,
    type_id         INT             NOT NULL,
    airline_id      INT             NOT NULL
);

CREATE TABLE ap_employee (
    employee_id     BIGINT          GENERATED ALWAYS AS IDENTITY,
    firstname       STRING          NOT NULL,
    lastname        STRING          NOT NULL,
    birthdate       DATE            NOT NULL,
    sex             STRING,
    street          STRING          NOT NULL,
    city            STRING          NOT NULL,
    zip             INT             NOT NULL,
    country         STRING          NOT NULL,
    email           STRING,
    phone           STRING,
    salary          DECIMAL(8,2),
    department      STRING,
    username        STRING,
    password        STRING
);

CREATE TABLE ap_passenger (
    passenger_id    BIGINT          GENERATED ALWAYS AS IDENTITY,
    passportno      STRING          NOT NULL,
    firstname       STRING          NOT NULL,
    lastname        STRING          NOT NULL
);

CREATE TABLE ap_passenger_details (
    passenger_id    INT             NOT NULL,
    birthdate       DATE            NOT NULL,
    sex             STRING,
    street          STRING          NOT NULL,
    city            STRING          NOT NULL,
    zip             INT             NOT NULL,
    country         STRING          NOT NULL,
    email           STRING,
    phone           STRING
);

CREATE TABLE ap_flight_schedule (
    flightno        STRING          NOT NULL,
    from_airport    INT             NOT NULL,
    to_airport      INT             NOT NULL,
    departure       STRING          NOT NULL,
    arrival         STRING          NOT NULL,
    airline_id      INT             NOT NULL,
    monday          SMALLINT,
    tuesday         SMALLINT,
    wednesday       SMALLINT,
    thursday        SMALLINT,
    friday          SMALLINT,
    saturday        SMALLINT,
    sunday          SMALLINT
);

CREATE TABLE ap_flight (
    flight_id       BIGINT          GENERATED ALWAYS AS IDENTITY,
    flightno        STRING          NOT NULL,
    from_airport    INT             NOT NULL,
    to_airport      INT             NOT NULL,
    departure       TIMESTAMP       NOT NULL,
    arrival         TIMESTAMP       NOT NULL,
    airline_id      INT             NOT NULL,
    airplane_id     INT             NOT NULL
);

CREATE TABLE ap_booking (
    booking_id      BIGINT          GENERATED ALWAYS AS IDENTITY,
    flight_id       INT             NOT NULL,
    seat            STRING,
    passenger_id    INT             NOT NULL,
    price           DECIMAL(10,2)   NOT NULL
);

CREATE TABLE ap_weather (
    log_date        DATE            NOT NULL,
    log_time        STRING          NOT NULL,
    station         INT             NOT NULL,
    temp            DECIMAL(4,1)    NOT NULL,
    humidity        DECIMAL(4,1)    NOT NULL,
    airpressure     DECIMAL(10,2)   NOT NULL,
    wind            DECIMAL(5,2)    NOT NULL,
    weather         STRING,
    winddirection   SMALLINT        NOT NULL
);
