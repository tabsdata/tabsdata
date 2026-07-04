-- =============================================================
-- Oracle DDL — Airport DB
-- =============================================================

CREATE TABLE ap_airport (
    airport_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iata            CHAR(3),
    icao            CHAR(4)         NOT NULL UNIQUE,
    name            VARCHAR2(80)    NOT NULL
);
CREATE INDEX ix_ap_airport_iata ON ap_airport (iata);
CREATE INDEX ix_ap_airport_name ON ap_airport (name);

CREATE TABLE ap_airport_geo (
    airport_id      NUMBER          PRIMARY KEY REFERENCES ap_airport (airport_id),
    name            VARCHAR2(80)    NOT NULL,
    city            VARCHAR2(80),
    country         VARCHAR2(80),
    latitude        NUMBER(11,8)    NOT NULL,
    longitude       NUMBER(11,8)    NOT NULL
);

CREATE TABLE ap_airline (
    airline_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iata            CHAR(2)         NOT NULL UNIQUE,
    airline_name    VARCHAR2(50),
    base_airport    NUMBER          NOT NULL REFERENCES ap_airport (airport_id)
);
CREATE INDEX ix_ap_airline_base ON ap_airline (base_airport);

CREATE TABLE ap_airplane_type (
    type_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identifier      VARCHAR2(50),
    description     CLOB
);

CREATE TABLE ap_airplane (
    airplane_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    capacity        NUMBER          NOT NULL,
    type_id         NUMBER          NOT NULL REFERENCES ap_airplane_type (type_id),
    airline_id      NUMBER          NOT NULL REFERENCES ap_airline (airline_id)
);
CREATE INDEX ix_ap_airplane_type    ON ap_airplane (type_id);
CREATE INDEX ix_ap_airplane_airline ON ap_airplane (airline_id);

CREATE TABLE ap_employee (
    employee_id     NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    firstname       VARCHAR2(100)   NOT NULL,
    lastname        VARCHAR2(100)   NOT NULL,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR2(100)   NOT NULL,
    city            VARCHAR2(100)   NOT NULL,
    zip             NUMBER          NOT NULL,
    country         VARCHAR2(100)   NOT NULL,
    email           VARCHAR2(120),
    phone           VARCHAR2(30),
    salary          NUMBER(8,2),
    department      VARCHAR2(20),
    username        VARCHAR2(20)    UNIQUE,
    password        CHAR(32)
);

CREATE TABLE ap_passenger (
    passenger_id    NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    passportno      CHAR(9)         NOT NULL UNIQUE,
    firstname       VARCHAR2(100)   NOT NULL,
    lastname        VARCHAR2(100)   NOT NULL
);

CREATE TABLE ap_passenger_details (
    passenger_id    NUMBER          PRIMARY KEY REFERENCES ap_passenger (passenger_id) ON DELETE CASCADE,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR2(100)   NOT NULL,
    city            VARCHAR2(100)   NOT NULL,
    zip             NUMBER          NOT NULL,
    country         VARCHAR2(100)   NOT NULL,
    email           VARCHAR2(120),
    phone           VARCHAR2(30)
);

CREATE TABLE ap_flight_schedule (
    flightno        CHAR(8)         PRIMARY KEY,
    from_airport    NUMBER          NOT NULL REFERENCES ap_airport (airport_id),
    to_airport      NUMBER          NOT NULL REFERENCES ap_airport (airport_id),
    departure       VARCHAR2(8)     NOT NULL,
    arrival         VARCHAR2(8)     NOT NULL,
    airline_id      NUMBER          NOT NULL REFERENCES ap_airline (airline_id),
    monday          NUMBER(1)       DEFAULT 0,
    tuesday         NUMBER(1)       DEFAULT 0,
    wednesday       NUMBER(1)       DEFAULT 0,
    thursday        NUMBER(1)       DEFAULT 0,
    friday          NUMBER(1)       DEFAULT 0,
    saturday        NUMBER(1)       DEFAULT 0,
    sunday          NUMBER(1)       DEFAULT 0
);
CREATE INDEX ix_apfs_from    ON ap_flight_schedule (from_airport);
CREATE INDEX ix_apfs_to      ON ap_flight_schedule (to_airport);
CREATE INDEX ix_apfs_airline ON ap_flight_schedule (airline_id);

CREATE TABLE ap_flight (
    flight_id       NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    flightno        CHAR(8)         NOT NULL REFERENCES ap_flight_schedule (flightno),
    from_airport    NUMBER          NOT NULL REFERENCES ap_airport (airport_id),
    to_airport      NUMBER          NOT NULL REFERENCES ap_airport (airport_id),
    departure       TIMESTAMP       NOT NULL,
    arrival         TIMESTAMP       NOT NULL,
    airline_id      NUMBER          NOT NULL REFERENCES ap_airline (airline_id),
    airplane_id     NUMBER          NOT NULL REFERENCES ap_airplane (airplane_id)
);
CREATE INDEX ix_apfl_from     ON ap_flight (from_airport);
CREATE INDEX ix_apfl_to       ON ap_flight (to_airport);
CREATE INDEX ix_apfl_dep      ON ap_flight (departure);
CREATE INDEX ix_apfl_arr      ON ap_flight (arrival);
CREATE INDEX ix_apfl_airline  ON ap_flight (airline_id);
CREATE INDEX ix_apfl_airplane ON ap_flight (airplane_id);
CREATE INDEX ix_apfl_flightno ON ap_flight (flightno);

CREATE TABLE ap_booking (
    booking_id      NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    flight_id       NUMBER          NOT NULL REFERENCES ap_flight (flight_id),
    seat            CHAR(4),
    passenger_id    NUMBER          NOT NULL REFERENCES ap_passenger (passenger_id),
    price           NUMBER(10,2)    NOT NULL,
    CONSTRAINT uq_apbk_seat UNIQUE (flight_id, seat)
);
CREATE INDEX ix_apbk_flight ON ap_booking (flight_id);
CREATE INDEX ix_apbk_pass   ON ap_booking (passenger_id);

CREATE TABLE ap_weather (
    log_date        DATE            NOT NULL,
    log_time        VARCHAR2(8)     NOT NULL,
    station         NUMBER          NOT NULL,
    temp            NUMBER(4,1)     NOT NULL,
    humidity        NUMBER(4,1)     NOT NULL,
    airpressure     NUMBER(10,2)    NOT NULL,
    wind            NUMBER(5,2)     NOT NULL,
    weather         VARCHAR2(30),
    winddirection   NUMBER(5)       NOT NULL,
    PRIMARY KEY (log_date, log_time, station)
);
