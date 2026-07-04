-- =============================================================
-- Microsoft SQL Server DDL — Airport DB
-- =============================================================

CREATE TABLE ap_airport (
    airport_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    iata            CHAR(3),
    icao            CHAR(4)         NOT NULL UNIQUE,
    name            VARCHAR(80)     NOT NULL
);
CREATE INDEX ix_ap_airport_iata ON ap_airport (iata);
CREATE INDEX ix_ap_airport_name ON ap_airport (name);

CREATE TABLE ap_airport_geo (
    airport_id      INT             NOT NULL PRIMARY KEY REFERENCES ap_airport (airport_id),
    name            VARCHAR(80)     NOT NULL,
    city            VARCHAR(80),
    country         VARCHAR(80),
    latitude        DECIMAL(11,8)   NOT NULL,
    longitude       DECIMAL(11,8)   NOT NULL
);

CREATE TABLE ap_airline (
    airline_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    iata            CHAR(2)         NOT NULL UNIQUE,
    airline_name    VARCHAR(50),
    base_airport    INT             NOT NULL REFERENCES ap_airport (airport_id)
);
CREATE INDEX ix_ap_airline_base ON ap_airline (base_airport);

CREATE TABLE ap_airplane_type (
    type_id         INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    identifier      VARCHAR(50),
    description     NVARCHAR(MAX)
);

CREATE TABLE ap_airplane (
    airplane_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    capacity        INT             NOT NULL,
    type_id         INT             NOT NULL REFERENCES ap_airplane_type (type_id),
    airline_id      INT             NOT NULL REFERENCES ap_airline (airline_id)
);
CREATE INDEX ix_ap_airplane_type    ON ap_airplane (type_id);
CREATE INDEX ix_ap_airplane_airline ON ap_airplane (airline_id);

CREATE TABLE ap_employee (
    employee_id     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
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
    salary          DECIMAL(8,2),
    department      VARCHAR(20),
    username        VARCHAR(20)     UNIQUE,
    password        CHAR(32)
);

CREATE TABLE ap_passenger (
    passenger_id    INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    passportno      CHAR(9)         NOT NULL UNIQUE,
    firstname       VARCHAR(100)    NOT NULL,
    lastname        VARCHAR(100)    NOT NULL
);

CREATE TABLE ap_passenger_details (
    passenger_id    INT             NOT NULL PRIMARY KEY REFERENCES ap_passenger (passenger_id) ON DELETE CASCADE,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR(100)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    zip             INT             NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    email           VARCHAR(120),
    phone           VARCHAR(30)
);

CREATE TABLE ap_flight_schedule (
    flightno        CHAR(8)         NOT NULL PRIMARY KEY,
    from_airport    INT             NOT NULL REFERENCES ap_airport (airport_id),
    to_airport      INT             NOT NULL REFERENCES ap_airport (airport_id),
    departure       TIME            NOT NULL,
    arrival         TIME            NOT NULL,
    airline_id      INT             NOT NULL REFERENCES ap_airline (airline_id),
    monday          SMALLINT        DEFAULT 0,
    tuesday         SMALLINT        DEFAULT 0,
    wednesday       SMALLINT        DEFAULT 0,
    thursday        SMALLINT        DEFAULT 0,
    friday          SMALLINT        DEFAULT 0,
    saturday        SMALLINT        DEFAULT 0,
    sunday          SMALLINT        DEFAULT 0
);
CREATE INDEX ix_apfs_from    ON ap_flight_schedule (from_airport);
CREATE INDEX ix_apfs_to      ON ap_flight_schedule (to_airport);
CREATE INDEX ix_apfs_airline ON ap_flight_schedule (airline_id);

CREATE TABLE ap_flight (
    flight_id       INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    flightno        CHAR(8)         NOT NULL REFERENCES ap_flight_schedule (flightno),
    from_airport    INT             NOT NULL REFERENCES ap_airport (airport_id),
    to_airport      INT             NOT NULL REFERENCES ap_airport (airport_id),
    departure       DATETIME2       NOT NULL,
    arrival         DATETIME2       NOT NULL,
    airline_id      INT             NOT NULL REFERENCES ap_airline (airline_id),
    airplane_id     INT             NOT NULL REFERENCES ap_airplane (airplane_id)
);
CREATE INDEX ix_apfl_from     ON ap_flight (from_airport);
CREATE INDEX ix_apfl_to       ON ap_flight (to_airport);
CREATE INDEX ix_apfl_dep      ON ap_flight (departure);
CREATE INDEX ix_apfl_arr      ON ap_flight (arrival);
CREATE INDEX ix_apfl_airline  ON ap_flight (airline_id);
CREATE INDEX ix_apfl_airplane ON ap_flight (airplane_id);
CREATE INDEX ix_apfl_flightno ON ap_flight (flightno);

CREATE TABLE ap_booking (
    booking_id      INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    flight_id       INT             NOT NULL REFERENCES ap_flight (flight_id),
    seat            CHAR(4),
    passenger_id    INT             NOT NULL REFERENCES ap_passenger (passenger_id),
    price           DECIMAL(10,2)   NOT NULL,
    CONSTRAINT uq_apbk_seat UNIQUE (flight_id, seat)
);
CREATE INDEX ix_apbk_flight ON ap_booking (flight_id);
CREATE INDEX ix_apbk_pass   ON ap_booking (passenger_id);

CREATE TABLE ap_weather (
    log_date        DATE            NOT NULL,
    log_time        TIME            NOT NULL,
    station         INT             NOT NULL,
    temp            DECIMAL(4,1)    NOT NULL,
    humidity        DECIMAL(4,1)    NOT NULL,
    airpressure     DECIMAL(10,2)   NOT NULL,
    wind            DECIMAL(5,2)    NOT NULL,
    weather         VARCHAR(30),
    winddirection   SMALLINT        NOT NULL,
    PRIMARY KEY (log_date, log_time, station)
);
