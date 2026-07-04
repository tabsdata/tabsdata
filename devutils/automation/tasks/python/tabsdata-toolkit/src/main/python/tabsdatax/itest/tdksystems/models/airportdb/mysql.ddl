-- =============================================================
-- MySQL DDL — Airport DB (Flughafen DB)
-- Airlines, airports, planes, schedules, flights, bookings,
-- passengers, employees and weather data.
-- =============================================================

CREATE TABLE ap_airport (
    airport_id      INT             NOT NULL AUTO_INCREMENT,
    iata            CHAR(3),
    icao            CHAR(4)         NOT NULL,
    name            VARCHAR(80)     NOT NULL,
    PRIMARY KEY (airport_id),
    UNIQUE KEY uq_ap_airport_icao (icao),
    KEY ix_ap_airport_iata (iata),
    KEY ix_ap_airport_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_airport_geo (
    airport_id      INT             NOT NULL,
    name            VARCHAR(80)     NOT NULL,
    city            VARCHAR(80),
    country         VARCHAR(80),
    latitude        DECIMAL(11,8)   NOT NULL,
    longitude       DECIMAL(11,8)   NOT NULL,
    PRIMARY KEY (airport_id),
    CONSTRAINT fk_apgeo_airport FOREIGN KEY (airport_id) REFERENCES ap_airport (airport_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_airline (
    airline_id      INT             NOT NULL AUTO_INCREMENT,
    iata            CHAR(2)         NOT NULL,
    airline_name    VARCHAR(50),
    base_airport    INT             NOT NULL,
    PRIMARY KEY (airline_id),
    UNIQUE KEY uq_ap_airline_iata (iata),
    KEY ix_ap_airline_base (base_airport),
    CONSTRAINT fk_apairl_airport FOREIGN KEY (base_airport) REFERENCES ap_airport (airport_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_airplane_type (
    type_id         INT             NOT NULL AUTO_INCREMENT,
    identifier      VARCHAR(50),
    description     TEXT,
    PRIMARY KEY (type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_airplane (
    airplane_id     INT             NOT NULL AUTO_INCREMENT,
    capacity        INT             NOT NULL,
    type_id         INT             NOT NULL,
    airline_id      INT             NOT NULL,
    PRIMARY KEY (airplane_id),
    KEY ix_ap_airplane_type (type_id),
    KEY ix_ap_airplane_airline (airline_id),
    CONSTRAINT fk_appln_type FOREIGN KEY (type_id) REFERENCES ap_airplane_type (type_id),
    CONSTRAINT fk_appln_airline FOREIGN KEY (airline_id) REFERENCES ap_airline (airline_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_employee (
    employee_id     INT             NOT NULL AUTO_INCREMENT,
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
    username        VARCHAR(20),
    password        CHAR(32),
    PRIMARY KEY (employee_id),
    UNIQUE KEY uq_ap_emp_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_passenger (
    passenger_id    INT             NOT NULL AUTO_INCREMENT,
    passportno      CHAR(9)         NOT NULL,
    firstname       VARCHAR(100)    NOT NULL,
    lastname        VARCHAR(100)    NOT NULL,
    PRIMARY KEY (passenger_id),
    UNIQUE KEY uq_ap_pass_passport (passportno)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_passenger_details (
    passenger_id    INT             NOT NULL,
    birthdate       DATE            NOT NULL,
    sex             CHAR(1),
    street          VARCHAR(100)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    zip             INT             NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    email           VARCHAR(120),
    phone           VARCHAR(30),
    PRIMARY KEY (passenger_id),
    CONSTRAINT fk_appd_pass FOREIGN KEY (passenger_id) REFERENCES ap_passenger (passenger_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_flight_schedule (
    flightno        CHAR(8)         NOT NULL,
    from_airport    INT             NOT NULL,
    to_airport      INT             NOT NULL,
    departure       TIME            NOT NULL,
    arrival         TIME            NOT NULL,
    airline_id      INT             NOT NULL,
    monday          SMALLINT        DEFAULT 0,
    tuesday         SMALLINT        DEFAULT 0,
    wednesday       SMALLINT        DEFAULT 0,
    thursday        SMALLINT        DEFAULT 0,
    friday          SMALLINT        DEFAULT 0,
    saturday        SMALLINT        DEFAULT 0,
    sunday          SMALLINT        DEFAULT 0,
    PRIMARY KEY (flightno),
    KEY ix_apfs_from (from_airport),
    KEY ix_apfs_to (to_airport),
    KEY ix_apfs_airline (airline_id),
    CONSTRAINT fk_apfs_from FOREIGN KEY (from_airport) REFERENCES ap_airport (airport_id),
    CONSTRAINT fk_apfs_to   FOREIGN KEY (to_airport)   REFERENCES ap_airport (airport_id),
    CONSTRAINT fk_apfs_airl FOREIGN KEY (airline_id)   REFERENCES ap_airline (airline_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_flight (
    flight_id       INT             NOT NULL AUTO_INCREMENT,
    flightno        CHAR(8)         NOT NULL,
    from_airport    INT             NOT NULL,
    to_airport      INT             NOT NULL,
    departure       DATETIME        NOT NULL,
    arrival         DATETIME        NOT NULL,
    airline_id      INT             NOT NULL,
    airplane_id     INT             NOT NULL,
    PRIMARY KEY (flight_id),
    KEY ix_apfl_from (from_airport),
    KEY ix_apfl_to (to_airport),
    KEY ix_apfl_dep (departure),
    KEY ix_apfl_arr (arrival),
    KEY ix_apfl_airline (airline_id),
    KEY ix_apfl_airplane (airplane_id),
    KEY ix_apfl_flightno (flightno),
    CONSTRAINT fk_apfl_from FOREIGN KEY (from_airport) REFERENCES ap_airport (airport_id),
    CONSTRAINT fk_apfl_to   FOREIGN KEY (to_airport)   REFERENCES ap_airport (airport_id),
    CONSTRAINT fk_apfl_airl FOREIGN KEY (airline_id)   REFERENCES ap_airline (airline_id),
    CONSTRAINT fk_apfl_pln  FOREIGN KEY (airplane_id)  REFERENCES ap_airplane (airplane_id),
    CONSTRAINT fk_apfl_fs   FOREIGN KEY (flightno)     REFERENCES ap_flight_schedule (flightno)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE ap_booking (
    booking_id      INT             NOT NULL AUTO_INCREMENT,
    flight_id       INT             NOT NULL,
    seat            CHAR(4),
    passenger_id    INT             NOT NULL,
    price           DECIMAL(10,2)   NOT NULL,
    PRIMARY KEY (booking_id),
    UNIQUE KEY uq_apbk_seat (flight_id, seat),
    KEY ix_apbk_flight (flight_id),
    KEY ix_apbk_pass (passenger_id),
    CONSTRAINT fk_apbk_flight FOREIGN KEY (flight_id) REFERENCES ap_flight (flight_id),
    CONSTRAINT fk_apbk_pass   FOREIGN KEY (passenger_id) REFERENCES ap_passenger (passenger_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
