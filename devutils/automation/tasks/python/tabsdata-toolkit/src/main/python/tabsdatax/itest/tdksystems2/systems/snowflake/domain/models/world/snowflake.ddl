-- =============================================================
-- Snowflake SQL DDL — World
-- Targets a Snowflake schema (database.schema).
-- PK/FK constraints are informational on Snowflake; we keep
-- AUTOINCREMENT identity columns and index-free DDL (no CREATE INDEX).
-- =============================================================

CREATE TABLE country (
    code             STRING          NOT NULL,
    name             STRING          NOT NULL,
    continent        STRING          NOT NULL,
    region           STRING          NOT NULL,
    surface_area     DECIMAL(10,2)   NOT NULL,
    indep_year       SMALLINT,
    population       INT             NOT NULL,
    life_expectancy  DECIMAL(3,1),
    gnp              DECIMAL(10,2),
    gnp_old          DECIMAL(10,2),
    local_name       STRING          NOT NULL,
    government_form  STRING          NOT NULL,
    head_of_state    STRING,
    capital          INT,
    code2            STRING          NOT NULL
);

CREATE TABLE city (
    id               BIGINT          AUTOINCREMENT,
    name             STRING          NOT NULL,
    country_code     STRING          NOT NULL,
    district         STRING,
    population       INT             NOT NULL
);

CREATE TABLE country_language (
    country_code     STRING          NOT NULL,
    language         STRING          NOT NULL,
    is_official      STRING          NOT NULL,
    percentage       DECIMAL(4,1)    NOT NULL
);
