-- =============================================================
-- Oracle DDL — World
-- =============================================================

CREATE TABLE country (
    code             CHAR(3)         PRIMARY KEY,
    name             VARCHAR2(52)    NOT NULL,
    continent        VARCHAR2(20)    NOT NULL CHECK (continent IN ('Asia','Europe','North America','Africa','Oceania','Antarctica','South America')),
    region           VARCHAR2(26)    NOT NULL,
    surface_area     NUMBER(10,2)    NOT NULL,
    indep_year       NUMBER(5),
    population       NUMBER(10)      NOT NULL,
    life_expectancy  NUMBER(3,1),
    gnp              NUMBER(10,2),
    gnp_old          NUMBER(10,2),
    local_name       VARCHAR2(45)    NOT NULL,
    government_form  VARCHAR2(45)    NOT NULL,
    head_of_state    VARCHAR2(60),
    capital          NUMBER(10),
    code2            CHAR(2)         NOT NULL
);

CREATE TABLE city (
    id               NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name             VARCHAR2(35)    NOT NULL,
    country_code     CHAR(3)         NOT NULL REFERENCES country (code),
    district         VARCHAR2(30),
    population       NUMBER(10)      NOT NULL
);

CREATE INDEX ix_wldcity_cc ON city (country_code);

CREATE TABLE country_language (
    country_code     CHAR(3)         NOT NULL REFERENCES country (code),
    language         VARCHAR2(30)    NOT NULL,
    is_official      CHAR(1)         DEFAULT 'F' NOT NULL CHECK (is_official IN ('T','F')),
    percentage       NUMBER(4,1)     NOT NULL,
    PRIMARY KEY (country_code, language)
);
