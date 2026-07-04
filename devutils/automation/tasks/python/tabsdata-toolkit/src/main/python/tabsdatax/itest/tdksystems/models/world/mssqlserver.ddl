-- =============================================================
-- Microsoft SQL Server DDL — World
-- =============================================================

CREATE TABLE wld_country (
    code             CHAR(3)         NOT NULL PRIMARY KEY,
    name             VARCHAR(52)     NOT NULL,
    continent        VARCHAR(20)     NOT NULL CHECK (continent IN ('Asia','Europe','North America','Africa','Oceania','Antarctica','South America')),
    region           VARCHAR(26)     NOT NULL,
    surface_area     DECIMAL(10,2)   NOT NULL,
    indep_year       SMALLINT,
    population       INT             NOT NULL,
    life_expectancy  DECIMAL(3,1),
    gnp              DECIMAL(10,2),
    gnp_old          DECIMAL(10,2),
    local_name       VARCHAR(45)     NOT NULL,
    government_form  VARCHAR(45)     NOT NULL,
    head_of_state    VARCHAR(60),
    capital          INT,
    code2            CHAR(2)         NOT NULL
);

CREATE TABLE wld_city (
    id               INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name             VARCHAR(35)     NOT NULL,
    country_code     CHAR(3)         NOT NULL REFERENCES wld_country (code),
    district         VARCHAR(20),
    population       INT             NOT NULL
);

CREATE INDEX ix_wldcity_cc ON wld_city (country_code);

CREATE TABLE wld_country_language (
    country_code     CHAR(3)         NOT NULL REFERENCES wld_country (code),
    language         VARCHAR(30)     NOT NULL,
    is_official      CHAR(1)         NOT NULL DEFAULT 'F' CHECK (is_official IN ('T','F')),
    percentage       DECIMAL(4,1)    NOT NULL,
    PRIMARY KEY (country_code, language)
);
