-- =============================================================
-- MySQL DDL — World
-- Countries, cities, and spoken languages
-- =============================================================

CREATE TABLE wld_country (
    code             CHAR(3)         NOT NULL,
    name             VARCHAR(52)     NOT NULL,
    continent        ENUM('Asia','Europe','North America','Africa','Oceania','Antarctica','South America') NOT NULL,
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
    code2            CHAR(2)         NOT NULL,
    PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE wld_city (
    id               INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    name             VARCHAR(35)     NOT NULL,
    country_code     CHAR(3)         NOT NULL,
    district         VARCHAR(20),
    population       INT             NOT NULL,
    PRIMARY KEY (id),
    KEY ix_wldcity_cc (country_code),
    CONSTRAINT fk_wldcity_country FOREIGN KEY (country_code) REFERENCES wld_country (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE wld_country_language (
    country_code     CHAR(3)         NOT NULL,
    language         VARCHAR(30)     NOT NULL,
    is_official      ENUM('T','F')   NOT NULL DEFAULT 'F',
    percentage       DECIMAL(4,1)    NOT NULL,
    PRIMARY KEY (country_code, language),
    CONSTRAINT fk_wldlang_country FOREIGN KEY (country_code) REFERENCES wld_country (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
