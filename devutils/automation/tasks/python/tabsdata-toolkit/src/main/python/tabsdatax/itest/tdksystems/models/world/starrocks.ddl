-- =============================================================
-- StarRocks DDL — World
-- Speaks the MySQL protocol (pymysql dialect), but uses the
-- StarRocks Primary Key model: key columns first, no ENGINE /
-- ENUM / FK / secondary KEY, and a DISTRIBUTED BY + replica count.
-- =============================================================

CREATE TABLE wld_country (
    code             VARCHAR(3)      NOT NULL,
    name             VARCHAR(52)     NOT NULL,
    continent        VARCHAR(20)     NOT NULL,
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
)
PRIMARY KEY (code)
DISTRIBUTED BY HASH (code) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE wld_city (
    id               BIGINT          NOT NULL AUTO_INCREMENT,
    name             VARCHAR(35)     NOT NULL,
    country_code     CHAR(3)         NOT NULL,
    district         VARCHAR(20),
    population       INT             NOT NULL
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE wld_country_language (
    country_code     VARCHAR(3)      NOT NULL,
    language         VARCHAR(30)     NOT NULL,
    is_official      CHAR(1)         NOT NULL DEFAULT 'F',
    percentage       DECIMAL(4,1)    NOT NULL
)
PRIMARY KEY (country_code, language)
DISTRIBUTED BY HASH (country_code, language) BUCKETS 1
PROPERTIES ("replication_num" = "1");
