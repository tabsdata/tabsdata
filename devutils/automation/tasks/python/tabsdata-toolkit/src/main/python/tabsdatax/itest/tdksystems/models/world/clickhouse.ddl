-- =============================================================
-- ClickHouse DDL — World
-- Executed via the clickhouse-connect SQLAlchemy dialect. ClickHouse
-- has no PK/FK constraints or AUTO_INCREMENT: tables use a MergeTree
-- engine sorted by their natural key, char/varchar map to String, and
-- optional columns are Nullable(T). `wld_city.id` is a plain column
-- (the SQL loader omits IDENTITY columns, and ClickHouse can't generate
-- them), so it is not used as the sort key.
-- =============================================================

CREATE TABLE wld_country (
    code             String,
    name             String,
    continent        String,
    region           String,
    surface_area     Decimal(10, 2),
    indep_year       Nullable(Int16),
    population       Int32,
    life_expectancy  Nullable(Decimal(3, 1)),
    gnp              Nullable(Decimal(10, 2)),
    gnp_old          Nullable(Decimal(10, 2)),
    local_name       String,
    government_form  String,
    head_of_state    Nullable(String),
    capital          Nullable(Int32),
    code2            String
)
ENGINE = MergeTree
ORDER BY code;

CREATE TABLE wld_city (
    id               Int32,
    name             String,
    country_code     String,
    district         String,
    population       Int32
)
ENGINE = MergeTree
ORDER BY (country_code, name);

CREATE TABLE wld_country_language (
    country_code     String,
    language         String,
    is_official      String,
    percentage       Decimal(4, 1)
)
ENGINE = MergeTree
ORDER BY (country_code, language);
