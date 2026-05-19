//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum DbError {
    #[error("Tabsdata database cannot be opened in the specified database engine: '{0}'")]
    UnsupportedDatabase(String) = 0,
    #[error("Max connections must be greater than 0. Current: '{0}'")]
    InvalidMaxConnections(u32) = 1,
    #[error(
        "Max connections must be greater than min connections. Current max: '{0}'. Current min: '{1}'"
    )]
    InvalidConnections(u32, u32) = 2,
    #[error("Tabsdata database schema has not be created. It must be created")]
    DatabaseSchemaDoesNotExist = 5000,
    #[error("Tabsdata database must be upgraded: {0}")]
    DatabaseNeedsUpgrade(String) = 5001,
    #[error(
        "Tabsdata database belongs to major version '{0}' and cannot be opened by major version '{1}'. \
         Cross-major upgrade is not supported; a fresh database is required."
    )]
    IncompatibleMajorVersion(usize, usize) = 5017,
    #[error(
        "Tabsdata database version is '{0}', binary database version is '{1}'. Binary must be upgraded"
    )]
    DatabaseIsNewer(String, usize) = 5002,
    #[error("Tabsdata database corrupted. {0}")]
    DatabaseCorrupted(String) = 5003,
    #[error("Database location is missing in the given configuration")]
    MissingDatabaseLocation = 5004,
    #[error("Failed to connect to the database: {0}")]
    FailedToConnectToDatabase(#[from] sqlx::Error) = 5007,
    #[error("Failed to create or upgrade the database: {0}")]
    FailedToCreateOrUpgradeDatabaseSchema(#[source] sqlx::Error) = 5008,
    #[error("Sql error: {0}")]
    SqlError(#[source] sqlx::Error) = 5009,
    #[error("Failed to create database connection URL: {0}")]
    DatabaseInvalidUrl(String) = 5013,
    #[error(
        "Tabsdata instance edition is '{0}', and binary edition is '{1}'. This instance cannot run with '{1}' edition"
    )]
    InvalidEdition(String, String) = 5014,
    #[error("Failed to create or upgrade the database tabsdata edition: {0}")]
    FailedToCreateOrUpgradeDatabaseEdition(#[source] sqlx::Error) = 5015,
    #[error("Failed to upgrade the database tabsdata edition: {0}")]
    CannotUpgradeEdition(String) = 5016,
    #[error("Failed to start embedded postgres: {0}")]
    EmbeddedPostgres(String) = 5018,
}
