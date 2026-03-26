//
// Copyright 2026 Tabs Data Inc.
//

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!("You must enable at least one database feature: `sqlite`, `postgres` or `mysql`.");

use sea_query::Iden;

pub mod schema;

/// Tabsdata system table stucture.
#[derive(Iden)]
pub enum TabsdataSystem {
    Table,
    Name,
    Value,
}

/// The name of the system table key for database version.
pub const DB_VERSION_NAME: &str = "db_version";
pub const DB_EDITION_NAME: &str = "edition";

/// Version that the current schema expects the database to be at.
pub const CURRENT_VERSION: usize = <schema::current::Schema as schema::SchemaVersion>::VERSION;
