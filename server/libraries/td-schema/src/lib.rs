//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::Iden;

pub mod schema;

pub use schema::{CurrentSource, SchemaSource, tabsdata_system_ddl};

/// Tabsdata system table stucture.
#[derive(Iden)]
pub enum TabsdataSystem {
    Table,
    Name,
    Value,
}

/// The name of the system table key for the schema counter within the
/// current major version.
pub const DB_VERSION_NAME: &str = "db_version";
/// The name of the system table key for the Tabsdata major-product version
/// the database was created under. Gate-checked at startup; databases without
/// this row are treated as `1` (pre-2.0) and refused.
pub const TABSDATA_VERSION_NAME: &str = "tabsdata_version";
pub const DB_EDITION_NAME: &str = "edition";

/// Tabsdata major-product version this binary speaks.
///
/// Bump only when cutting a new major (2.0 → 3.0). Within a major, schema
/// evolution happens via `db_version` and delta modules. Cross-major upgrade
/// is not supported; mismatches fail loudly at startup.
pub const CURRENT_TABSDATA_VERSION: usize = 2;

/// Schema counter within the current major version. Resets to 1 at each
/// major bump.
pub const CURRENT_VERSION: usize = <schema::current::Schema as schema::SchemaVersion>::VERSION;
