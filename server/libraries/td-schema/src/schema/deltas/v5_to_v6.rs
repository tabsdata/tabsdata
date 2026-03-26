//
// Copyright 2026 Tabs Data Inc.
//

//! Empty migration from v5 to v6.
//!
//! This delta exists to showcase the migration system. It contains no schema
//! changes — only the mandatory `db_version` bump.

use crate::schema::SchemaDelta;
use crate::schema::backend::Backend;
use tracing::info;

/// Marker type for the v5-to-v6 migration.
pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 5;
    const TO_VERSION: usize = 6;
    const FINGERPRINTS: [&'static str; 3] = [
        "d7967eeb5fd886fddae36e8097aa930958290b9832c70622e63524ea68121f19", // Sqlite
        "d7967eeb5fd886fddae36e8097aa930958290b9832c70622e63524ea68121f19", // Postgres
        "d7967eeb5fd886fddae36e8097aa930958290b9832c70622e63524ea68121f19", // MySql
    ];

    fn up(b: Backend) -> Vec<String> {
        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            backend = ?b,
            "Applying migration"
        );
        let stmts = vec![
            // No schema changes in this version.
            // The only statement is the mandatory version bump, due to migration system change.
            b.update_db_version(Self::TO_VERSION),
        ];
        info!(
            from = Self::FROM_VERSION,
            to = Self::TO_VERSION,
            statements = stmts.len(),
            "Migration built"
        );
        stmts
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v5_to_v6() {
        validate_delta::<Migration>();
    }
}
