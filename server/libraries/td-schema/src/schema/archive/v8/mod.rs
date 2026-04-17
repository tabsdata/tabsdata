//
// Copyright 2026 Tabs Data Inc.
//

//! Frozen schema snapshot for version 8.

mod collections;
mod functions;
mod indices;
mod permissions;
mod roles;
mod sessions;
mod system;
mod users;

use crate::schema::SchemaVersion;
use crate::schema::backend::Backend;
use tracing::{debug, info};

/// Marker type for the archived v8 schema.
pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 8;

    const FINGERPRINTS: [&'static str; 3] = [
        "0df576d595479aaf01f9f6eab0720eaa25c9cc64fdac4c079fd5d896f08193ff", // Sqlite
        "0680c535e3c170835912d651b6a2de2767d11c0c70c55416ee297e4134c814a7", // Postgres
        "c61a18085f25f33dd32c3f030896e6bb91b22e59b3c69376a8152aa9fe3fe61c", // MySql
    ];

    fn build(backend: impl Into<Backend>) -> Vec<String> {
        let backend = backend.into();
        info!(version = Self::VERSION, backend = ?backend, "Building archived schema");
        let mut stmts = Vec::new();

        // 1. Tables (order respects foreign key dependencies)
        let before = stmts.len();
        for table in system::tables()
            .into_iter()
            .chain(users::tables())
            .chain(collections::tables())
            .chain(roles::tables())
            .chain(functions::tables())
            .chain(sessions::tables())
            .chain(permissions::tables())
        {
            stmts.push(backend.build_schema(table));
        }
        debug!(count = stmts.len() - before, "Tables created");

        // 2. Triggers (after tables, before views — views may read _current tables)
        let before = stmts.len();
        stmts.extend(functions::triggers(&backend));
        debug!(count = stmts.len() - before, "Triggers created");

        // 3. Views (order respects view dependencies)
        let before = stmts.len();
        for view in system::views(&backend)
            .iter()
            .chain(users::views(&backend).iter())
            .chain(collections::views(&backend).iter())
            .chain(sessions::views(&backend).iter())
            .chain(roles::views(&backend).iter())
            .chain(permissions::views(&backend).iter())
            .chain(functions::views(&backend).iter())
            .chain(functions::current_views(&backend).iter())
        {
            stmts.push(backend.create_view(view));
        }
        debug!(count = stmts.len() - before, "Views created");

        // 4. Indices
        let before = stmts.len();
        for idx in indices::all() {
            stmts.push(backend.build_schema(idx));
        }
        debug!(count = stmts.len() - before, "Indices created");

        // 5. Seed data (order matters: regular inserts before INSERT...SELECT)
        let before = stmts.len();
        for insert in system::seed()
            .into_iter()
            .chain(users::seed())
            .chain(collections::seed())
            .chain(roles::seed())
            .chain(sessions::seed())
            .chain(permissions::seed())
            .chain(functions::seed())
            .chain(roles::seed_insert_select())
            .chain(permissions::seed_insert_select())
        {
            stmts.push(backend.build_query(insert));
        }
        // Seed _current tables from existing data (INSERT...SELECT with ROW_NUMBER CTE)
        stmts.extend(functions::seed_current(&backend));
        debug!(count = stmts.len() - before, "Seed rows inserted");

        info!(
            version = Self::VERSION,
            total = stmts.len(),
            "Archived schema built"
        );
        stmts
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use crate::schema::tests::validate_schema_version;

    #[test]
    fn test_validate_archive_v8() {
        validate_schema_version::<Schema>();
    }
}
