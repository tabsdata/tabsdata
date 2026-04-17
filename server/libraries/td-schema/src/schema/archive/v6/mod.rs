//
// Copyright 2026 Tabs Data Inc.
//

//! Frozen schema snapshot for version 6.

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

/// Marker type for the archived v6 schema.
pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 6;

    const FINGERPRINTS: [&'static str; 3] = [
        "bdb6b7eb97700271e60c9fec66f8bcc49a82a21744dbd4bbcd9d361ac57fe9b4", // Sqlite
        "9a9b7edbb38d634b8cf704620570f3dfbef3f3a752e5f97c90ee10b028f24200", // Postgres
        "10ec0a938adb9e039fee3f29b8560f7bb22e120749819da631b6c7ceee1ce7b5", // MySql
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

        // 2. Views (order respects view dependencies)
        let before = stmts.len();
        for view in system::views(&backend)
            .iter()
            .chain(users::views(&backend).iter())
            .chain(collections::views(&backend).iter())
            .chain(sessions::views(&backend).iter())
            .chain(roles::views(&backend).iter())
            .chain(permissions::views(&backend).iter())
            .chain(functions::views(&backend).iter())
        {
            stmts.push(backend.create_view(view));
        }
        debug!(count = stmts.len() - before, "Views created");

        // 3. Indices
        let before = stmts.len();
        for idx in indices::all() {
            stmts.push(backend.build_schema(idx));
        }
        debug!(count = stmts.len() - before, "Indices created");

        // 4. Seed data (order matters: regular inserts before INSERT...SELECT)
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
    fn test_validate_archive_v6() {
        validate_schema_version::<Schema>();
    }
}
