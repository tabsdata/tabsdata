//
// Copyright 2026 Tabs Data Inc.
//

//! Frozen schema snapshot for version 5.

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

/// Marker type for the archived v5 schema.
pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 5;

    const FINGERPRINTS: [&'static str; 3] = [
        "a38fb3a284a90ce994d88a094a045177bbf28dc0964d92a66887981329d98ab8", // Sqlite
        "141cf456cbdff5ed85683af916b0239b4c8bf27147df1f3ec3e812dd05b6f9e9", // Postgres
        "3d0bb60856c06e76788603c645fcc09d3b1de107f9064deb248be15ce2169b1c", // MySql
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
    fn test_validate_archive_v5() {
        validate_schema_version::<Schema>();
    }
}
