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
        "68d13fea51a8d5b3b020e784714cf1a17c2f2dfd8b692c6e9484750e66e386db", // Sqlite
        "214722cf93be668a0010aecaf9e2d0af3978cf81cd9801dbc6756c4a78d9e2b5", // Postgres
        "bd872226d84a50d9b4303f4aabe6dc460b8f54861dcdc672fff488b24944ce1b", // MySql
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
