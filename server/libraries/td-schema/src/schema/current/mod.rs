//
// Copyright 2026 Tabs Data Inc.
//

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

/// Marker type for the current schema version.
pub struct Schema;

impl SchemaVersion for Schema {
    const VERSION: usize = 8;

    const FINGERPRINTS: [&'static str; 3] = [
        "d5828e880eea828629d6f24116e0411f3860a552804cc0fd21c805e9b22a2b33", // Sqlite
        "69dd3a9f81f1818b94050db8ebb92b55b176035219c45bd3763480662eaae0c6", // Postgres
        "513948fb1063049b2f5412f9f7e7748e7811c13f1ee27b96cec4b75b467faaa5", // MySql
    ];

    fn build(backend: impl Into<Backend>) -> Vec<String> {
        let backend = backend.into();
        info!(version = Self::VERSION, backend = ?backend, "Building schema");
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

        info!(version = Self::VERSION, total = stmts.len(), "Schema built");
        stmts
    }
}

/// Build the complete current schema as a list of SQL statements for the given backend.
///
/// Delegates to [`Schema::build()`] via the [`SchemaVersion`] trait.
pub fn build(b: Backend) -> Vec<String> {
    Schema::build(b)
}
