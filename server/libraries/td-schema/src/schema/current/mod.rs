//
// Copyright 2026 Tabs Data Inc.
//

mod collections;
pub(crate) mod functions;
mod indices;
mod logical;
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
    const VERSION: usize = 1;

    // Refresh via `cargo test -p td-schema --lib --features sqlite,postgres,mysql
    // print_schema_fingerprints -- --ignored --nocapture` after any
    // intentional change to current/.
    const FINGERPRINTS: [&'static str; 3] = [
        "31faf07fa170d05d2d938c84adbd57516915519dfb1239ea3e123f50145f2123", // Sqlite
        "90fd241c60e0fa1cb0d96f2b7a02581c51f9688853e87bfeee97aa4da5f63687", // Postgres
        "0cbb729f6723ef787215cebb89722f09772589e458f32aeb4785da8904df70e1", // MySql
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
            .chain(logical::tables())
        {
            stmts.push(backend.build_schema(table));
        }
        debug!(count = stmts.len() - before, "Tables created");

        // 2. Triggers (after tables, before views — views may read _current tables)
        let before = stmts.len();
        stmts.extend(functions::triggers(&backend));
        stmts.extend(logical::triggers(&backend));
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
