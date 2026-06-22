//
// Copyright 2026 Tabs Data Inc.
//

mod bundles;
mod collections;
mod connections;
mod dependencies;
mod dependencies_current;
mod executions;
mod function_patches;
mod function_patches_current;
mod functions;
mod functions_current;
mod groups;
mod logical_function_run_inputs;
mod logical_function_run_pointers;
mod logical_function_runs;
mod logical_table_data_version_pointers;
mod logical_table_data_versions;
mod permissions;
mod physical_function_runs;
mod physical_table_data_versions;
mod projects;
mod roles;
mod sessions;
mod system;
mod table_partitions;
mod tables;
mod tables_current;
mod transactions;
mod triggers;
mod triggers_current;
mod users;
mod users_roles;
mod with_status_views;
mod workers;

use tracing::{debug, info};

use crate::schema::backend::Backend;
use crate::schema::sql;
use crate::schema::{Entity, SchemaVersion};

/// Marker type for the current schema version.
pub struct Schema;

/// FK-safe ordering of every entity in the current schema.
///
/// THIS is the single source of truth for entity order across every
/// phase of schema construction (CREATE TABLE / TRIGGER / VIEW / INDEX
/// / INSERT). Add a new entity by writing its file and inserting one
/// line here in the position dictated by its FK targets — every
/// referenced table must appear strictly earlier.
///
/// `with_status_views` is intentionally last: it owns the `*__with_status`
/// views that read the status-summary views from `logical_function_runs`,
/// so every base view from earlier entities is already emitted when the
/// view loop reaches it.
pub(crate) const ENTITIES: &[&dyn Entity] = &[
    &system::Defs,
    &users::Defs,
    &groups::Defs,
    &projects::Defs,
    &connections::Defs,
    &collections::Defs,
    &roles::Defs,
    &users_roles::Defs,
    &sessions::Defs,
    &permissions::Defs,
    &bundles::Defs,
    &functions::Defs,
    &functions_current::Defs,
    &function_patches::Defs,
    &function_patches_current::Defs,
    &tables::Defs,
    &tables_current::Defs,
    &dependencies::Defs,
    &dependencies_current::Defs,
    &triggers::Defs,
    &triggers_current::Defs,
    &executions::Defs,
    &transactions::Defs,
    &physical_function_runs::Defs,
    &physical_table_data_versions::Defs,
    &table_partitions::Defs,
    &workers::Defs,
    &logical_function_runs::Defs,
    &logical_function_run_pointers::Defs,
    &logical_table_data_versions::Defs,
    &logical_table_data_version_pointers::Defs,
    &logical_function_run_inputs::Defs,
    &with_status_views::Defs,
];

/// Tables that carry a `project_id` and get PostgreSQL row-level security
/// (Phase 6). These are exactly the tables to which `project_id` was added in
/// `current/`; keep this list in sync with that column. The `projects` registry
/// itself is intentionally absent — it is keyed by `id`, not `project_id`, and
/// the app never lists it (the active project arrives in the request).
///
/// `test_project_scoped_tables_match_project_id_columns` cross-checks this list
/// against the tables that actually declare a `project_id` column, so an
/// omission (which would silently skip RLS for that table) fails the build.
pub(crate) const PROJECT_SCOPED_TABLES: &[&str] = &[
    "groups",
    "connections",
    "collections",
    "bundles",
    "functions",
    "functions_current",
    "function_patches",
    "function_patches_current",
    "tables",
    "tables_current",
    "dependencies",
    "dependencies_current",
    "triggers",
    "triggers_current",
    "executions",
    "transactions",
    "physical_function_runs",
    "physical_table_data_versions",
    "table_partitions",
    "workers",
    "logical_function_runs",
    "logical_table_data_versions",
    "logical_function_run_inputs",
    "logical_function_run_pointers",
    "logical_table_data_version_pointers",
];

impl SchemaVersion for Schema {
    const VERSION: usize = 1;

    // Refresh via `cargo test -p td-schema --lib print_schema_fingerprints --
    // --ignored --nocapture` after any intentional change to current/.
    const FINGERPRINTS: [&'static str; 3] = [
        "3aa10775d7843cda975fcbd50a33d0fd9cbb6c546330ba1d72666d4af0a6e32f", // Sqlite
        "f64ac403850bb1d43692af90a86f3b224060be5b488a927d39208b9270c13bca", // Postgres
        "bc817eaa8a77e6128b147251a53deb8aaf039ca7e7ee47850bb0fb27f5b4debd", // MySql
    ];

    fn build(backend: impl Into<Backend>) -> Vec<String> {
        let b = backend.into();
        info!(version = Self::VERSION, backend = ?b, "Building schema");
        let mut out: Vec<String> = Vec::new();

        // Phase 1: CREATE TABLE — FK-safe ENTITIES order.
        let before = out.len();
        for e in ENTITIES {
            for t in e.create_tables() {
                out.push(b.build_schema(t));
            }
        }
        debug!(count = out.len() - before, "Tables created");

        // Phase 2: CREATE TRIGGER — after tables, before views.
        let before = out.len();
        for e in ENTITIES {
            for spec in e.create_db_triggers() {
                out.extend(spec.render(&b));
            }
        }
        debug!(count = out.len() - before, "Triggers created");

        // Phase 3: CREATE VIEW — single ENTITIES-order pass. Cross-view
        // dependencies are isolated in `with_status_views` (declared last),
        // so every consumer view is emitted after its producer.
        let before = out.len();
        for e in ENTITIES {
            for v in e.create_views(&b) {
                out.push(v.render(&b));
            }
        }
        debug!(count = out.len() - before, "Views created");

        // Phase 4: CREATE INDEX.
        let before = out.len();
        for e in ENTITIES {
            for i in e.create_indices() {
                out.push(b.build_schema(i));
            }
        }
        debug!(count = out.len() - before, "Indices created");

        // Phase 5a: INSERT seeds — all of one entity's seeds before the next,
        // so any cross-entity dependency resolves via ENTITIES ordering.
        let before = out.len();
        for e in ENTITIES {
            for ins in e.seeds() {
                out.push(b.build_query(ins));
            }
        }
        // Phase 5b: `_current` snapshot seeding — runs after every event-log
        // table is seeded so `ROW_NUMBER` ranks against the complete history.
        for e in ENTITIES {
            for spec in e.seed_current() {
                out.push(spec.render(&b));
            }
        }
        debug!(count = out.len() - before, "Seed rows inserted");

        // Phase 6: PostgreSQL row-level security — enable + force RLS and a
        // project-isolation policy on every project-scoped table, and override
        // the `project_id` default to read `app.project_id`. PostgreSQL only;
        // SQLite/MySQL have no RLS. Runs last so FORCE RLS + WITH CHECK never
        // reject the un-scoped seed inserts emitted above.
        if matches!(b, Backend::Postgres) {
            let before = out.len();
            for table in PROJECT_SCOPED_TABLES {
                out.extend(sql::rls::policy_statements(table));
            }
            debug!(count = out.len() - before, "RLS statements emitted");
        }

        info!(version = Self::VERSION, total = out.len(), "Schema built");
        out
    }
}

/// Build the complete current schema as a list of SQL statements for the given backend.
///
/// Delegates to [`Schema::build()`] via the [`SchemaVersion`] trait.
pub fn build(b: Backend) -> Vec<String> {
    Schema::build(b)
}
