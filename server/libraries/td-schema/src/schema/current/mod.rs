//
// Copyright 2026 Tabsdata Inc.
//

mod bundles;
mod collections;
mod connections;
mod dependencies;
mod dependencies_current;
mod executions;
mod function_patches;
mod function_patches_current;
mod function_versions;
mod functions;
mod functions_current;
mod groups;
mod logical_function_run_inputs;
mod logical_function_run_pointers;
mod logical_function_run_pointers_current;
mod logical_function_runs;
mod logical_table_data_version_pointers;
mod logical_table_data_version_pointers_current;
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

use crate::schema::Entity;
use crate::schema::deltas;
use crate::schema::render::{render_query, render_schema};
use crate::schema::source::{Derived, SchemaSource, install_script};
use crate::schema::sql;
use sea_query::{DynIden, IntoIden};

/// Schema revision within the current major; resets to 1 at each major bump
/// (cross-major upgrade is refused via `tabsdata_version`). To evolve the
/// schema, follow the README runbook; `test_schema_ddl_snapshot` gates the DDL.
pub const VERSION: usize = 2;

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
    &function_versions::Defs,
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
    &logical_function_run_pointers_current::Defs,
    &logical_table_data_versions::Defs,
    &logical_table_data_version_pointers::Defs,
    &logical_table_data_version_pointers_current::Defs,
    &logical_function_run_inputs::Defs,
    &with_status_views::Defs,
];

/// Tables that carry a `project_id` and get PostgreSQL row-level security.
/// These are exactly the tables to which `project_id` was added in `current/`;
/// keep this list in sync with that column. The `projects` registry itself is
/// intentionally absent — it is keyed by `id`, not `project_id`, and the app
/// never lists it (the active project arrives in the request).
///
/// `test_project_scoped_tables_match_project_id_columns` cross-checks this list
/// against the tables that actually declare a `project_id` column, so an
/// omission (which would silently skip RLS for that table) fails the build.
pub(crate) fn project_scoped_tables() -> Vec<DynIden> {
    vec![
        groups::Groups::Table.into_iden(),
        connections::Connections::Table.into_iden(),
        collections::Collections::Table.into_iden(),
        bundles::Bundles::Table.into_iden(),
        functions::Functions::Table.into_iden(),
        functions_current::FunctionsCurrent::Table.into_iden(),
        function_patches::FunctionPatches::Table.into_iden(),
        function_patches_current::FunctionPatchesCurrent::Table.into_iden(),
        tables::Tables::Table.into_iden(),
        tables_current::TablesCurrent::Table.into_iden(),
        dependencies::Dependencies::Table.into_iden(),
        dependencies_current::DependenciesCurrent::Table.into_iden(),
        triggers::Triggers::Table.into_iden(),
        triggers_current::TriggersCurrent::Table.into_iden(),
        executions::Executions::Table.into_iden(),
        transactions::Transactions::Table.into_iden(),
        physical_function_runs::PhysicalFunctionRuns::Table.into_iden(),
        physical_table_data_versions::PhysicalTableDataVersions::Table.into_iden(),
        table_partitions::TablePartitions::Table.into_iden(),
        workers::Workers::Table.into_iden(),
        logical_function_runs::LogicalFunctionRuns::Table.into_iden(),
        logical_table_data_versions::LogicalTableDataVersions::Table.into_iden(),
        logical_function_run_inputs::LogicalFunctionRunInputs::Table.into_iden(),
        logical_function_run_pointers::LogicalFunctionRunPointers::Table.into_iden(),
        logical_function_run_pointers_current::LogicalFunctionRunPointersCurrent::Table.into_iden(),
        logical_table_data_version_pointers::LogicalTableDataVersionPointers::Table.into_iden(),
        logical_table_data_version_pointers_current::LogicalTableDataVersionPointersCurrent::Table.into_iden(),
    ]
}

/// The complete fresh-install script for the current schema.
pub fn build() -> Vec<String> {
    install_script(&Source)
}

/// [`SchemaSource`] for the live schema, over `ENTITIES` plus [`deltas`].
/// Re-exported as `CurrentSource`. An archive copy of this module keeps the
/// struct, dropping only the `delta` override (archives install fresh).
pub struct Source;

impl SchemaSource for Source {
    fn current_version(&self) -> usize {
        VERSION
    }

    fn tables(&self) -> Vec<String> {
        ENTITIES.iter().flat_map(|e| e.create_tables()).map(render_schema).collect()
    }

    fn triggers(&self) -> Vec<Derived> {
        ENTITIES
            .iter()
            .flat_map(|e| e.create_db_triggers())
            .map(|t| Derived {
                create: t.render(),
                drop: t.render_drop(),
            })
            .collect()
    }

    fn views(&self) -> Vec<Derived> {
        ENTITIES
            .iter()
            .flat_map(|e| e.create_views())
            .map(|v| Derived {
                create: vec![v.render()],
                drop: vec![v.render_drop()],
            })
            .collect()
    }

    fn indexes(&self) -> Vec<String> {
        ENTITIES.iter().flat_map(|e| e.create_indices()).map(render_schema).collect()
    }

    fn seeds(&self) -> Vec<String> {
        let mut out: Vec<String> = ENTITIES.iter().flat_map(|e| e.seeds()).map(render_query).collect();
        // `_current` snapshot seeding after every event-log table is seeded,
        // so `ROW_NUMBER` ranks against the complete history.
        out.extend(ENTITIES.iter().flat_map(|e| e.seed_current()).map(|spec| spec.render()));
        out
    }

    fn rls(&self) -> Vec<String> {
        project_scoped_tables().iter().flat_map(|t| sql::rls::policy_statements(&t.to_string())).collect()
    }

    fn delta(&self, target: usize) -> Vec<String> {
        deltas::get(target)
    }
}
