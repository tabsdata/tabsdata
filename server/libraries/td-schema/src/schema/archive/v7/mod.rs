//
// Copyright 2026 Tabsdata Inc.
//

mod bundles;
mod collections;
mod connections;
mod dependencies;
mod executions;
mod function_patches;
mod function_versions;
mod functions;
mod groups;
mod logical_function_run_inputs;
mod logical_function_runs;
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
mod transactions;
mod triggers;
mod users;
mod users_roles;
mod with_status_views;
mod workers;

use crate::schema::Entity;
use crate::schema::render::{render_query, render_schema};
use crate::schema::source::{Derived, SchemaSource, index_derived};
use crate::schema::sql::rls::policy_statements;
use sea_query::{DynIden, IntoIden};

/// Frozen schema revision of this archive.
pub const VERSION: usize = 7;

/// FK-safe ordering of every entity in the current schema.
///
/// THIS is the single source of truth for entity order across every
/// phase of schema construction (CREATE TABLE / TRIGGER / VIEW / INDEX
/// / INSERT). Add a new entity by writing its file and inserting one
/// line here in the position dictated by its FK targets — every
/// referenced table must appear strictly earlier.
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
    &function_patches::Defs,
    &function_versions::Defs,
    &tables::Defs,
    &dependencies::Defs,
    &triggers::Defs,
    &executions::Defs,
    &transactions::Defs,
    &logical_function_runs::Defs,
    &logical_table_data_versions::Defs,
    &physical_function_runs::Defs,
    &physical_table_data_versions::Defs,
    &table_partitions::Defs,
    &workers::Defs,
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
        function_patches::FunctionPatches::Table.into_iden(),
        tables::Tables::Table.into_iden(),
        dependencies::Dependencies::Table.into_iden(),
        triggers::Triggers::Table.into_iden(),
        executions::Executions::Table.into_iden(),
        transactions::Transactions::Table.into_iden(),
        physical_function_runs::PhysicalFunctionRuns::Table.into_iden(),
        physical_table_data_versions::PhysicalTableDataVersions::Table.into_iden(),
        table_partitions::TablePartitions::Table.into_iden(),
        workers::Workers::Table.into_iden(),
        logical_function_runs::LogicalFunctionRuns::Table.into_iden(),
        logical_table_data_versions::LogicalTableDataVersions::Table.into_iden(),
        logical_function_run_inputs::LogicalFunctionRunInputs::Table.into_iden(),
    ]
}

/// [`SchemaSource`] for this archived version, over its frozen `ENTITIES`.
pub struct Source;

impl SchemaSource for Source {
    fn current_version(&self) -> usize {
        VERSION
    }

    fn tables(&self) -> Vec<String> {
        ENTITIES.iter().flat_map(|e| e.create_tables()).map(render_schema).collect()
    }

    fn triggers(&self) -> Vec<Derived> {
        ENTITIES.iter().flat_map(|e| e.create_db_triggers()).collect()
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

    fn indexes(&self) -> Vec<Derived> {
        ENTITIES.iter().flat_map(|e| e.create_indices()).map(index_derived).collect()
    }

    fn seeds(&self) -> Vec<String> {
        ENTITIES.iter().flat_map(|e| e.seeds()).map(render_query).collect()
    }

    fn rls(&self) -> Vec<String> {
        project_scoped_tables().iter().flat_map(|t| policy_statements(&t.to_string())).collect()
    }
}
