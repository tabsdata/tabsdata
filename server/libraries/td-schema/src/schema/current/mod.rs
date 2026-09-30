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
mod metrics_facts;
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
mod workers;

use crate::schema::Entity;
use crate::schema::deltas;
use crate::schema::render::{render_query, render_schema};
use crate::schema::source::{Delta, Derived, SchemaSource, index_derived, install_script};
use crate::schema::sql::rls::policy_statements;
use sea_query::{DynIden, IntoIden};

/// Schema revision within the current major; resets to 1 at each major bump
/// (cross-major upgrade is refused via `tabsdata_version`). To evolve the
/// schema, follow the README runbook; `test_schema_ddl_snapshot` gates the DDL.
pub const VERSION: usize = 11;

pub use crate::schema::sql::{Changed, Logical, Parent, Physical, RollupArm, RollupDef};
pub use executions::rollup as execution_rollup;
pub use transactions::rollup as transaction_rollup;

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
    &metrics_facts::Defs,
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
        metrics_facts::MetricsFacts::Table.into_iden(),
    ]
}

/// The complete fresh-install script for the current schema.
/// One index of the current schema, by name: what a registry of index
/// consumers is checked against.
pub struct IndexInfo {
    pub name: String,
    pub unique: bool,
}

pub fn indexes() -> Vec<IndexInfo> {
    ENTITIES
        .iter()
        .flat_map(|e| e.create_indices())
        .map(|i| IndexInfo {
            name: i.get_index_spec().get_name().expect("every index is named").to_string(),
            unique: i.is_unique_key(),
        })
        .chain(ENTITIES.iter().flat_map(|e| e.create_partial_indices()).map(|i| IndexInfo {
            name: i.name().to_string(),
            unique: false,
        }))
        .collect()
}

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
        ENTITIES
            .iter()
            .flat_map(|e| e.create_indices())
            .map(index_derived)
            .chain(ENTITIES.iter().flat_map(|e| e.create_partial_indices()).map(|i| i.render()))
            .collect()
    }

    fn statistics(&self) -> Vec<Derived> {
        ENTITIES.iter().flat_map(|e| e.create_statistics()).map(|s| s.render()).collect()
    }

    fn seeds(&self) -> Vec<String> {
        ENTITIES.iter().flat_map(|e| e.seeds()).map(render_query).collect()
    }

    fn rls(&self) -> Vec<String> {
        project_scoped_tables().iter().flat_map(|t| policy_statements(&t.to_string())).collect()
    }

    fn delta(&self, target: usize) -> Delta {
        deltas::get(target)
    }
}
