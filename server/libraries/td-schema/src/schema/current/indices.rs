//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Index, IndexCreateStatement};

use super::collections::Collections;
use super::functions::{
    Bundles, Dependencies, DependenciesCurrent, Executions, FunctionPatches, Functions,
    FunctionsCurrent, PhysicalFunctionRuns, PhysicalTableDataVersions, Tables, TablesCurrent,
    Transactions, Triggers, TriggersCurrent,
};
use super::logical::{
    LogicalFunctionRunInputs, LogicalFunctionRunPointers, LogicalFunctionRuns,
    LogicalTableDataVersionPointers, LogicalTableDataVersions,
};
use super::permissions::{InterCollectionPermissions, Permissions};
use super::roles::{Roles, UsersRoles};
use super::sessions::Sessions;
use super::users::Users;

pub fn all() -> Vec<IndexCreateStatement> {
    let mut indices = Vec::new();
    indices.extend(user_indices());
    indices.extend(collection_indices());
    indices.extend(function_indices());
    indices.extend(functions_current_indices());
    indices.extend(function_patch_indices());
    indices.extend(table_indices());
    indices.extend(tables_current_indices());
    indices.extend(execution_indices());
    indices.extend(function_run_indices());
    indices.extend(table_data_version_indices());
    indices.extend(transaction_indices());
    indices.extend(trigger_indices());
    indices.extend(triggers_current_indices());
    indices.extend(bundle_indices());
    indices.extend(dependency_indices());
    indices.extend(dependencies_current_indices());
    indices.extend(session_indices());
    indices.extend(role_indices());
    indices.extend(permission_indices());
    indices.extend(logical_indices());
    indices
}

fn logical_indices() -> Vec<IndexCreateStatement> {
    vec![
        // LFR: lookup by execution_id (read-execution path).
        Index::create()
            .name("logical_function_runs___execution_id___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::ExecutionId)
            .to_owned(),
        // LFR: lookup by transaction_id (cancel cascade scans the transaction's LFRs).
        Index::create()
            .name("logical_function_runs___transaction_id___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::TransactionId)
            .to_owned(),
        // LFR pointers: latest-pointer-per-LFR scan
        // (PARTITION BY logical_function_run_id ORDER BY set_on DESC, id DESC).
        Index::create()
            .name("logical_function_run_pointers___logical_function_run_id__set_on__id___idx")
            .table(LogicalFunctionRunPointers::Table)
            .col(LogicalFunctionRunPointers::LogicalFunctionRunId)
            .col(LogicalFunctionRunPointers::SetOn)
            .col(LogicalFunctionRunPointers::Id)
            .to_owned(),
        // LFR pointers: reverse lookup from concrete FR to the LFR(s) that
        // currently or historically point at it.
        Index::create()
            .name("logical_function_run_pointers___function_run_id___idx")
            .table(LogicalFunctionRunPointers::Table)
            .col(LogicalFunctionRunPointers::PhysicalFunctionRunId)
            .to_owned(),
        // LTDV: hot path — `Head(0)` resolution scans by table_id ordered by triggered_on.
        Index::create()
            .name("logical_table_data_versions___table_id__triggered_on___idx")
            .table(LogicalTableDataVersions::Table)
            .col(LogicalTableDataVersions::TableId)
            .col(LogicalTableDataVersions::TriggeredOn)
            .to_owned(),
        // LTDV: lookup by execution / transaction (read-execution and cancel paths).
        Index::create()
            .name("logical_table_data_versions___execution_id___idx")
            .table(LogicalTableDataVersions::Table)
            .col(LogicalTableDataVersions::ExecutionId)
            .to_owned(),
        Index::create()
            .name("logical_table_data_versions___transaction_id___idx")
            .table(LogicalTableDataVersions::Table)
            .col(LogicalTableDataVersions::TransactionId)
            .to_owned(),
        // LTDV: producer-side lookup (find every output an LFR produces).
        Index::create()
            .name("logical_table_data_versions___producer_logical_function_run_id___idx")
            .table(LogicalTableDataVersions::Table)
            .col(LogicalTableDataVersions::ProducerLogicalFunctionRunId)
            .to_owned(),
        // LTDV pointers: latest-pointer-per-LTDV scan
        // (PARTITION BY logical_table_data_version_id ORDER BY set_on DESC, id DESC).
        Index::create()
            .name("logical_table_data_version_pointers___logical_table_data_version_id__set_on__id___idx")
            .table(LogicalTableDataVersionPointers::Table)
            .col(LogicalTableDataVersionPointers::LogicalTableDataVersionId)
            .col(LogicalTableDataVersionPointers::SetOn)
            .col(LogicalTableDataVersionPointers::Id)
            .to_owned(),
        // LTDV pointers: reverse lookup from concrete TDV.
        Index::create()
            .name("logical_table_data_version_pointers___table_data_version_id___idx")
            .table(LogicalTableDataVersionPointers::Table)
            .col(LogicalTableDataVersionPointers::PhysicalTableDataVersionId)
            .to_owned(),
        // LFR inputs: scheduler readiness query joins on logical_function_run_id.
        Index::create()
            .name("logical_function_run_inputs___logical_function_run_id___idx")
            .table(LogicalFunctionRunInputs::Table)
            .col(LogicalFunctionRunInputs::LogicalFunctionRunId)
            .to_owned(),
        // LFR inputs: lookup by execution (plan rebuild and read paths).
        Index::create()
            .name("logical_function_run_inputs___execution_id___idx")
            .table(LogicalFunctionRunInputs::Table)
            .col(LogicalFunctionRunInputs::ExecutionId)
            .to_owned(),
    ]
}

fn user_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("users___name___idx")
            .table(Users::Table)
            .col(Users::Name)
            .to_owned(),
        Index::create()
            .name("users___enabled___idx")
            .table(Users::Table)
            .col(Users::Enabled)
            .to_owned(),
    ]
}

fn collection_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("collections___name___idx")
            .table(Collections::Table)
            .col(Collections::Name)
            .to_owned(),
    ]
}

fn function_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("functions___collection_id___idx")
            .table(Functions::Table)
            .col(Functions::CollectionId)
            .to_owned(),
        // Composite index for versioned queries: PARTITION BY function_id ORDER BY defined_on DESC, id DESC
        Index::create()
            .name("functions___function_id__defined_on__id___idx")
            .table(Functions::Table)
            .col(Functions::FunctionId)
            .col(Functions::DefinedOn)
            .col(Functions::Id)
            .to_owned(),
    ]
}

fn functions_current_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("functions_current___collection_id___idx")
            .table(FunctionsCurrent::Table)
            .col(FunctionsCurrent::CollectionId)
            .to_owned(),
        Index::create()
            .name("functions_current___status___idx")
            .table(FunctionsCurrent::Table)
            .col(FunctionsCurrent::Status)
            .to_owned(),
    ]
}

fn function_patch_indices() -> Vec<IndexCreateStatement> {
    // The "latest patch of head fvid" hot path now reads
    // `function_patches_current` (one row per parent_function_version_id,
    // with the partition column declared `unique_key()` → indexed). The
    // historic `function_patches` base table is only scanned by
    // list-patches, which orders by `(function_id, defined_on DESC)`.
    vec![
        Index::create()
            .name("function_patches___function_id__defined_on___idx")
            .table(FunctionPatches::Table)
            .col(FunctionPatches::FunctionId)
            .col(FunctionPatches::DefinedOn)
            .to_owned(),
    ]
}

fn table_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("tables___function_id__status___idx")
            .table(Tables::Table)
            .col(Tables::FunctionId)
            .col(Tables::Status)
            .to_owned(),
        Index::create()
            .name("tables___status___idx")
            .table(Tables::Table)
            .col(Tables::Status)
            .to_owned(),
        // Composite index for versioned queries: PARTITION BY table_id ORDER BY defined_on DESC, id DESC
        Index::create()
            .name("tables___table_id__defined_on__id___idx")
            .table(Tables::Table)
            .col(Tables::TableId)
            .col(Tables::DefinedOn)
            .col(Tables::Id)
            .to_owned(),
    ]
}

fn tables_current_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("tables_current___collection_id___idx")
            .table(TablesCurrent::Table)
            .col(TablesCurrent::CollectionId)
            .to_owned(),
        Index::create()
            .name("tables_current___function_id___idx")
            .table(TablesCurrent::Table)
            .col(TablesCurrent::FunctionId)
            .to_owned(),
        Index::create()
            .name("tables_current___status___idx")
            .table(TablesCurrent::Table)
            .col(TablesCurrent::Status)
            .to_owned(),
    ]
}

fn execution_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("executions___collection_id___idx")
            .table(Executions::Table)
            .col(Executions::CollectionId)
            .to_owned(),
        Index::create()
            .name("executions___function_version_id___idx")
            .table(Executions::Table)
            .col(Executions::FunctionVersionId)
            .to_owned(),
        Index::create()
            .name("executions___triggered_by_id___idx")
            .table(Executions::Table)
            .col(Executions::TriggeredById)
            .to_owned(),
        Index::create()
            .name("executions___triggered_on___idx")
            .table(Executions::Table)
            .col(Executions::TriggeredOn)
            .to_owned(),
    ]
}

fn function_run_indices() -> Vec<IndexCreateStatement> {
    use crate::schema::current::logical::LogicalFunctionRuns;

    vec![
        Index::create()
            .name("function_runs___collection_id___idx")
            .table(PhysicalFunctionRuns::Table)
            .col(PhysicalFunctionRuns::CollectionId)
            .to_owned(),
        Index::create()
            .name("function_runs___function_version_id___idx")
            .table(PhysicalFunctionRuns::Table)
            .col(PhysicalFunctionRuns::FunctionVersionId)
            .to_owned(),
        // Per-attempt status indices (FR-listing endpoints).
        Index::create()
            .name("function_runs___execution_id__status___idx")
            .table(PhysicalFunctionRuns::Table)
            .col(PhysicalFunctionRuns::ExecutionId)
            .col(PhysicalFunctionRuns::Status)
            .to_owned(),
        Index::create()
            .name("function_runs___transaction_id__status___idx")
            .table(PhysicalFunctionRuns::Table)
            .col(PhysicalFunctionRuns::TransactionId)
            .col(PhysicalFunctionRuns::Status)
            .to_owned(),
        Index::create()
            .name("function_runs___status___idx")
            .table(PhysicalFunctionRuns::Table)
            .col(PhysicalFunctionRuns::Status)
            .to_owned(),
        // FR → LFR derivation (status cascade, callback, redirect, replay).
        // Unique by construction: at most one LFR per `(execution_id,
        // function_id)`. The `.unique()` lifts that invariant from
        // service-layer convention to DB structure.
        Index::create()
            .name("logical_function_runs___execution_id__function_id___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::ExecutionId)
            .col(LogicalFunctionRuns::FunctionId)
            .unique()
            .to_owned(),
        // Canonical (LFR) status indices for the logical-chain cascade.
        Index::create()
            .name("logical_function_runs___execution_id__status___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::ExecutionId)
            .col(LogicalFunctionRuns::Status)
            .to_owned(),
        Index::create()
            .name("logical_function_runs___transaction_id__status___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::TransactionId)
            .col(LogicalFunctionRuns::Status)
            .to_owned(),
        Index::create()
            .name("logical_function_runs___status___idx")
            .table(LogicalFunctionRuns::Table)
            .col(LogicalFunctionRuns::Status)
            .to_owned(),
    ]
}

fn table_data_version_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("table_data_versions___collection_id___idx")
            .table(PhysicalTableDataVersions::Table)
            .col(PhysicalTableDataVersions::CollectionId)
            .to_owned(),
        Index::create()
            .name("table_data_versions___execution_id___idx")
            .table(PhysicalTableDataVersions::Table)
            .col(PhysicalTableDataVersions::ExecutionId)
            .to_owned(),
        Index::create()
            .name("table_data_versions___function_run_id___idx")
            .table(PhysicalTableDataVersions::Table)
            .col(PhysicalTableDataVersions::FunctionRunId)
            .to_owned(),
        // Note: single table_id index removed — it's a prefix of these composites
        Index::create()
            .name("table_data_versions___table_id___has_data___idx")
            .table(PhysicalTableDataVersions::Table)
            .col(PhysicalTableDataVersions::TableId)
            .col(PhysicalTableDataVersions::HasData)
            .to_owned(),
        // Composite index for versioned queries: PARTITION BY table_id ORDER BY triggered_on DESC, id DESC
        Index::create()
            .name("table_data_versions___table_id__triggered_on__id___idx")
            .table(PhysicalTableDataVersions::Table)
            .col(PhysicalTableDataVersions::TableId)
            .col(PhysicalTableDataVersions::TriggeredOn)
            .col(PhysicalTableDataVersions::Id)
            .to_owned(),
    ]
}

fn transaction_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("transactions___collection_id___idx")
            .table(Transactions::Table)
            .col(Transactions::CollectionId)
            .to_owned(),
        Index::create()
            .name("transactions___execution_id___idx")
            .table(Transactions::Table)
            .col(Transactions::ExecutionId)
            .to_owned(),
    ]
}

fn trigger_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("triggers___collection_id___idx")
            .table(Triggers::Table)
            .col(Triggers::CollectionId)
            .to_owned(),
        Index::create()
            .name("triggers___function_id___idx")
            .table(Triggers::Table)
            .col(Triggers::FunctionId)
            .to_owned(),
        // Composite index for versioned queries: PARTITION BY trigger_id ORDER BY defined_on DESC, id DESC
        Index::create()
            .name("triggers___trigger_id__defined_on__id___idx")
            .table(Triggers::Table)
            .col(Triggers::TriggerId)
            .col(Triggers::DefinedOn)
            .col(Triggers::Id)
            .to_owned(),
        // For recursive upstream traversal
        Index::create()
            .name("triggers___trigger_by_function_id___idx")
            .table(Triggers::Table)
            .col(Triggers::TriggerByFunctionId)
            .to_owned(),
    ]
}

fn dependencies_current_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("dependencies_current___function_id___idx")
            .table(DependenciesCurrent::Table)
            .col(DependenciesCurrent::FunctionId)
            .to_owned(),
        Index::create()
            .name("dependencies_current___table_function_id___idx")
            .table(DependenciesCurrent::Table)
            .col(DependenciesCurrent::TableFunctionId)
            .to_owned(),
    ]
}

fn triggers_current_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("triggers_current___collection_id___idx")
            .table(TriggersCurrent::Table)
            .col(TriggersCurrent::CollectionId)
            .to_owned(),
        Index::create()
            .name("triggers_current___function_id___idx")
            .table(TriggersCurrent::Table)
            .col(TriggersCurrent::FunctionId)
            .to_owned(),
        Index::create()
            .name("triggers_current___trigger_by_function_id___idx")
            .table(TriggersCurrent::Table)
            .col(TriggersCurrent::TriggerByFunctionId)
            .to_owned(),
    ]
}

fn bundle_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("bundles___collection_id___idx")
            .table(Bundles::Table)
            .col(Bundles::CollectionId)
            .to_owned(),
    ]
}

fn dependency_indices() -> Vec<IndexCreateStatement> {
    vec![
        // Composite index for versioned queries: PARTITION BY dependency_id ORDER BY defined_on DESC, id DESC
        Index::create()
            .name("dependencies___dependency_id__defined_on__id___idx")
            .table(Dependencies::Table)
            .col(Dependencies::DependencyId)
            .col(Dependencies::DefinedOn)
            .col(Dependencies::Id)
            .to_owned(),
        // For recursive downstream graph traversal (recurse_down column)
        Index::create()
            .name("dependencies___function_id___idx")
            .table(Dependencies::Table)
            .col(Dependencies::FunctionId)
            .to_owned(),
        // For recursive upstream graph traversal (recurse_up column)
        Index::create()
            .name("dependencies___table_function_id___idx")
            .table(Dependencies::Table)
            .col(Dependencies::TableFunctionId)
            .to_owned(),
    ]
}

fn session_indices() -> Vec<IndexCreateStatement> {
    vec![
        // For session purge: DELETE WHERE expires_on < now
        Index::create()
            .name("sessions___expires_on___idx")
            .table(Sessions::Table)
            .col(Sessions::ExpiresOn)
            .to_owned(),
        // For session lookup by user during refresh
        Index::create()
            .name("sessions___user_id___idx")
            .table(Sessions::Table)
            .col(Sessions::UserId)
            .to_owned(),
    ]
}

fn role_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("roles___name___idx")
            .table(Roles::Table)
            .col(Roles::Name)
            .unique()
            .to_owned(),
        Index::create()
            .name("roles___user_id__role_id___idx")
            .table(UsersRoles::Table)
            .col(UsersRoles::UserId)
            .col(UsersRoles::RoleId)
            .unique()
            .to_owned(),
    ]
}

fn permission_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("permissions___role_id__permission_type__entity___idx")
            .table(Permissions::Table)
            .col(Permissions::RoleId)
            .col(Permissions::PermissionType)
            .col(Permissions::EntityType)
            .col(Permissions::EntityId)
            .unique()
            .to_owned(),
        Index::create()
            .name("inter_collection_permissions___from_collection_id__to_collection_id__idx")
            .table(InterCollectionPermissions::Table)
            .col(InterCollectionPermissions::FromCollectionId)
            .col(InterCollectionPermissions::ToCollectionId)
            .unique()
            .to_owned(),
    ]
}
