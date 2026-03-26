//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Index, IndexCreateStatement};

use super::collections::Collections;
use super::functions::{
    Bundles, Dependencies, Executions, FunctionRequirements, FunctionRuns, Functions,
    TableDataVersions, Tables, Transactions, Triggers,
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
    indices.extend(table_indices());
    indices.extend(execution_indices());
    indices.extend(function_run_indices());
    indices.extend(table_data_version_indices());
    indices.extend(transaction_indices());
    indices.extend(trigger_indices());
    indices.extend(bundle_indices());
    indices.extend(dependency_indices());
    indices.extend(function_requirement_indices());
    indices.extend(session_indices());
    indices.extend(role_indices());
    indices.extend(permission_indices());
    indices
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
    vec![
        Index::create()
            .name("function_runs___collection_id___idx")
            .table(FunctionRuns::Table)
            .col(FunctionRuns::CollectionId)
            .to_owned(),
        // Composite for executions__with_status view (aggregates by execution_id + status)
        Index::create()
            .name("function_runs___execution_id__status___idx")
            .table(FunctionRuns::Table)
            .col(FunctionRuns::ExecutionId)
            .col(FunctionRuns::Status)
            .to_owned(),
        // Composite for transactions__with_status view (aggregates by transaction_id + status)
        Index::create()
            .name("function_runs___transaction_id__status___idx")
            .table(FunctionRuns::Table)
            .col(FunctionRuns::TransactionId)
            .col(FunctionRuns::Status)
            .to_owned(),
        Index::create()
            .name("function_runs___function_version_id___idx")
            .table(FunctionRuns::Table)
            .col(FunctionRuns::FunctionVersionId)
            .to_owned(),
        Index::create()
            .name("function_runs___status___idx")
            .table(FunctionRuns::Table)
            .col(FunctionRuns::Status)
            .to_owned(),
    ]
}

fn table_data_version_indices() -> Vec<IndexCreateStatement> {
    vec![
        Index::create()
            .name("table_data_versions___collection_id___idx")
            .table(TableDataVersions::Table)
            .col(TableDataVersions::CollectionId)
            .to_owned(),
        Index::create()
            .name("table_data_versions___execution_id___idx")
            .table(TableDataVersions::Table)
            .col(TableDataVersions::ExecutionId)
            .to_owned(),
        Index::create()
            .name("table_data_versions___function_run_id___idx")
            .table(TableDataVersions::Table)
            .col(TableDataVersions::FunctionRunId)
            .to_owned(),
        // Note: single table_id index removed — it's a prefix of these composites
        Index::create()
            .name("table_data_versions___table_id___has_data___idx")
            .table(TableDataVersions::Table)
            .col(TableDataVersions::TableId)
            .col(TableDataVersions::HasData)
            .to_owned(),
        // Composite index for versioned queries: PARTITION BY table_id ORDER BY triggered_on DESC, id DESC
        Index::create()
            .name("table_data_versions___table_id__triggered_on__id___idx")
            .table(TableDataVersions::Table)
            .col(TableDataVersions::TableId)
            .col(TableDataVersions::TriggeredOn)
            .col(TableDataVersions::Id)
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

fn function_requirement_indices() -> Vec<IndexCreateStatement> {
    vec![
        // For recursive downstream traversal (recurse_down column)
        Index::create()
            .name("function_requirements___function_run_id___idx")
            .table(FunctionRequirements::Table)
            .col(FunctionRequirements::FunctionRunId)
            .to_owned(),
        // For recursive upstream traversal (recurse_up column)
        Index::create()
            .name("function_requirements___requirement_function_run_id___idx")
            .table(FunctionRequirements::Table)
            .col(FunctionRequirements::RequirementFunctionRunId)
            .to_owned(),
        // For function_requirements__with_status view JOIN
        Index::create()
            .name("function_requirements___requirement_table_data_version_id___idx")
            .table(FunctionRequirements::Table)
            .col(FunctionRequirements::RequirementTableDataVersionId)
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
