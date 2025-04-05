//
// Copyright 2025 Tabs Data Inc.
//

//! Datasets Data Access Objects

use crate::tower_service::extractor::{
    DataVersionIdProvider, ExecutionPlanIdProvider, FunctionIdProvider, TransactionIdProvider,
};
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use getset::Getters;
use sqlx::FromRow;
use td_common::execution_status::{DataVersionStatus, ExecutionPlanStatus, TransactionStatus};
use td_database::sql::DbData;
use td_storage::location::StorageLocation;
use td_transaction::TransactionBy;

#[derive(Debug, Clone, PartialEq, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct Dataset {
    id: String,
    name: String,
    collection_id: String,

    created_on: DateTime<Utc>,
    created_by_id: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,

    current_function_id: String,
    current_data_id: Option<String>,
    last_run_on: Option<DateTime<Utc>>,
    data_versions: i64,
}

impl Dataset {
    /// Returns a new [`DatasetBuilder`] with default values.
    pub fn builder() -> DatasetBuilder {
        DatasetBuilder::default()
    }
}

impl DbData for Dataset {}

#[derive(Debug, Clone, Default, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DatasetWithNames {
    id: String,
    name: String,
    description: String,
    collection_id: String,
    collection: String,

    created_on: DateTime<Utc>,
    created_by_id: String,
    created_by: String,
    modified_on: DateTime<Utc>,
    modified_by_id: String,
    modified_by: String,

    current_function_id: String,
    current_data_id: Option<String>,
    last_run_on: Option<DateTime<Utc>>,
    data_versions: i64,
    data_location: String,
    bundle_avail: bool,
    function_snippet: Option<String>,
}

impl DatasetWithNames {
    /// Returns a new [`DatasetWithNamesBuilder`] with default values.
    pub fn builder() -> DatasetWithNamesBuilder {
        DatasetWithNamesBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsFunction {
    id: String,
    name: String,
    description: String,
    collection_id: String,
    dataset_id: String,
    data_location: String,
    #[sqlx(try_from = "String")]
    storage_location_version: StorageLocation,
    bundle_hash: String,
    bundle_avail: bool,
    function_snippet: Option<String>,

    execution_template: Option<String>,
    execution_template_created_on: Option<DateTime<Utc>>,

    created_on: DateTime<Utc>,
    created_by_id: String,
}

impl DsFunction {
    /// Returns a new [`DsFunctionBuilder`] with default values.
    pub fn builder() -> DsFunctionBuilder {
        DsFunctionBuilder::default()
    }
}

impl DbData for DsFunction {}

#[derive(Debug, Clone, PartialEq, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsFunctionWithNames {
    id: String,
    name: String,
    description: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
    data_location: String,
    bundle_avail: bool,
    function_snippet: Option<String>,
    created_on: DateTime<Utc>,
    created_by_id: String,
    created_by: String,
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsTable {
    id: String,
    name: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    pos: i64,
}

impl DsTable {
    /// Returns a new [`DsTableBuilder`] with default values.
    pub fn builder() -> DsTableBuilder {
        DsTableBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsTableWithNames {
    id: String,
    name: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
    function_id: String,
}

#[derive(Debug, Clone, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsTableList {
    id: String,
    name: String,
    dataset_id: String,
    function: String,
    function_id: String,
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsTableData {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    data_version_id: String,
    table_id: String,
    partition: String,
    schema_id: String,
    data_location: String,
    #[sqlx(try_from = "String")]
    storage_location_version: StorageLocation,
}

impl DsTableData {
    /// Returns a new [`DsTableDataBuilder`] with default values.
    pub fn builder() -> DsTableDataBuilder {
        DsTableDataBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsDependency {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,

    table_collection_id: String,
    table_dataset_id: String,
    table_name: String,
    table_versions: String,
    pos: i64,
}

impl DsDependency {
    /// Returns a new [`DsDependencyBuilder`] with default values.
    pub fn builder() -> DsDependencyBuilder {
        DsDependencyBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsDependenciesWithNames {
    id: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
    function_id: String,

    table_collection_id: String,
    table_collection: String,
    table_dataset_id: String,
    table_dataset: String,
    table_function_id: String,
    table_function: String,
    table_name: String,
    table_versions: String,
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsTrigger {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    trigger_collection_id: String,
    trigger_dataset_id: String,
}

impl DsTrigger {
    /// Returns a new [`DsTriggerBuilder`] with default values.
    pub fn builder() -> DsTriggerBuilder {
        DsTriggerBuilder::default()
    }
}

#[derive(Debug, Clone, FromRow, Getters)]
#[getset(get = "pub")]
pub struct FunctionWithNames {
    id: String,
    name: String,
    description: String,
    data_location: String,
    function_snippet: String,
    created_on: DateTime<Utc>,
    created_by_id: String,
    created_by: String,
}

#[derive(Debug, Clone, FromRow, Getters)]
#[getset(get = "pub")]
pub struct DependencyUris {
    function_id: String,
    uri_with_ids: String,
    uri_with_names: String,
}

#[derive(Debug, Clone, FromRow, Getters)]
#[getset(get = "pub")]
pub struct TriggerUris {
    function_id: String,
    uri_with_ids: String,
    uri_with_names: String,
}

#[derive(Debug, Clone, FromRow, Builder, Getters)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsDataVersion {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    transaction_id: String,
    execution_plan_id: String,
    trigger: String,
    triggered_on: DateTime<Utc>,
    started_on: Option<DateTime<Utc>>,
    ended_on: Option<DateTime<Utc>>,
    commit_id: Option<String>,
    commited_on: Option<DateTime<Utc>>,
    #[sqlx(try_from = "String")]
    status: DataVersionStatus,
}

impl ExecutionPlanIdProvider for DsDataVersion {
    fn execution_plan_id(&self) -> String {
        self.execution_plan_id().to_string()
    }
}

impl TransactionIdProvider for DsDataVersion {
    fn transaction_id(&self) -> String {
        self.transaction_id().to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsExecutionPlan {
    id: String,
    name: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    plan: String,
    triggered_by_id: String,
    triggered_on: DateTime<Utc>,
}

impl DsExecutionPlan {
    /// Returns a new [`DsExecutionPlanBuilder`] with default values.
    pub fn builder() -> DsExecutionPlanBuilder {
        DsExecutionPlanBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsExecutionPlanWithNames {
    id: String,
    name: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
    triggered_by_id: String,
    triggered_by: String,
    triggered_on: DateTime<Utc>,
    started_on: Option<DateTime<Utc>>,
    ended_on: Option<DateTime<Utc>>,
    #[sqlx(try_from = "String")]
    status: ExecutionPlanStatus,
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsTransaction {
    id: String,
    execution_plan_id: String,
    #[sqlx(try_from = "String")]
    transaction_by: TransactionBy,
    transaction_key: String,
    triggered_by_id: String,
    triggered_on: DateTime<Utc>,
    started_on: Option<DateTime<Utc>>,
    ended_on: Option<DateTime<Utc>>,
    commit_id: Option<String>,
    commited_on: Option<DateTime<Utc>>,
    #[sqlx(try_from = "String")]
    status: TransactionStatus,
}

impl DsTransaction {
    /// Returns a new [`DsTransactionBuilder`] with default values.
    pub fn builder() -> DsTransactionBuilder {
        DsTransactionBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq, Getters, FromRow, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsExecutionRequirement {
    id: String,
    transaction_id: String,
    execution_plan_id: String,
    execution_plan_triggered_on: DateTime<Utc>,

    target_collection_id: String,
    target_dataset_id: String,
    target_function_id: String,
    target_data_version: String,
    target_existing_dependency_count: i64,

    dependency_collection_id: Option<String>,
    dependency_dataset_id: Option<String>,
    dependency_function_id: Option<String>,
    dependency_table_id: Option<String>,
    dependency_pos: Option<i64>,
    dependency_data_version: Option<String>,
    dependency_formal_data_version: Option<String>,
    dependency_data_version_pos: Option<i64>,
}

impl DsExecutionRequirement {
    /// Returns a new [`DsExecutionRequirementsBuilder`] with default values.
    pub fn builder() -> DsExecutionRequirementBuilder {
        DsExecutionRequirementBuilder::default()
    }
}

#[derive(Debug, Clone, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsReadyToExecute {
    transaction_id: String,
    execution_plan_id: String,

    collection_id: String,
    collection_name: String,
    dataset_id: String,
    dataset_name: String,
    function_id: String,
    data_version: String,

    data_location: String,
    #[sqlx(try_from = "String")]
    storage_location_version: StorageLocation,
}

impl DataVersionIdProvider for DsReadyToExecute {
    fn data_version_id(&self) -> String {
        self.data_version().to_string()
    }
}

impl FunctionIdProvider for DsReadyToExecute {
    fn function_id(&self) -> String {
        self.function_id().to_string()
    }
}

#[derive(Debug, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsExecutionRequirementDependency {
    collection_id: String,
    collection_name: String,
    dataset_id: String,
    dataset_name: String,
    function_id: String,
    table_name: String,
    pos: i64,

    data_version: Option<String>,
    formal_data_version: Option<String>,
    data_version_pos: i64,

    data_location: String,
    #[sqlx(try_from = "String")]
    storage_location_version: StorageLocation,
}

#[derive(Debug, FromRow, Getters)]
#[getset(get = "pub")]
pub struct VersionInfo {
    commit_id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    version_id: String,
    #[sqlx(try_from = "String")]
    storage_location_version: StorageLocation,
    data_location: String,
}

#[derive(Debug, Clone, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsWorkerMessage {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    transaction_id: String,
    execution_plan_id: String,
    data_version_id: String,
}

#[derive(Debug, Clone, Getters, Builder, FromRow)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DsWorkerMessageWithNames {
    id: String,
    collection: String,
    collection_id: String,
    dataset: String,
    dataset_id: String,
    function: String,
    function_id: String,
    transaction_id: String,
    execution_plan: String,
    execution_plan_id: String,
    data_version_id: String,
    started_on: Option<DateTime<Utc>>,
    #[sqlx(try_from = "String")]
    status: DataVersionStatus,
}

#[cfg(test)]
mod tests {
    use crate::datasets::dao::Dataset;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_dataset_builder() {
        let now = UniqueUtc::now_millis();
        let dataset_db = Dataset::builder()
            .id(String::from("id"))
            .name(String::from("name"))
            .collection_id(String::from("collection_id"))
            .created_on(now)
            .created_by_id(String::from("created_by"))
            .modified_on(now)
            .modified_by_id(String::from("modified_by"))
            .current_function_id(String::from("current_function_id"))
            .current_data_id(Some(String::from("current_data_id")))
            .last_run_on(Some(now))
            .data_versions(1)
            .build()
            .unwrap();
        assert_eq!(dataset_db.id(), "id");
        assert_eq!(dataset_db.name(), "name");
        assert_eq!(dataset_db.collection_id(), "collection_id");
        assert_eq!(*dataset_db.created_on(), now);
        assert_eq!(dataset_db.created_by_id(), "created_by");
        assert_eq!(*dataset_db.modified_on(), now);
        assert_eq!(dataset_db.modified_by_id(), "modified_by");
        assert_eq!(dataset_db.current_function_id(), "current_function_id");
        assert_eq!(
            *dataset_db.current_data_id(),
            Some("current_data_id".to_string())
        );
        assert_eq!(*dataset_db.last_run_on(), Some(now));
        assert_eq!(*dataset_db.data_versions(), 1);
    }
}
