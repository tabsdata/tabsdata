//
// Copyright 2025 Tabs Data Inc.
//

//! Datasets Data Transfer Objects (API)

use crate::datasets::dao::{
    DatasetWithNames, DependencyUris, DsDataVersion, DsExecutionPlanWithNames, DsTableList,
    DsTransaction, DsWorkerMessageWithNames, FunctionWithNames, TriggerUris,
};
use crate::rest_urls::FunctionIdParam;
use axum::body::BodyDataStream;
use axum::extract::Request;
use derive_builder::Builder;
use getset::Getters;
use polars::datatypes::Field;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use td_utoipa::api_server_schema;
use tokio::sync::Mutex;
use utoipa::IntoParams;

#[api_server_schema]
#[derive(Debug, Clone, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
pub struct DatasetWrite {
    pub name: String,
    pub description: String,
    pub data_location: Option<String>,
    pub bundle_hash: String,
    pub tables: Vec<String>,
    pub dependencies: Vec<String>,
    pub trigger_by: Option<Vec<String>>,
    pub function_snippet: Option<String>,
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
pub struct DatasetRead {
    id: String,
    name: String,
    description: String,
    collection_id: String,
    collection: String,

    created_on: i64,
    created_by_id: String,
    created_by: String,
    modified_on: i64,
    modified_by_id: String,
    modified_by: String,

    current_function_id: String,
    current_data_version_id: Option<String>,
    last_run_on: Option<i64>,
    data_versions: usize,
    data_location: String,
    bundle_avail: bool,
    function_snippet: Option<String>,
}

#[api_server_schema]
#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct FunctionList {
    id: String,
    name: String,
    description: String,
    data_location: String,
    function_snippet: String,
    created_on: i64,
    created_by_id: String,
    created_by: String,
    tables: Vec<String>,
    trigger_with_ids: Vec<String>,
    trigger_with_names: Vec<String>,
    dependencies_with_ids: Vec<String>,
    dependencies_with_names: Vec<String>,
}

pub type FunctionRead = FunctionList;

impl FunctionList {
    pub fn new(
        f: FunctionWithNames,
        tables: Vec<String>,
        deps: Vec<DependencyUris>,
        triggers: Vec<TriggerUris>,
    ) -> Self {
        let (deps_ids, deps_names) = deps
            .iter()
            .map(|dep| (dep.uri_with_ids().clone(), dep.uri_with_names().clone()))
            .collect();
        let (trigger_ids, trigger_names) = triggers
            .iter()
            .map(|trg| (trg.uri_with_ids().clone(), trg.uri_with_names().clone()))
            .collect();
        Self {
            id: f.id().clone(),
            name: f.name().clone(),
            description: f.description().clone(),
            data_location: f.data_location().clone(),
            function_snippet: f.function_snippet().clone(),
            created_on: f.created_on().timestamp_millis(),
            created_by_id: f.created_by_id().clone(),
            created_by: f.created_by().clone(),
            tables,
            trigger_with_ids: trigger_ids,
            trigger_with_names: trigger_names,
            dependencies_with_ids: deps_ids,
            dependencies_with_names: deps_names,
        }
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct DataVersionList {
    id: String,
    collection_id: String,
    dataset_id: String,
    function_id: String,
    execution_plan_id: String,
    trigger: String,
    triggered_on: i64,
    started_on: Option<i64>,
    ended_on: Option<i64>,
    status: String,
}

impl From<&DsDataVersion> for DataVersionList {
    fn from(dv: &DsDataVersion) -> Self {
        Self {
            id: dv.id().clone(),
            collection_id: dv.collection_id().clone(),
            dataset_id: dv.dataset_id().clone(),
            function_id: dv.function_id().clone(),
            execution_plan_id: dv.execution_plan_id().clone(),
            trigger: dv.trigger().clone(),
            triggered_on: dv.triggered_on().timestamp_millis(),
            started_on: dv.started_on().map(|d| d.timestamp_millis()),
            ended_on: dv.ended_on().map(|d| d.timestamp_millis()),
            status: dv.status().to_string(),
        }
    }
}

impl DatasetRead {
    pub fn from(dataset: &DatasetWithNames) -> Self {
        Self {
            id: dataset.id().clone(),
            name: dataset.name().clone(),
            description: dataset.description().clone(),
            collection_id: dataset.collection_id().clone(),
            collection: dataset.collection().clone(),

            created_on: dataset.created_on().timestamp_millis(),
            created_by_id: dataset.created_by_id().clone(),
            created_by: dataset.created_by().clone(),
            modified_on: dataset.modified_on().timestamp_millis(),
            modified_by_id: dataset.modified_by_id().clone(),
            modified_by: dataset.modified_by().clone(),

            current_function_id: dataset.current_function_id().clone(),
            current_data_version_id: dataset.current_data_id().clone(),
            last_run_on: dataset.last_run_on().map(|d| d.timestamp_millis()),
            data_versions: *dataset.data_versions() as usize,
            data_location: dataset.data_location().clone(),
            bundle_avail: *dataset.bundle_avail(),
            function_snippet: dataset.function_snippet().clone(),
        }
    }
}

pub type DatasetList = DatasetRead;

#[derive(Debug, Clone)]
pub struct UploadFunction {
    function_param: FunctionIdParam,
    request: Arc<Mutex<Option<Request>>>,
}

impl UploadFunction {
    pub fn new(function_param: impl Into<FunctionIdParam>, request: Request) -> Self {
        Self {
            function_param: function_param.into(),
            request: Arc::new(Mutex::new(Some(request))),
        }
    }

    pub fn collection(&self) -> &str {
        self.function_param.collection()
    }

    pub fn dataset(&self) -> &str {
        self.function_param.function()
    }

    pub fn function_id(&self) -> &str {
        self.function_param.function_id()
    }

    pub async fn stream(&self) -> Option<BodyDataStream> {
        self.request
            .lock()
            .await
            .take()
            .map(|request| request.into_body().into_data_stream())
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Deserialize, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct ExecutionPlanWrite {
    name: Option<String>,
}

#[api_server_schema]
#[derive(Debug, Getters, Builder)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct ExecutionTemplateRead {
    collection_name: String,
    dataset_name: String,
    triggered_datasets: Vec<String>,
    dot: String,
}

impl ExecutionTemplateRead {
    pub fn builder() -> ExecutionTemplateReadBuilder {
        ExecutionTemplateReadBuilder::default()
    }
}

#[api_server_schema]
#[derive(Debug, Getters, Builder, Serialize)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct ExecutionPlanRead {
    name: String,
    triggered_datasets_with_ids: Vec<String>,
    triggered_datasets_with_names: Vec<String>,
    dot: String,
}

impl ExecutionPlanRead {
    pub fn builder() -> ExecutionPlanReadBuilder {
        ExecutionPlanReadBuilder::default()
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, Serialize)]
#[getset(get = "pub")]
pub struct ExecutionPlanList {
    id: String,
    name: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
    triggered_by_id: String,
    triggered_by: String,
    triggered_on: i64,
    started_on: Option<i64>,
    ended_on: Option<i64>,
    status: String,
}

impl From<&DsExecutionPlanWithNames> for ExecutionPlanList {
    fn from(value: &DsExecutionPlanWithNames) -> Self {
        Self {
            id: value.id().clone(),
            name: value.name().clone(),
            collection_id: value.collection_id().clone(),
            collection: value.collection().clone(),
            dataset_id: value.dataset_id().clone(),
            dataset: value.dataset().clone(),
            triggered_by_id: value.triggered_by().clone(),
            triggered_by: value.triggered_by().clone(),
            triggered_on: value.triggered_on().timestamp_millis(),
            started_on: value.started_on().map(|d| d.timestamp_millis()),
            ended_on: value.ended_on().map(|d| d.timestamp_millis()),
            status: value.status().to_string(),
        }
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, Serialize)]
#[getset(get = "pub")]
pub struct TransactionList {
    id: String,
    execution_plan_id: String,
    transaction_by: String,
    transaction_key: String,
    triggered_on: i64,
    started_on: Option<i64>,
    ended_on: Option<i64>,
    status: String,
}

impl From<&DsTransaction> for TransactionList {
    fn from(value: &DsTransaction) -> Self {
        Self {
            id: value.id().clone(),
            execution_plan_id: value.execution_plan_id().clone(),
            transaction_by: value.transaction_by().to_string(),
            transaction_key: value.transaction_key().clone(),
            triggered_on: value.triggered_on().timestamp_millis(),
            started_on: value.started_on().map(|d| d.timestamp_millis()),
            ended_on: value.ended_on().map(|d| d.timestamp_millis()),
            status: value.status().to_string(),
        }
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, Serialize)]
#[getset(get = "pub")]
pub struct CommitList {
    id: String,
    transaction_id: String,
    execution_plan_id: String,
    transaction_by: String,
    transaction_key: String,
    commited_on: Option<i64>,
    triggered_on: i64,
    started_on: Option<i64>,
    ended_on: Option<i64>,
    status: String,
}

impl From<&DsTransaction> for CommitList {
    fn from(value: &DsTransaction) -> Self {
        Self {
            id: value.commit_id().clone().unwrap_or("".to_string()),
            transaction_id: value.id().clone(),
            execution_plan_id: value.execution_plan_id().clone(),
            transaction_by: value.transaction_by().to_string(),
            transaction_key: value.transaction_key().clone(),
            commited_on: value.commited_on().map(|d| d.timestamp_millis()),
            triggered_on: value.triggered_on().timestamp_millis(),
            started_on: value.started_on().map(|d| d.timestamp_millis()),
            ended_on: value.ended_on().map(|d| d.timestamp_millis()),
            status: value.status().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
#[allow(dead_code)]
pub struct TableUriParams {
    /// Collection name
    pub collection: String,
    /// Dataset name
    pub dataset: String,
    /// Dataset version
    pub version: String,
    /// Table name
    pub table: String,
}

impl From<&TableUriParams> for TableUriParams {
    fn from(value: &TableUriParams) -> Self {
        value.clone()
    }
}

#[api_server_schema]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    type_: String,
}

impl SchemaField {
    pub fn new(name: impl Into<String>, type_: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_: type_.into(),
        }
    }
}
impl From<Field> for SchemaField {
    fn from(value: Field) -> Self {
        Self {
            name: value.name().to_string(),
            type_: format!("{:?}", value.dtype()),
        }
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct TableList {
    id: String,
    name: String,
    dataset_id: String,
    function: String,
    function_id: String,
}

impl From<&DsTableList> for TableList {
    fn from(value: &DsTableList) -> Self {
        Self {
            id: value.id().clone(),
            name: value.name().clone(),
            dataset_id: value.dataset_id().clone(),
            function: value.function().clone(),
            function_id: value.function_id().clone(),
        }
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct WorkerMessageList {
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
    started_on: i64,
    status: String,
}

impl From<&DsWorkerMessageWithNames> for WorkerMessageList {
    fn from(value: &DsWorkerMessageWithNames) -> Self {
        Self {
            id: value.id().clone(),
            collection: value.collection().clone(),
            collection_id: value.collection_id().clone(),
            dataset: value.dataset().clone(),
            dataset_id: value.dataset_id().clone(),
            function: value.function().clone(),
            function_id: value.function_id().clone(),
            transaction_id: value.transaction_id().clone(),
            execution_plan: value.execution_plan().clone(),
            execution_plan_id: value.execution_plan_id().clone(),
            data_version_id: value.data_version_id().clone(),
            started_on: value.started_on().unwrap_or_default().timestamp_millis(),
            status: value.status().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datasets::dao::DatasetWithNames;
    use crate::datasets::dto::DatasetRead;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_dataset_read_from_dataset_with_names() {
        let dao = DatasetWithNames::builder()
            .id("id".to_string())
            .name("name".to_string())
            .description("description".to_string())
            .collection_id("collection_id".to_string())
            .collection("collection".to_string())
            .created_on(UniqueUtc::now_millis().await)
            .created_by_id("created_by_id".to_string())
            .created_by("created_by".to_string())
            .modified_on(UniqueUtc::now_millis().await)
            .modified_by_id("modified_by_id".to_string())
            .modified_by("modified_by".to_string())
            .current_function_id("current_function_id".to_string())
            .current_data_id(Some("current_data_id".to_string()))
            .last_run_on(Some(UniqueUtc::now_millis().await))
            .data_versions(1)
            .data_location("/")
            .bundle_avail(false)
            .function_snippet("SNIPPET".to_string())
            .build()
            .unwrap();
        let dto = DatasetRead::from(&dao);
        assert_eq!(dto.id(), dao.id());
        assert_eq!(dto.name(), dao.name());
        assert_eq!(dto.collection_id(), dao.collection_id());
        assert_eq!(dto.collection(), dao.collection());

        assert_eq!(dto.created_on(), &dao.created_on().timestamp_millis());
        assert_eq!(dto.created_by_id(), dao.created_by_id());
        assert_eq!(dto.created_by(), dao.created_by());
        assert_eq!(dto.modified_on(), &dao.modified_on().timestamp_millis());
        assert_eq!(dto.modified_by_id(), dao.modified_by_id());
        assert_eq!(dto.modified_by(), dao.modified_by());

        assert_eq!(dto.current_function_id(), dao.current_function_id());
        assert_eq!(dto.current_data_version_id(), dao.current_data_id());
        assert_eq!(
            dto.last_run_on(),
            &dao.last_run_on().map(|d| d.timestamp_millis())
        );
        assert_eq!(*dto.data_versions(), (*dao.data_versions()) as usize);
    }
}
