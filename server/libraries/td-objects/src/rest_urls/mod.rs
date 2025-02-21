//
// Copyright 2025 Tabs Data Inc.
//

use crate::dlo::{
    CollectionName, Creator, DatasetName, ExecutionPlanId, TableName, WorkerMessageId,
};
use chrono::{DateTime, NaiveDateTime, ParseError, Utc};
use constcat::concat;
use getset::Getters;
use serde::Deserialize;
use serde_valid::Validate;
use td_apiforge::api_server_schema;
use td_common::error::TdError;
use td_common::id::Id;
use td_common::uri::Version;
use utoipa::IntoParams;

pub const COLLECTIONS: &str = "/collections";
pub const COLLECTION: &str = concat!(COLLECTIONS, "/{collection}");

pub const LIST_COLLECTIONS: &str = COLLECTIONS;
pub const GET_COLLECTION: &str = COLLECTION;
pub const CREATE_COLLECTION: &str = COLLECTIONS;
pub const UPDATE_COLLECTION: &str = COLLECTION;
pub const DELETE_COLLECTION: &str = COLLECTION;

#[api_server_schema]
#[derive(Debug, Clone, Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
pub struct CollectionParam {
    /// Collection name URL path parameter
    pub collection: String,
}

impl CollectionParam {
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
        }
    }
}

impl From<&CollectionParam> for CollectionParam {
    fn from(params: &CollectionParam) -> Self {
        params.clone()
    }
}

impl From<CollectionParam> for CollectionName {
    fn from(params: CollectionParam) -> Self {
        CollectionName::new(&params.collection)
    }
}

impl Creator<CollectionParam> for CollectionName {
    fn create(value: impl Into<CollectionParam>) -> Self {
        CollectionName::new(value.into().collection)
    }
}

pub const FUNCTIONS: &str = "/collections/{collection}/functions";
pub const FUNCTION: &str = concat!(FUNCTIONS, "/{function}");

pub const FUNCTION_CREATE: &str = FUNCTIONS;
pub const FUNCTION_GET: &str = FUNCTION;
pub const FUNCTION_DELETE: &str = FUNCTION;
pub const FUNCTION_LIST: &str = FUNCTIONS;
pub const FUNCTION_UPDATE: &str = FUNCTION;
pub const FUNCTION_UPLOAD: &str = concat!(FUNCTION, "/upload/{function_id}");
pub const FUNCTION_HISTORY: &str = concat!(FUNCTION, "/history");
pub const FUNCTION_EXECUTE: &str = concat!(FUNCTION, "/execute");

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, IntoParams)]
#[getset(get = "pub")]
pub struct FunctionParam {
    collection: String,
    function: String,
}

impl FunctionParam {
    pub fn new(collection: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            function: dataset.into(),
        }
    }
}

impl From<&FunctionParam> for FunctionParam {
    fn from(value: &FunctionParam) -> Self {
        value.clone()
    }
}

impl From<FunctionParam> for CollectionName {
    fn from(params: FunctionParam) -> Self {
        CollectionName::new(params.collection)
    }
}

impl From<FunctionParam> for DatasetName {
    fn from(params: FunctionParam) -> Self {
        DatasetName::new(params.function)
    }
}

impl Creator<FunctionParam> for CollectionName {
    fn create(value: impl Into<FunctionParam>) -> Self {
        CollectionName::new(value.into().collection())
    }
}

impl Creator<FunctionParam> for DatasetName {
    fn create(value: impl Into<FunctionParam>) -> Self {
        DatasetName::new(value.into().function())
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
#[allow(dead_code)]
pub struct FunctionIdParam {
    collection: String,
    function: String,
    function_id: String,
}

impl FunctionIdParam {
    pub fn new(
        collection: impl Into<String>,
        dataset: impl Into<String>,
        function_id: impl Into<String>,
    ) -> Self {
        Self {
            collection: collection.into(),
            function: dataset.into(),
            function_id: function_id.into(),
        }
    }
}

pub const TABLES: &str = "/collections/{collection}/tables";
pub const TABLES_LIST: &str = TABLES;
pub const TABLE: &str = concat!(TABLES, "/{table}");
pub const TABLE_SCHEMA: &str = concat!(TABLE, "/schema");
pub const TABLE_SAMPLE: &str = concat!(TABLE, "/sample");
pub const TABLE_DATA: &str = concat!(TABLE, "/data");

pub const EXECUTION_PLANS: &str = "/execution_plans";
pub const EXECUTION_PLAN: &str = concat!(EXECUTION_PLANS, "/{execution_plan_id}");
pub const EXECUTION_PLAN_GET: &str = EXECUTION_PLAN;
pub const EXECUTION_PLANS_LIST: &str = EXECUTION_PLANS;

#[api_server_schema]
#[derive(Debug, Clone, Deserialize, Getters, IntoParams)]
#[getset(get = "pub")]
#[allow(dead_code)]
pub struct ExecutionPlanIdParam {
    execution_plan_id: String,
}

impl ExecutionPlanIdParam {
    pub fn new(execution_plan_id: impl Into<String>) -> Self {
        Self {
            execution_plan_id: execution_plan_id.into(),
        }
    }
}

impl From<&ExecutionPlanIdParam> for ExecutionPlanIdParam {
    fn from(value: &ExecutionPlanIdParam) -> Self {
        value.clone()
    }
}

impl From<ExecutionPlanIdParam> for ExecutionPlanId {
    fn from(params: ExecutionPlanIdParam) -> Self {
        ExecutionPlanId::new(params.execution_plan_id)
    }
}

impl Creator<ExecutionPlanIdParam> for ExecutionPlanId {
    fn create(value: impl Into<ExecutionPlanIdParam>) -> Self {
        ExecutionPlanId::new(value.into().execution_plan_id())
    }
}

pub const TRANSACTIONS: &str = "/transactions";
pub const TRANSACTION: &str = concat!(TRANSACTIONS, "/{transaction_id}");
pub const TRANSACTION_CANCEL: &str = concat!(TRANSACTION, "/cancel");
pub const TRANSACTION_RECOVER: &str = concat!(TRANSACTION, "/recover");
pub const TRANSACTIONS_LIST: &str = TRANSACTIONS;

pub const COMMITS: &str = "/commits";
pub const COMMITS_LIST: &str = COMMITS;

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, IntoParams)]
#[getset(get = "pub")]
pub struct TableParam {
    collection: String,
    table: String,
}

impl TableParam {
    pub fn new(collection: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            table: table.into(),
        }
    }
}

impl From<&TableParam> for TableParam {
    fn from(value: &TableParam) -> Self {
        value.clone()
    }
}

impl From<TableParam> for CollectionName {
    fn from(params: TableParam) -> Self {
        CollectionName::new(params.collection)
    }
}

impl Creator<TableParam> for CollectionName {
    fn create(value: impl Into<TableParam>) -> Self {
        CollectionName::new(value.into().collection())
    }
}

impl Creator<TableParam> for TableName {
    fn create(value: impl Into<TableParam>) -> Self {
        TableName::new(value.into().table())
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, Validate, IntoParams)]
#[getset(get = "pub")]
pub struct AtParam {
    #[serde(default = "AtParam::none")]
    at_version: Option<String>,
    #[serde(default = "AtParam::none")]
    at_commit: Option<String>,
    #[serde(default = "AtParam::none")]
    at_time: Option<String>,
}

#[derive(Debug, Clone)]
pub enum At {
    Version(Version),
    Commit(Id),
    Time(DateTime<Utc>),
}

#[td_error::td_error]
pub enum TableAtParamError {
    #[error("Only one alternative option can be provided")]
    OnlyOneAltOptionCanBeProvided = 0,
    #[error("Datetime must be <yyyy>-<mm>-<dd>T<HH>:<MM>:<SS>.<mmm>Z")]
    InvalidDateTimeFormat(#[from] ParseError) = 1,
}

impl TryInto<At> for &AtParam {
    type Error = TdError;

    fn try_into(self) -> Result<At, Self::Error> {
        let count = self.at_version.as_ref().map(|_| 1).unwrap_or(0)
            + self.at_commit.as_ref().map(|_| 1).unwrap_or(0)
            + self.at_time.as_ref().map(|_| 1).unwrap_or(0);
        match count {
            c if c <= 1 => get_table_at(self),
            _ => Err(TableAtParamError::OnlyOneAltOptionCanBeProvided)?,
        }
    }
}

pub const DATE_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

fn get_table_at(at: &AtParam) -> Result<At, TdError> {
    let at = if let Some(version) = &at.at_version {
        At::Version(Version::parse(version)?)
    } else if let Some(commit) = &at.at_commit {
        At::Commit(Id::try_from(commit)?)
    } else if let Some(time) = &at.at_time {
        At::Time(
            NaiveDateTime::parse_from_str(time, DATE_TIME_FORMAT)
                .map_err(TableAtParamError::InvalidDateTimeFormat)?
                .and_utc(),
        )
    } else {
        At::Version(Version::Head(0))
    };
    Ok(at)
}

impl AtParam {
    pub fn version(version: impl Into<Option<String>>) -> Self {
        Self {
            at_version: version.into(),
            at_commit: None,
            at_time: None,
        }
    }

    pub fn commit(commit: impl Into<String>) -> Self {
        Self {
            at_version: None,
            at_commit: Some(commit.into()),
            at_time: None,
        }
    }

    pub fn time(time: impl Into<String>) -> Self {
        Self {
            at_version: None,
            at_commit: None,
            at_time: Some(time.into()),
        }
    }

    fn none() -> Option<String> {
        None
    }
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct TableCommitParam {
    collection: String,
    table: String,
    at: At,
}

impl TableCommitParam {
    pub fn new(table: &TableParam, at: &AtParam) -> Result<Self, TdError> {
        Ok(Self {
            collection: table.collection.clone(),
            table: table.table.clone(),
            at: at.try_into()?,
        })
    }
}

impl From<&TableCommitParam> for TableCommitParam {
    fn from(value: &TableCommitParam) -> Self {
        value.clone()
    }
}

impl From<TableCommitParam> for CollectionName {
    fn from(params: TableCommitParam) -> Self {
        CollectionName::new(params.collection)
    }
}

impl From<TableCommitParam> for TableName {
    fn from(params: TableCommitParam) -> Self {
        TableName::new(params.table)
    }
}

impl Creator<TableCommitParam> for CollectionName {
    fn create(value: impl Into<TableCommitParam>) -> Self {
        CollectionName::new(value.into().collection())
    }
}

impl Creator<TableCommitParam> for TableName {
    fn create(value: impl Into<TableCommitParam>) -> Self {
        TableName::new(value.into().table())
    }
}

impl Creator<TableCommitParam> for At {
    fn create(value: impl Into<TableCommitParam>) -> Self {
        value.into().at
    }
}

pub const WORKERS: &str = "/workers";
pub const WORKER: &str = concat!(WORKERS, "/{worker_id}");

pub const LIST_WORKERS: &str = WORKERS;
pub const WORKER_LOGS: &str = concat!(WORKER, "/logs");

// TODO this should be managed by filters once we have them
#[api_server_schema]
#[derive(Debug, Clone, Default, Getters, Deserialize, IntoParams)]
#[getset(get = "pub")]
pub struct ByParam {
    by_function_id: Option<String>,
    by_transaction_id: Option<String>,
    by_execution_plan_id: Option<String>,
    by_data_version_id: Option<String>,
}

impl ByParam {
    pub fn function_id(function_id: impl Into<String>) -> Self {
        Self {
            by_function_id: Some(function_id.into()),
            ..Default::default()
        }
    }

    pub fn transaction_id(transaction_id: impl Into<String>) -> Self {
        Self {
            by_transaction_id: Some(transaction_id.into()),
            ..Default::default()
        }
    }

    pub fn execution_plan_id(execution_plan_id: impl Into<String>) -> Self {
        Self {
            by_execution_plan_id: Some(execution_plan_id.into()),
            ..Default::default()
        }
    }

    pub fn data_version_id(data_version_id: impl Into<String>) -> Self {
        Self {
            by_data_version_id: Some(data_version_id.into()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub enum By {
    FunctionId(Id),
    TransactionId(Id),
    ExecutionPlanId(Id),
    DataVersionId(Id),
}

#[td_error::td_error]
pub enum ByParamError {
    #[error("One alternative option must be provided")]
    OneAltOptionMustBeProvided = 0,
}

impl TryInto<By> for &ByParam {
    type Error = TdError;

    fn try_into(self) -> Result<By, Self::Error> {
        let mut iter = [
            self.by_function_id
                .as_ref()
                .map(|id| Id::try_from(id).map(By::FunctionId)),
            self.by_transaction_id
                .as_ref()
                .map(|id| Id::try_from(id).map(By::TransactionId)),
            self.by_execution_plan_id
                .as_ref()
                .map(|id| Id::try_from(id).map(By::ExecutionPlanId)),
            self.by_data_version_id
                .as_ref()
                .map(|id| Id::try_from(id).map(By::DataVersionId)),
        ]
        .into_iter()
        .flatten();

        if let (Some(by), None) = (iter.next(), iter.next()) {
            Ok(by?)
        } else {
            Err(ByParamError::OneAltOptionMustBeProvided)?
        }
    }
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct WorkerMessageListParam {
    by: By,
}

impl WorkerMessageListParam {
    pub fn new(by: &ByParam) -> Result<Self, TdError> {
        Ok(Self { by: by.try_into()? })
    }
}

impl From<&WorkerMessageListParam> for WorkerMessageListParam {
    fn from(value: &WorkerMessageListParam) -> Self {
        value.clone()
    }
}

impl Creator<WorkerMessageListParam> for By {
    fn create(value: impl Into<WorkerMessageListParam>) -> Self {
        value.into().by
    }
}

#[api_server_schema]
#[derive(Debug, Clone, Getters, Deserialize, IntoParams)]
#[getset(get = "pub")]
pub struct WorkerMessageParam {
    worker_id: String,
}

impl WorkerMessageParam {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
        }
    }
}

impl From<&WorkerMessageParam> for WorkerMessageParam {
    fn from(value: &WorkerMessageParam) -> Self {
        value.clone()
    }
}

impl Creator<WorkerMessageParam> for WorkerMessageId {
    fn create(value: impl Into<WorkerMessageParam>) -> Self {
        WorkerMessageId::new(value.into().worker_id())
    }
}
