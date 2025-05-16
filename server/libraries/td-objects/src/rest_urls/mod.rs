//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtMulti, CollectionIdName, ExecutionIdName, FunctionIdName, FunctionRunId,
    FunctionVersionIdName, InterCollectionPermissionIdName, PermissionIdName, RoleIdName,
    SampleLen, SampleOffset, TableIdName, TransactionIdName, UserIdName,
};
use chrono::{DateTime, NaiveDateTime, ParseError, Utc};
use constcat::concat;
use getset::Getters;
use serde::Deserialize;
use serde_valid::Validate;
use td_apiforge::apiserver_schema;
use td_common::id::Id;
use td_common::uri::Version;
use td_error::TdError;
use utoipa::IntoParams;

pub const AUTH: &str = "/auth";
pub const AUTH_LOGIN: &str = concat!(AUTH, "/login");
pub const AUTH_REFRESH: &str = concat!(AUTH, "/refresh");
pub const AUTH_ROLE_CHANGE: &str = concat!(AUTH, "/role_change");
pub const AUTH_LOGOUT: &str = concat!(AUTH, "/logout");
pub const AUTH_USER_INFO: &str = concat!(AUTH, "/info");
pub const AUTH_PASSWORD_CHANGE: &str = concat!(AUTH, "/password_change");

#[apiserver_schema]
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

pub const WORKERS: &str = "/workers";
pub const WORKER: &str = concat!(WORKERS, "/{worker_id}");

pub const LIST_WORKERS: &str = WORKERS;
pub const WORKER_LOGS: &str = concat!(WORKER, "/logs");

// TODO this should be managed by filters once we have them
#[apiserver_schema]
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

#[apiserver_schema]
#[derive(Debug, Clone, Getters, Deserialize, IntoParams)]
#[getset(get = "pub")]
pub struct WorkerMessageParam {
    worker_id: String,
}

// TODO here starts the refactored apiserver
macro_rules! url {
    ($( $path:expr $(,)? )*) => {
        concat!($( $path, )*)
    };
}

// Base URL
pub const BASE_API_URL: &str = url!("/api");

pub const V1: &str = url!("/v1");
pub const BASE_URL_V1: &str = url!(BASE_API_URL, V1);

pub const BASE_URL: &str = BASE_URL_V1;

// OpenApi URLs
pub const DOCS_URL: &str = url!(BASE_API_URL, "/docs");
pub const OPENAPI_JSON_URL: &str = url!(BASE_API_URL, "/api-docs/openapi.json");

// Private URLs
pub const INTERNAL_PREFIX: &str = url!("/internal");
pub const UPDATE_FUNCTION_RUN: &str = url!(INTERNAL_PREFIX, "/function_run/{function_run_id}");

#[td_type::UrlParam]
pub struct FunctionRunParam {
    #[td_type(extractor)]
    function_run_id: FunctionRunId,
}

// Endpoints URLs

// Users
pub const USERS: &str = url!("/users");
pub const USER: &str = url!("/{user}");

#[td_type::UrlParam]
pub struct UserParam {
    #[td_type(extractor)]
    user: UserIdName,
}

// Roles
pub const ROLES: &str = url!("/roles");
pub const ROLE: &str = url!(ROLES, "/{role}");

#[td_type::UrlParam]
pub struct RoleParam {
    #[td_type(extractor)]
    role: RoleIdName,
}

pub const LIST_ROLES: &str = url!(ROLES);
pub const GET_ROLE: &str = url!(ROLE);
pub const CREATE_ROLE: &str = url!(ROLES);
pub const UPDATE_ROLE: &str = url!(ROLE);
pub const DELETE_ROLE: &str = url!(ROLE);

// Permissions
pub const PERMISSIONS: &str = url!(ROLE, "/permissions");
pub const PERMISSION: &str = url!(PERMISSIONS, "/{permission}");

#[td_type::UrlParam]
pub struct RolePermissionParam {
    #[td_type(extractor)]
    role: RoleIdName,
    #[td_type(extractor)]
    permission: PermissionIdName,
}

pub const LIST_PERMISSIONS: &str = url!(PERMISSIONS);
pub const CREATE_PERMISSION: &str = url!(PERMISSIONS);
pub const DELETE_PERMISSION: &str = url!(PERMISSION);

// User roles
pub const USER_ROLES: &str = url!(ROLE, "/users");
pub const USER_ROLE: &str = url!(USER_ROLES, "/{user}");

#[td_type::UrlParam]
pub struct UserRoleParam {
    #[td_type(extractor)]
    role: RoleIdName,
    #[td_type(extractor)]
    user: UserIdName,
}

pub const LIST_USER_ROLES: &str = url!(USER_ROLES);
pub const GET_USER_ROLE: &str = url!(USER_ROLE);
pub const CREATE_USER_ROLE: &str = url!(USER_ROLES);
pub const DELETE_USER_ROLE: &str = url!(USER_ROLE);

// Collections
pub const COLLECTIONS: &str = url!("/collections");
pub const COLLECTION: &str = url!(COLLECTIONS, "/{collection}");

#[td_type::UrlParam]
pub struct CollectionParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
}

pub const LIST_COLLECTIONS: &str = url!(COLLECTIONS);
pub const GET_COLLECTION: &str = url!(COLLECTION);
pub const CREATE_COLLECTION: &str = url!(COLLECTIONS);
pub const UPDATE_COLLECTION: &str = url!(COLLECTION);
pub const DELETE_COLLECTION: &str = url!(COLLECTION);

#[td_type::UrlParam]
pub struct InterCollectionPermissionParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    permission: InterCollectionPermissionIdName,
}

pub const INTER_COLLECTION_PERMISSIONS: &str = url!(COLLECTION, "/inter-collection-permissions");
pub const INTER_COLLECTION_PERMISSION: &str = url!(INTER_COLLECTION_PERMISSIONS, "/{permission}");

pub const LIST_INTER_COLLECTION_PERMISSIONS: &str = url!(INTER_COLLECTION_PERMISSIONS);
pub const CREATE_INTER_COLLECTION_PERMISSION: &str = url!(INTER_COLLECTION_PERMISSIONS);
pub const DELETE_INTER_COLLECTION_PERMISSION: &str = url!(INTER_COLLECTION_PERMISSION);

// Functions
pub const FUNCTIONS: &str = url!(COLLECTION, "/functions");
pub const FUNCTION: &str = url!(FUNCTIONS, "/{function}");

#[td_type::UrlParam]
pub struct FunctionParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    function: FunctionIdName,
}

pub const FUNCTION_CREATE: &str = url!(FUNCTIONS);
pub const FUNCTION_GET: &str = url!(FUNCTION);
pub const FUNCTION_DELETE: &str = url!(FUNCTION);
pub const FUNCTION_LIST: &str = url!(FUNCTIONS);
pub const FUNCTION_UPDATE: &str = url!(FUNCTION);
pub const FUNCTION_UPLOAD: &str = url!(COLLECTION, "/function-bundle-upload");

pub const FUNCTION_HISTORY: &str = url!(FUNCTION, "/history");
pub const FUNCTION_EXECUTE: &str = url!(FUNCTION, "/execute");

// Function versions
pub const FUNCTION_VERSIONS: &str = url!(COLLECTION, "/function_versions");
pub const FUNCTION_VERSION: &str = url!(FUNCTION_VERSIONS, "/{function_version}");

#[td_type::UrlParam]
pub struct FunctionVersionParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    function_version: FunctionVersionIdName,
}

pub const FUNCTION_VERSION_GET: &str = url!(FUNCTION_VERSION);

#[td_type::QueryParam]
pub struct AtMultiParam {
    #[td_type(extractor)]
    #[serde(default)]
    at: AtMulti,
}

#[td_type::QueryParam]
pub struct SampleOffsetLenParam {
    #[td_type(extractor)]
    #[serde(default)]
    offset: SampleOffset,
    #[td_type(extractor)]
    #[serde(default)]
    len: SampleLen,
}

// Tables
pub const TABLES: &str = url!(COLLECTION, "/tables");
pub const TABLE: &str = url!(TABLES, "/{table}");

#[td_type::UrlParam]
pub struct TableParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    table: TableIdName,
}

pub const LIST_TABLES: &str = url!(TABLES);
pub const LIST_TABLE_DATA_VERSIONS: &str = url!(TABLE, "/data-versions");
pub const SCHEMA_TABLE: &str = url!(TABLE, "/schema");
pub const SAMPLE_TABLE: &str = url!(TABLE, "/sample");
pub const DOWNLOAD_TABLE: &str = url!(TABLE, "/download");

pub const TABLE_DELETE: &str = url!(TABLE);

// Executions
pub const EXECUTIONS: &str = url!(COLLECTION, "/executions");
pub const EXECUTION: &str = url!(EXECUTIONS, "/{execution}");

#[td_type::UrlParam]
pub struct ExecutionParam {
    #[td_type(extractor)]
    execution: ExecutionIdName,
}

pub const EXECUTION_CANCEL: &str = concat!(EXECUTION, "/cancel");
pub const EXECUTION_RECOVER: &str = concat!(EXECUTION, "/recover");

// Transactions
pub const TRANSACTIONS: &str = url!(COLLECTION, "/transactions");
pub const TRANSACTION: &str = url!(TRANSACTIONS, "/{transaction}");

#[td_type::UrlParam]
pub struct TransactionParam {
    #[td_type(extractor)]
    transaction: TransactionIdName,
}

pub const TRANSACTION_CANCEL: &str = concat!(TRANSACTION, "/cancel");
pub const TRANSACTION_RECOVER: &str = concat!(TRANSACTION, "/recover");
pub const TRANSACTIONS_LIST: &str = TRANSACTIONS;
