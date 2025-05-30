//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionIdName, ExecutionIdName, FunctionIdName, FunctionRunId,
    InterCollectionPermissionIdName, PermissionIdName, RoleIdName, SampleLen, SampleOffset,
    TableIdName, TransactionIdName, UserIdName, WorkerMessageIdName,
};
use constcat::concat;

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
pub struct FunctionRunIdParam {
    #[td_type(extractor)]
    function_run_id: FunctionRunId,
}

// Endpoints URLs

// Auth
pub const AUTH: &str = "/auth";
pub const AUTH_LOGIN: &str = concat!(AUTH, "/login");
pub const AUTH_REFRESH: &str = concat!(AUTH, "/refresh");
pub const AUTH_ROLE_CHANGE: &str = concat!(AUTH, "/role_change");
pub const AUTH_LOGOUT: &str = concat!(AUTH, "/logout");
pub const AUTH_USER_INFO: &str = concat!(AUTH, "/info");
pub const AUTH_PASSWORD_CHANGE: &str = concat!(AUTH, "/password_change");

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
#[td_type::QueryParam]
pub struct AtTimeParam {
    #[td_type(extractor)]
    #[serde(default)]
    at: AtTime,
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
pub const EXECUTIONS: &str = url!("/executions");
pub const EXECUTION: &str = url!(EXECUTIONS, "/{execution}");

#[td_type::UrlParam]
pub struct ExecutionParam {
    #[td_type(extractor)]
    execution: ExecutionIdName,
}

pub const EXECUTION_CANCEL: &str = url!(EXECUTION, "/cancel");
pub const EXECUTION_RECOVER: &str = url!(EXECUTION, "/recover");
pub const EXECUTION_LIST: &str = EXECUTIONS;

// Transactions
pub const TRANSACTIONS: &str = url!("/transactions");
pub const TRANSACTION: &str = url!(TRANSACTIONS, "/{transaction}");

#[td_type::UrlParam]
pub struct TransactionParam {
    #[td_type(extractor)]
    transaction: TransactionIdName,
}

pub const TRANSACTION_CANCEL: &str = url!(TRANSACTION, "/cancel");
pub const TRANSACTION_RECOVER: &str = url!(TRANSACTION, "/recover");
pub const TRANSACTIONS_LIST: &str = TRANSACTIONS;

// Synchrotron
pub const SYNCHROTRON_READ: &str = url!("/synchrotron");

// Worker messages
pub const WORKERS: &str = url!("/workers");
pub const WORKER: &str = url!(WORKERS, "/{worker}");

#[td_type::UrlParam]
pub struct WorkerMessageParam {
    #[td_type(extractor)]
    worker: WorkerMessageIdName,
}

pub const WORKER_LOGS: &str = url!(WORKER_MESSAGE, "/logs");

// Function runs
pub const FUNCTION_RUNS: &str = url!(FUNCTION, "/executions");
pub const FUNCTION_RUN: &str = url!(FUNCTION_RUNS, "/{execution}");

#[td_type::UrlParam]
pub struct FunctionRunParam {
    #[td_type(extractor)]
    collection: CollectionIdName,
    #[td_type(extractor)]
    function: FunctionIdName,
    #[td_type(extractor)]
    execution: ExecutionIdName,
}

pub const FUNCTION_RUN_GET: &str = url!(FUNCTION_RUN);
