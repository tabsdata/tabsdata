//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, CollectionIdName, ExecutionIdName, FunctionIdName, FunctionRunId,
    InterCollectionPermissionIdName, LogsCastNumber, PermissionIdName, RoleIdName, SampleLen,
    SampleOffset, Sql, TableIdName, TransactionIdName, UserIdName, WorkerIdName,
};
use constcat::concat;
use td_common::logging::LOG_EXTENSION;
use td_common::server::{ERR_LOG_FILE_NAME, FN_LOG_FILE_NAME, OUT_LOG_FILE_NAME, TD_LOG_FILE_NAME};

#[macro_export]
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
pub const AUTH_LOGIN: &str = url!(AUTH, "/login");
pub const AUTH_REFRESH: &str = url!(AUTH, "/refresh");
pub const AUTH_ROLE_CHANGE: &str = url!(AUTH, "/role_change");
pub const AUTH_LOGOUT: &str = url!(AUTH, "/logout");
pub const AUTH_USER_INFO: &str = url!(AUTH, "/info");
pub const AUTH_PASSWORD_CHANGE: &str = url!(AUTH, "/password_change");

pub const CERT_DOWNLOAD: &str = url!("/ssl-cert");

// Users
pub const USERS: &str = url!("/users");
pub const USER: &str = url!(USERS, "/{user}");

#[td_type::UrlParam]
pub struct UserParam {
    #[td_type(extractor)]
    user: UserIdName,
}

pub const LIST_USERS: &str = url!(USERS);
pub const GET_USER: &str = url!(USER);
pub const CREATE_USER: &str = url!(USERS);
pub const UPDATE_USER: &str = url!(USER);
pub const DELETE_USER: &str = url!(USER);

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
pub const FUNCTION_LIST_BY_COLL: &str = url!(FUNCTIONS);
pub const FUNCTION_LIST: &str = url!("/functions");
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

#[td_type::QueryParam]
pub struct FileFormatParam {
    #[td_type(extractor)]
    #[serde(default)]
    format: FileFormat,
}

#[td_type::typed_enum]
#[derive(Default)]
pub enum FileFormat {
    #[default]
    Parquet,
    Csv,
    Json,
}

#[td_type::QueryParam]
pub struct SqlParam {
    #[td_type(extractor)]
    #[serde(default)]
    sql: Option<Sql>,
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

pub const LIST_TABLES_BY_COLL: &str = url!(TABLES);
pub const LIST_TABLES: &str = url!("/tables");
pub const LIST_TABLE_DATA_VERSIONS: &str = url!(TABLE, "/data-versions");
pub const SCHEMA_TABLE: &str = url!(TABLE, "/schema");
pub const SAMPLE_TABLE: &str = url!(TABLE, "/sample");
pub const DOWNLOAD_TABLE: &str = url!(TABLE, "/download");

pub const TABLE_DELETE: &str = url!(TABLE);

// Server status
pub const SERVER_STATUS: &str = url!("/status");
pub const RUNTIME_INFO: &str = url!("/runtime-info");

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
pub const EXECUTION_READ: &str = url!(EXECUTION);
pub const EXECUTION_DETAILS: &str = url!(EXECUTION, "/details");
pub const EXECUTION_LIST: &str = url!(EXECUTIONS);

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
pub const TRANSACTIONS_LIST: &str = url!(TRANSACTIONS);

// Synchrotron
pub const SYNCHROTRON_READ: &str = url!("/synchrotron");

// Worker messages
pub const WORKERS: &str = url!("/workers");
pub const WORKER: &str = url!(WORKERS, "/{worker}");

#[td_type::UrlParam]
pub struct WorkerParam {
    #[td_type(extractor)]
    worker: WorkerIdName,
}

pub const WORKERS_LIST: &str = url!(WORKERS);
pub const WORKER_LOGS: &str = url!(WORKER, "/logs");

#[td_type::typed_enum]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[derive(Default)]
pub enum LogsExtension {
    #[default]
    All,
    Err,
    Fn,
    Out,
    Td,
}

impl LogsExtension {
    pub fn glob_pattern(&self) -> String {
        match self {
            LogsExtension::All => format!("*.{LOG_EXTENSION}"), // Note that it might get more than the "known" log files.
            LogsExtension::Err => format!("{ERR_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Fn => format!("{FN_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Out => format!("{OUT_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
            LogsExtension::Td => format!("{TD_LOG_FILE_NAME}*.{LOG_EXTENSION}"),
        }
    }

    #[cfg(feature = "test-utils")]
    pub fn files(&self, rotations: usize) -> Vec<std::path::PathBuf> {
        fn log_file(name: &str, rotation: usize) -> std::path::PathBuf {
            let ext = if rotation == 1 {
                format!(".{LOG_EXTENSION}")
            } else {
                format!("_{}.{}", rotation - 1, LOG_EXTENSION)
            };
            format!("{name}{ext}").into()
        }

        let mut log_files = Vec::new();
        for rotation in 1..=rotations {
            match self {
                LogsExtension::All => {
                    log_files.push(log_file(ERR_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(FN_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(OUT_LOG_FILE_NAME, rotation));
                    log_files.push(log_file(TD_LOG_FILE_NAME, rotation));
                }
                LogsExtension::Err => log_files.push(log_file(ERR_LOG_FILE_NAME, rotation)),
                LogsExtension::Fn => log_files.push(log_file(FN_LOG_FILE_NAME, rotation)),
                LogsExtension::Out => log_files.push(log_file(OUT_LOG_FILE_NAME, rotation)),
                LogsExtension::Td => log_files.push(log_file(TD_LOG_FILE_NAME, rotation)),
            }
        }

        log_files
    }
}

#[td_type::QueryParam]
pub struct WorkerLogsQueryParams {
    #[td_type(extractor)]
    #[serde(default = "extension_default")]
    extension: Vec<LogsExtension>,
    #[td_type(extractor)]
    #[serde(default)]
    /// Empty means all retries available.
    retry: Vec<LogsCastNumber>,
}

fn extension_default() -> Vec<LogsExtension> {
    vec![LogsExtension::All]
}

#[td_type::Dlo]
pub struct WorkerLogsParams {
    #[td_type(extractor)]
    worker: WorkerIdName,
    #[td_type(extractor)]
    extension: Vec<LogsExtension>,
    #[td_type(extractor)]
    retry: Vec<LogsCastNumber>,
}

impl WorkerLogsParams {
    pub fn new(path: WorkerParam, query: WorkerLogsQueryParams) -> Self {
        Self {
            worker: path.worker,
            extension: query.extension,
            retry: query.retry,
        }
    }
}

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
pub const FUNCTION_RUN_LIST: &str = url!("/function_runs");
