//
// Copyright 2025 Tabs Data Inc.
//

use crate::auth::jwt::JwtConfig;
use crate::auth::services::AuthServices;
use crate::auth::session::Sessions;
use crate::collection::service::CollectionServices;
use crate::execution::services::ExecutionServices;
use crate::execution::services::runtime_info::RuntimeContext;
use crate::function::services::FunctionServices;
use crate::function_run::services::FunctionRunServices;
use crate::inter_coll_permission::services::InterCollectionPermissionServices;
use crate::permission::services::PermissionServices;
use crate::role::services::RoleServices;
use crate::scheduler::services::ScheduleServices;
use crate::system::services::SystemServices;
use crate::table::services::TableServices;
use crate::transaction::services::TransactionServices;
use crate::user::service::UserServices;
use crate::user_role::services::UserRoleServices;
use crate::worker::services::WorkerServices;
use axum::extract::FromRef;
use std::path::PathBuf;
use std::sync::Arc;
use ta_services::factory::{FieldAccessors, ServiceFactory};
use td_authz::AuthzContext;
use td_common::server::FileWorkerMessageQueue;
use td_database::sql::DbPool;
use td_objects::sql::DaoQueries;
use td_objects::types::addresses::{ApiServerAddresses, InternalServerAddresses};
use td_security::config::PasswordHashingConfig;
use td_storage::Storage;
use te_execution::transaction::TransactionBy;

pub mod auth;
pub mod collection;
pub mod execution;
pub mod function;
pub mod function_run;
pub mod inter_coll_permission;
pub mod permission;
pub mod role;
pub mod scheduler;
pub mod system;
pub mod table;
pub mod transaction;
pub mod user;
pub mod user_role;
pub mod worker;

#[derive(ServiceFactory, FieldAccessors, FromRef, Clone)]
pub struct Services {
    auth: Arc<AuthServices>,
    collection: Arc<CollectionServices>,
    execution: Arc<ExecutionServices>,
    function: Arc<FunctionServices>,
    function_run: Arc<FunctionRunServices>,
    inter_coll_permission: Arc<InterCollectionPermissionServices>,
    permission: Arc<PermissionServices>,
    role: Arc<RoleServices>,
    system: Arc<SystemServices>,
    table: Arc<TableServices>,
    transaction: Arc<TransactionServices>,
    user: Arc<UserServices>,
    user_role: Arc<UserRoleServices>,
    worker: Arc<WorkerServices>,
}

#[derive(FieldAccessors, FromRef, Clone)]
pub struct Context {
    pub db: DbPool,
    pub queries: Arc<DaoQueries>,
    pub server_addresses: Arc<ApiServerAddresses>,
    pub jwt_config: Arc<JwtConfig>,
    pub auth_context: Arc<AuthzContext>,
    pub sessions: Arc<Sessions>,
    pub password_settings: Arc<PasswordHashingConfig>,
    pub ssl_folder: Arc<PathBuf>,
    pub storage: Arc<Storage>,
    pub runtime_context: Arc<RuntimeContext>,
    pub transaction_by: Arc<TransactionBy>,
}

#[cfg(feature = "test-utils")]
impl Context {
    pub fn with_defaults(db: DbPool) -> Self {
        Self {
            db,
            queries: Arc::new(DaoQueries::default()),
            server_addresses: Arc::new(ApiServerAddresses::default()),
            jwt_config: Arc::new(JwtConfig::default()),
            auth_context: Arc::new(AuthzContext::default()),
            sessions: Arc::new(Sessions::default()),
            password_settings: Arc::new(PasswordHashingConfig::default()),
            ssl_folder: Arc::new(PathBuf::default()),
            storage: Arc::new(Storage::default()),
            runtime_context: Arc::new(RuntimeContext::default()),
            transaction_by: Arc::new(TransactionBy::default()),
        }
    }
}

#[derive(ServiceFactory, FromRef, Clone)]
pub struct SchedulerServices {
    schedule: Arc<ScheduleServices>,
}

#[derive(FieldAccessors, FromRef, Clone)]
pub struct SchedulerContext {
    pub db: DbPool,
    pub queries: Arc<DaoQueries>,
    pub storage: Arc<Storage>,
    pub worker_queue: Arc<FileWorkerMessageQueue>,
    pub internal_addresses: Arc<InternalServerAddresses>,
}

#[cfg(feature = "test-utils")]
impl SchedulerContext {
    pub fn with_defaults(db: DbPool) -> Self {
        Self {
            db,
            queries: Arc::new(DaoQueries::default()),
            storage: Arc::new(Storage::default()),
            worker_queue: Arc::new(FileWorkerMessageQueue::default()),
            internal_addresses: Arc::new(InternalServerAddresses::default()),
        }
    }
}
