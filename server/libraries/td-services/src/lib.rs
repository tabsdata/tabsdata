//
// Copyright 2025 Tabs Data Inc.
//

use crate::auth::services::AuthServices;
use crate::collection::service::CollectionServices;
use crate::execution::services::ExecutionServices;
use crate::execution::services::runtime_info::RuntimeContext;
use crate::function::services::FunctionServices;
use crate::function_run::services::FunctionRunServices;
use crate::inter_coll_permission::services::InterCollectionPermissionServices;
use crate::logical::services::LogicalServices;
use crate::permission::services::PermissionServices;
use crate::role::services::RoleServices;
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
use td_authz::jwt::JwtConfig;
use td_authz::session_provider::Sessions;
use td_database::pool::TdPool;
use td_execution::SchedulerNotify;
use td_execution::TransactionBy;
use td_objects::types::addresses::ApiServerAddresses;
use td_security::config::PasswordHashingConfig;
use td_storage::Storage;

pub mod auth;
pub mod collection;
pub mod execution;
pub mod function;
pub mod function_run;
pub mod graph;
pub mod inter_coll_permission;
pub mod logical;
pub mod permission;
pub mod role;
pub mod scheduler;
pub mod system;
pub mod table;
pub mod transaction;
pub mod user;
pub mod user_role;
pub mod worker;

pub mod do_authz;

#[derive(ServiceFactory, FieldAccessors, FromRef, Clone)]
pub struct Services {
    auth: Arc<AuthServices>,
    collection: Arc<CollectionServices>,
    execution: Arc<ExecutionServices>,
    logical: Arc<LogicalServices>,
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
    pub db: TdPool,
    pub server_addresses: Arc<ApiServerAddresses>,
    pub jwt_config: Arc<JwtConfig>,
    pub auth_context: Arc<AuthzContext>,
    pub sessions: Arc<Sessions>,
    pub password_settings: Arc<PasswordHashingConfig>,
    pub ssl_folder: Arc<PathBuf>,
    pub storage: Arc<Storage>,
    pub runtime_context: Arc<RuntimeContext>,
    pub transaction_by: Arc<TransactionBy>,
    pub scheduler_notify: Arc<SchedulerNotify>,
}

#[cfg(feature = "test-utils")]
impl Context {
    pub fn with_defaults(db: TdPool) -> Self {
        Self {
            db,
            server_addresses: Arc::new(ApiServerAddresses::default()),
            jwt_config: Arc::new(JwtConfig::default()),
            auth_context: Arc::new(AuthzContext::default()),
            sessions: Arc::new(Sessions::default()),
            password_settings: Arc::new(PasswordHashingConfig::default()),
            ssl_folder: Arc::new(PathBuf::default()),
            storage: Arc::new(Storage::default()),
            runtime_context: Arc::new(RuntimeContext::default()),
            transaction_by: Arc::new(TransactionBy::default()),
            scheduler_notify: Arc::new(SchedulerNotify::new()),
        }
    }
}
