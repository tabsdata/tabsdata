//
// Copyright 2025 Tabs Data Inc.
//

//! API Server generator. Any number of routers might be added, with any number of layer per
//! router. Specifics of each router are defined in their respective modules.
//!
//! Layers go from general to specific. Following axum middleware documentation, for the layer
//! we use in [`users`]:
//! ```json
//!                    requests
//!                       |
//!                       v
//!         --------- TraceService ---------
//!          -------- CorsService ---------
//!           ------- ............ -------
//!            ---- JwtDecoderService ----   <--- RequestContext
//!                  users.router()
//!              ----- AdminOnly -----
//!
//!                   list_users
//!
//!              ----- AdminOnly -----
//!                 users.router()
//!            ---- JwtDecoderService ----
//!           ------- ............ -------
//!          -------- CorsService ---------
//!         --------- TraceService ---------
//!                       |
//!                       v
//!                    responses
//! ```

mod auth;
mod collections;
mod executions;
mod functions;
mod inter_collection_permissions;

mod function_runs;
mod internal;
#[cfg(feature = "api-docs")]
mod openapi;
mod permissions;
mod roles;
mod runtime_info;
pub mod scheduler_server;
mod server_status;
mod status;
mod tables;
mod transactions;
mod user_roles;
mod users;
mod workers;

use crate::config::Config;
use crate::layers::cors::CorsService;
use crate::layers::tracing::TraceService;
use crate::layers::uri_filter::LoopbackIpFilterService;
use crate::router::auth::authorization_layer::authorization_layer;
use crate::router::auth::{auth_secure, auth_unsecure};
use crate::router::status::StatusLogic;
use crate::{Server, ServerBuilder, ServerError};
use axum::middleware::{from_fn, from_fn_with_state};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_security::config::PasswordHashingConfig;
use td_services::auth::services::{AuthServices, PasswordHashConfig};
use td_services::auth::session;
use td_services::auth::session::Sessions;
use td_services::collection::service::CollectionServices;
use td_services::execution::services::ExecutionServices;
pub use td_services::execution::RuntimeContext;
use td_services::function::services::FunctionServices;
use td_services::function_run::services::FunctionRunServices;
use td_services::inter_coll_permission::services::InterCollectionPermissionServices;
use td_services::permission::services::PermissionServices;
use td_services::role::services::RoleServices;
use td_services::table::services::TableServices;
use td_services::transaction::services::TransactionServices;
use td_services::user::service::UserServices;
use td_services::user_role::services::UserRoleServices;
use td_services::worker::services::WorkerServices;
use td_storage::Storage;
use te_apiserver::{ExtendedRouter, RouterExtension};
use tower_http::timeout::TimeoutLayer;

pub mod state {
    use super::*;

    pub type Auth = Arc<AuthServices>;
    pub type Collections = Arc<CollectionServices>;
    pub type Executions = Arc<ExecutionServices>;
    pub type Functions = Arc<FunctionServices>;
    pub type FunctionRuns = Arc<FunctionRunServices>;
    pub type Permissions = Arc<PermissionServices>;
    pub type InterCollectionPermissions = Arc<InterCollectionPermissionServices>;
    pub type Roles = Arc<RoleServices>;
    pub type Status = Arc<StatusLogic>;
    pub type Tables = Arc<TableServices>;
    pub type Transactions = Arc<TransactionServices>;
    pub type Users = Arc<UserServices>;
    pub type UserRoles = Arc<UserRoleServices>;
    pub type Workers = Arc<WorkerServices>;

    pub type StorageRef = Arc<Storage>;
}

pub struct ApiServerInstance {
    internal: Box<dyn Server>,
    api_v1: Box<dyn Server>,
}

impl ApiServerInstance {
    pub async fn api_v1_addresses(&self) -> Result<Vec<SocketAddr>, Box<dyn Error>> {
        self.api_v1
            .listeners()
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub async fn internal_addresses(&self) -> Result<Vec<SocketAddr>, Box<dyn Error>> {
        self.internal
            .listeners()
            .iter()
            .map(|listener| listener.local_addr())
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        tokio::try_join!(self.internal.run(), self.api_v1.run()).map(|_| ())
    }
}

pub struct ApiServerInstanceBuilder {
    config: Config,
    db: DbPool,
    authz_context: Arc<AuthzContext>,
    auth_services: Arc<AuthServices>,
    storage: Arc<Storage>,
    runtime_context: Arc<RuntimeContext>,
}

impl ApiServerInstanceBuilder {
    pub fn new(
        config: Config,
        db: DbPool,
        storage: Arc<Storage>,
        runtime_context: Arc<RuntimeContext>,
    ) -> Self {
        let sessions: Arc<Sessions> = Arc::new(session::new(db.clone()));

        // to verify up front configuration is OK.
        let password_hash_config: PasswordHashConfig = (&config).into();
        password_hash_config.hasher();

        let authz_context = Arc::new(AuthzContext::default());

        let auth_services: Arc<AuthServices> = Arc::new(AuthServices::new(
            &db,
            sessions.clone(),
            password_hash_config,
            config.jwt().clone(),
            config.ssl_folder().clone(),
        ));

        Self {
            config,
            db,
            authz_context,
            auth_services,
            storage,
            runtime_context,
        }
    }

    fn auth_state(&self) -> state::Auth {
        self.auth_services.clone()
    }

    fn collection_state(&self) -> state::Collections {
        Arc::new(CollectionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn execution_state(&self) -> state::Executions {
        Arc::new(ExecutionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
            self.runtime_context.clone(),
        ))
    }

    fn function_state(&self) -> state::Functions {
        Arc::new(FunctionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
            self.storage.clone(),
        ))
    }

    fn function_run_state(&self) -> state::FunctionRuns {
        Arc::new(FunctionRunServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn inter_collection_permissions_state(&self) -> state::InterCollectionPermissions {
        Arc::new(InterCollectionPermissionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn permissions_state(&self) -> state::Permissions {
        Arc::new(PermissionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn roles_state(&self) -> state::Roles {
        Arc::new(RoleServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn status_state(&self) -> state::Status {
        Arc::new(StatusLogic::new(self.db.clone()))
    }

    fn storage_state(&self) -> state::StorageRef {
        self.storage.clone()
    }

    fn table_state(&self) -> state::Tables {
        Arc::new(TableServices::new(
            self.db.clone(),
            self.authz_context.clone(),
            self.storage.clone(),
        ))
    }

    fn transaction_state(&self) -> state::Transactions {
        Arc::new(TransactionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn user_roles_state(&self) -> state::UserRoles {
        Arc::new(UserRoleServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn users_state(&self) -> state::Users {
        Arc::new(UserServices::new(
            self.db.clone(),
            Arc::new(PasswordHashingConfig::default()),
            self.authz_context.clone(),
        ))
    }

    fn workers_state(&self) -> state::Workers {
        Arc::new(WorkerServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    pub async fn build(&self) -> Result<ApiServerInstance, ServerError> {
        let api_v1 = {
            // API router
            let router = axum::Router::new()
                // unsecure endpoints
                .merge(axum::Router::new().merge(auth_unsecure::router(self.auth_state())))
                // secure endpoints
                .merge(
                    axum::Router::new()
                        .merge(auth_secure::router(self.auth_state()))
                        .merge(collections::router(self.collection_state()))
                        .merge(executions::router(self.execution_state()))
                        .merge(functions::router(self.function_state()))
                        .merge(function_runs::router(self.function_run_state()))
                        .merge(inter_collection_permissions::router(
                            self.inter_collection_permissions_state(),
                        ))
                        .merge(permissions::router(self.permissions_state()))
                        .merge(roles::router(self.roles_state()))
                        .merge(server_status::router(self.status_state()))
                        .merge(user_roles::router(self.user_roles_state()))
                        .merge(users::router(self.users_state()))
                        .merge(tables::router(self.table_state(), self.storage_state()))
                        .merge(transactions::router(self.transaction_state()))
                        .merge(workers::router(self.workers_state()))
                        .merge(runtime_info::router(self.execution_state()))
                        // authorization layer
                        .layer(from_fn_with_state(self.auth_state(), authorization_layer)),
                );

            // Nest the router in the V1 address.
            let router = axum::Router::new().nest(td_objects::rest_urls::BASE_URL_V1, router);

            // Add any router extensions (not part of the API).
            let mut router = router.merge(ExtendedRouter::router());

            // Add docs endpoints if the feature is enabled.
            #[cfg(feature = "api-docs")]
            {
                router = router.merge(openapi::router());
            }

            // Default layers
            let router = router
                .layer(TimeoutLayer::new(Duration::from_secs(
                    *self.config.request_timeout() as u64,
                )))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::new(self.config.addresses().clone(), router)
                .tls(self.config.ssl_folder())
                .build()
                .await
        }?;

        let internal = {
            // Internal router, only accessible from loopback IPs
            let router = axum::Router::new().merge(
                axum::Router::new()
                    .merge(internal::router(self.execution_state()))
                    // internal authorization layer
                    .layer(from_fn(LoopbackIpFilterService::layer)),
            );

            // Nest the router in the V1 address.
            let router = axum::Router::new().nest(td_objects::rest_urls::BASE_URL_V1, router);

            // Default layers
            let router = router
                .layer(TimeoutLayer::new(Duration::from_secs(
                    *self.config.request_timeout() as u64,
                )))
                .layer(CorsService::layer())
                .layer(TraceService::layer());

            ServerBuilder::new(self.config.internal_addresses().clone(), router)
                .build()
                .await
        }?;

        Ok(ApiServerInstance { internal, api_v1 })
    }
}
