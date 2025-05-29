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
mod execution;
mod functions;
mod inter_collection_permissions;

#[cfg(feature = "api-docs")]
mod openapi;
mod permissions;
mod roles;
pub mod scheduler_server;
mod server_status;
mod status;
mod tables;
mod user_roles;
mod users;

use crate::config::Config;
use crate::layers::cors::CorsService;
use crate::layers::timeout::TimeoutService;
use crate::layers::tracing::TraceService;
use crate::layers::uri_filter::LoopbackIpFilterService;
use crate::router::auth::authorization_layer::authorization_layer;
use crate::router::auth::{auth_secure, auth_unsecure};
use crate::router::execution::callback;
use crate::router::status::StatusLogic;
use crate::{apiserver, ApiServer};
use axum::middleware::{from_fn, from_fn_with_state};
use chrono::Duration;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_security::config::PasswordHashingConfig;
use td_services::auth::services::{AuthServices, PasswordHashConfig};
use td_services::auth::session;
use td_services::auth::session::Sessions;
use td_services::collections::service::CollectionServices;
use td_services::execution::services::ExecutionServices;
use td_services::function::services::FunctionServices;
use td_services::inter_coll_permission::services::InterCollectionPermissionServices;
use td_services::permission::services::PermissionServices;
use td_services::role::services::RoleServices;
use td_services::table::services::TableServices;
use td_services::user_role::services::UserRoleServices;
use td_services::users::service::UserServices;
use td_storage::Storage;
use te_apiserver::{ExtendedRouter, RouterExtension};

pub struct ApiServerInstance {
    config: Config,
    db: DbPool,
    authz_context: Arc<AuthzContext>,
    auth_services: Arc<AuthServices>,
    storage: Arc<Storage>,
}

pub mod state {
    use super::*;
    use td_services::table::services::TableServices;

    pub type Auth = Arc<AuthServices>;
    pub type Collections = Arc<CollectionServices>;
    pub type Execution = Arc<ExecutionServices>;
    pub type Functions = Arc<FunctionServices>;
    pub type Permissions = Arc<PermissionServices>;
    pub type InterCollectionPermissions = Arc<InterCollectionPermissionServices>;
    pub type Roles = Arc<RoleServices>;
    pub type Status = Arc<StatusLogic>;
    pub type Tables = Arc<TableServices>;
    pub type Users = Arc<UserServices>;
    pub type UserRoles = Arc<UserRoleServices>;

    pub type StorageRef = Arc<Storage>;
}

impl ApiServerInstance {
    pub fn new(config: Config, db: DbPool, storage: Arc<Storage>) -> Self {
        let sessions: Arc<Sessions> = Arc::new(session::new(db.clone()));

        // to verify up front configuration is OK.
        let password_hash_config: PasswordHashConfig = (&config).into();
        password_hash_config.hasher();

        let authz_context = Arc::new(AuthzContext::default());

        let auth_services: Arc<AuthServices> = Arc::new(AuthServices::new(
            &db,
            sessions.clone(),
            password_hash_config,
            &config,
        ));
        Self {
            config,
            db,
            authz_context,
            auth_services,
            storage,
        }
    }

    fn auth_state(&self) -> state::Auth {
        self.auth_services.clone()
    }

    fn status_state(&self) -> state::Status {
        Arc::new(StatusLogic::new(self.db.clone()))
    }

    fn storage_state(&self) -> state::StorageRef {
        self.storage.clone()
    }

    fn users_state(&self) -> state::Users {
        Arc::new(UserServices::new(
            self.db.clone(),
            Arc::new(PasswordHashingConfig::default()),
            self.authz_context.clone(),
        ))
    }

    fn collection_state(&self) -> state::Collections {
        Arc::new(CollectionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn execution_state(&self) -> state::Execution {
        Arc::new(ExecutionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn function_state(&self) -> state::Functions {
        Arc::new(FunctionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
            self.storage.clone(),
        ))
    }

    fn roles_state(&self) -> state::Roles {
        Arc::new(RoleServices::new(
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

    fn inter_collection_permissions_state(&self) -> state::InterCollectionPermissions {
        Arc::new(InterCollectionPermissionServices::new(
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

    fn timeout_service(&self) -> TimeoutService {
        TimeoutService::new(Duration::seconds(*self.config.request_timeout()))
    }

    fn table_state(&self) -> state::Tables {
        Arc::new(TableServices::new(
            self.db.clone(),
            self.authz_context.clone(),
            self.storage.clone(),
        ))
    }

    pub async fn build(&self) -> ApiServer {
        apiserver! {
            apiserver {
                // Server Addresses
                addresses => self.config.addresses(),

                // Base URL
                base_url => td_objects::rest_urls::BASE_URL,

                // OpenAPI
                #[cfg(feature = "api-docs")]
                openapi => openapi,

                // Extended
                extension => ExtendedRouter,

                // Open Routes
                router => {
                    auth_unsecure => { state ( self.auth_state() ) },
                },

                // JWT Secured Routes
                router => {
                    auth_secure => { state ( self.auth_state() ) },
                    server_status => { state ( self.status_state() ) },
                    roles => { state ( self.roles_state() ) },
                    permissions => { state ( self.permissions_state() ) },
                    inter_collection_permissions => { state ( self.inter_collection_permissions_state() ) },
                    user_roles => { state ( self.user_roles_state() ) },
                    users => { state ( self.users_state() ) },
                    collections => { state ( self.collection_state() ) },
                    functions => { state ( self.function_state() ) },
                    execution => { state ( self.execution_state() ) },
                    tables => { state ( self.table_state(), self.storage_state() ) },
                }
                .layer => from_fn_with_state(self.auth_state(), authorization_layer),

                router => {
                    // Specific endpoint reachable from localhost only, non-secured, for execution update.
                    callback => { state ( self.execution_state() ) },
                }
                .layer => from_fn(LoopbackIpFilterService::layer),
            }

            // Global layer
            .layer => self.timeout_service().layer(),
            .layer => CorsService::layer(),
            .layer => TraceService::layer(),
        }

        apiserver
    }
}
