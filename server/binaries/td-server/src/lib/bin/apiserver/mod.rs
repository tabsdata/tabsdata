//
// Copyright 2024 Tabs Data Inc.
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

pub mod auth;
mod collections;
pub mod config;
mod data;
pub mod execution;
pub mod functions;
mod functions2;
mod jwt_login;
#[cfg(feature = "api-docs")]
mod openapi;
pub mod permissions;
pub mod roles;
pub mod scheduler_server;
mod server_status;
mod user_roles;
mod users;

use crate::apiserver;
use crate::bin::apiserver::auth::authorization_layer::authorization_layer;
use crate::bin::apiserver::auth::{auth_secure, auth_unsecure};
use crate::bin::apiserver::config::Config;
use crate::bin::apiserver::execution::update;
use crate::logic::apiserver::jwt::jwt_logic::JwtLogic;
use crate::logic::apiserver::layers::cors::CorsService;
use crate::logic::apiserver::layers::timeout::TimeoutService;
use crate::logic::apiserver::layers::tracing::TraceService;
use crate::logic::apiserver::layers::uri_filter::LoopbackIpFilterService;
use crate::logic::apiserver::ApiServer;
use crate::logic::collections::service::CollectionServices;
use crate::logic::datasets::service::DatasetServices;
use crate::logic::server_status::StatusLogic;
use crate::logic::users::service::UserServices;
use axum::middleware::{from_fn, from_fn_with_state};
use chrono::Duration;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_security::config::PasswordHashingConfig;
use td_services::execution::services::ExecutionServices;
use td_services::auth::services::{AuthServices, PasswordHashConfig};
use td_services::auth::session;
use td_services::auth::session::Sessions;
use td_services::permission::services::PermissionServices;
use td_services::role::services::RoleServices;
use td_services::user_role::services::UserRoleServices;
use td_storage::Storage;

pub struct ApiServerInstance {
    config: Config,
    db: DbPool,
    authz_context: Arc<AuthzContext>,
    auth_services: Arc<AuthServices>,
    jwt_logic: Arc<JwtLogic>,
    storage: Arc<Storage>,
}

pub type AuthState = Arc<AuthServices>;

pub type StatusState = Arc<StatusLogic>;
pub type UsersState = Arc<UserServices>;
pub type CollectionsState = Arc<CollectionServices>;
pub type DatasetsState = Arc<DatasetServices>;
pub type ExecutionState = Arc<ExecutionServices>;
pub type RolesState = Arc<RoleServices>;
pub type UserRolesState = Arc<UserRoleServices>;
pub type PermissionsState = Arc<PermissionServices>;

pub type StorageState = Arc<Storage>;

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
        let jwt_logic = Arc::new(JwtLogic::new(
            config.jwt().secret().as_ref().unwrap(), // at this point we know it's not None
            Duration::seconds(*config.jwt().access_token_expiration()),
            Duration::seconds(*config.jwt().access_token_expiration() * 2),
        ));
        Self {
            config,
            db,
            authz_context,
            auth_services,
            jwt_logic,
            storage,
        }
    }

    fn auth_state(&self) -> AuthState {
        self.auth_services.clone()
    }

    fn status_state(&self) -> StatusState {
        Arc::new(StatusLogic::new(self.db.clone()))
    }

    fn storage_state(&self) -> StorageState {
        self.storage.clone()
    }

    fn users_state(&self) -> UsersState {
        Arc::new(UserServices::new(
            self.db.clone(),
            Arc::new(PasswordHashingConfig::default()),
            self.jwt_logic.clone(),
            self.authz_context.clone(),
        ))
    }

    fn collection_state(&self) -> CollectionsState {
        Arc::new(CollectionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn dataset_state(&self) -> DatasetsState {
        Arc::new(DatasetServices::new(
            self.db.clone(),
            self.storage.clone(),
            Arc::new(self.config.transaction_by().clone()),
        ))
    }

    fn roles_state(&self) -> RolesState {
        Arc::new(RoleServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn permissions_state(&self) -> PermissionsState {
        Arc::new(PermissionServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn user_roles_state(&self) -> UserRolesState {
        Arc::new(UserRoleServices::new(
            self.db.clone(),
            self.authz_context.clone(),
        ))
    }

    fn timeout_service(&self) -> TimeoutService {
        TimeoutService::new(Duration::seconds(*self.config.request_timeout()))
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

                // Open Routes
                router => {
                    auth_unsecure => { state ( self.auth_state() ) },
                    jwt_login => { state ( self.users_state() ) },
                },

                // JWT Secured Routes
                router => {
                    auth_secure => { state ( self.auth_state() ) },
                    server_status => { state ( self.status_state() ) },
                    roles => { state ( self.roles_state() ) },
                    permissions => { state ( self.permissions_state() ) },
                    user_roles => { state ( self.user_roles_state() ) },
                    users => { state ( self.users_state() ) },
                    collections => { state ( self.collection_state() ) },
                    functions => { state ( self.dataset_state() ) },
                    execution => { state ( self.dataset_state() ) },
                    data => { state ( self.dataset_state(), self.storage_state() ) },
                }
                .layer => from_fn_with_state(self.auth_state(), authorization_layer),

                router => {
                    // Specific endpoint reachable from localhost only, non-secured, for execution update.
                    update => { state ( self.dataset_state() ) },
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
