//
// Copyright 2025 Tabsdata Inc.
//

#![cfg_attr(test, allow(linker_messages))]

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::bridge::ServerBridgeFactory;
use crate::config::{Config, Params};
use crate::recovery::WorkerRecoverer;
use crate::scheduler::{FunctionDispatcher, FunctionRequester};
use std::sync::Arc;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::{BridgeProviderFactory, BridgeProviderHandle};
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_execution::SchedulerNotify;
use td_process::launcher::cli::Cli;
use td_queries::initialize_logical_ids;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_storage::Storage;
use te_database as dbm;
use tracing::{error, info};

mod apiserver;
mod bridge;
pub mod config;
mod extractors;
mod layers;
mod recovery;
mod router;
mod scheduler;

/// The internal API's route templates, as this server serves them. Exported so
/// the scheduler that builds callbacks against them — and its tests — read the
/// served routes rather than a second copy.
///
/// The templates are exported as bare `&'static str` rather than as the
/// assembled `InternalRoutes`: td-services dev-depends on this crate (a
/// deliberate, test-only cycle), which puts two builds of td-services in that
/// graph, so one of its types cannot cross back. Plain strings can.
pub use apiserver::BASE_URL as INTERNAL_BASE_URL;
pub use router::internal::{
    DOWNLOAD_STATE_PATH, FUNCTION_RUN_CALLBACK_PATH, FUNCTION_VERSION_EXECUTE_PATH,
    FUNCTION_VERSION_EXECUTION_STATUS_PATH, WORKER_CALLBACK_PATH, WORKER_MESSAGE_PATH,
};

pub const CONFIG_NAME: &str = "apiserver";

pub fn run() {
    Cli::<Config, Params>::exec_async(
        CONFIG_NAME,
        |config, params| async move {
            let config = params.resolve(config).map_err(|e| {
                error!("Error resolving API Server configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let database_components = &config.database;
            dbm::validate_components(database_components).map_err(|e| {
                error!("Invalid database configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Connect the database, app-only: one serving pool per registered
            // component so each context grabs the one it needs. The server
            // lifecycle and schema management belong to `td_dbmanager`, never
            // to the apiserver.
            let serving_pools = dbm::connect_app_all(database_components)
                .await
                .map_err(|e| {
                    error!("Error connecting to database: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            // Runtime check runs on the serving pools themselves — no system access.
            dbm::check_pools(&serving_pools).await.map_err(|e| {
                error!("Error checking if database is up to date: {e:#?}");
                ExitStatus::GeneralError
            })?;
            info!("Database is up to date, starting apiserver");

            // The apiserver's regular pool, for the boot-time wiring below
            // (authz enablement seeds identities).
            let db = serving_pools
                .get(dbm::APISERVER_COMPONENT_KEY)
                .map_err(|e| {
                    error!("Error resolving apiserver database pool: {e:#?}");
                    ExitStatus::GeneralError
                })?
                .regular
                .clone()
                .ok_or_else(|| {
                    error!("Error resolving apiserver database pool: regular pool not connected");
                    ExitStatus::GeneralError
                })?;

            initialize_logical_ids(&db).await.map_err(|e| {
                error!("Error initializing logical IDs: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let mount_defs = config.storage_mounts().map_err(|e| {
                error!("Error creating storage: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let storage = Storage::from(mount_defs).map_err(|e| {
                error!("Error creating storage: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let storage = Ctx::new(storage);

            info!("Orchestrator: {}", config.orchestrator);
            let bridge_provider = ServerBridgeFactory
                .build(config.orchestrator)
                .await
                .map_err(|e| {
                    error!("Error creating bridge provider: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            let bridge = Ctx::new(BridgeProviderHandle::new(bridge_provider));

            let runtime_context =
                RuntimeContext::new(bridge.server_catalog())
                    .await
                    .map_err(|e| {
                        error!("Error creating runtime context: {e:#?}");
                        ExitStatus::GeneralError
                    })?;
            let runtime_context = Ctx::new(runtime_context);

            let scheduler_notify = Ctx::new(SchedulerNotify::new());

            // Bring the authz subsystem online behind one call so run() stays
            // free of authz wiring: build the (extension-augmented) registry,
            // reconcile sys_admin, seed any extension identities/sessions, and
            // run the simple-mode representability gate. The OS and EE builds
            // each supply their own `te-authz` with this same entry point.
            let authz_registry = te_authz::enable_extensions(
                &db,
                config.security_mode.is_simple(),
                &config.password,
            )
            .await
            .map_err(|e| {
                error!("Error enabling authz extensions: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // The internal listeners bind before `Services` is assembled so
            // the callback endpoint workers are minted against exists from
            // the start — no placeholder, no later swap.
            let (internal_listeners, internal_endpoint) =
                InternalServer::bind(&config).await.map_err(|e| {
                    error!("Error binding the Internal Server: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            info!(
                "Workers will call back on {}",
                internal_endpoint.authority()
            );

            let server_context = ApiServerCtx::new(
                config.clone(),
                serving_pools.clone(),
                storage.clone(),
                runtime_context.clone(),
                scheduler_notify.clone(),
                bridge.clone(),
                authz_registry.clone(),
                Arc::new(internal_endpoint),
            )
            .await
            .map_err(|e| {
                error!("Error building API Server context: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let api_server = ApiServer::new(&server_context).await.map_err(|e| {
                error!("Error creating API Server: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let internal_server = InternalServer::new(&server_context, internal_listeners)
                .await
                .map_err(|e| {
                    error!("Error creating Internal Server: {e:#?}");
                    ExitStatus::GeneralError
                })?;

            let function_request_server = FunctionRequester::new(&server_context.services).await;
            let function_dispatch_server = FunctionDispatcher::new(&server_context.services).await;
            let worker_recovery_server = WorkerRecoverer::new(&server_context.services).await;

            let outcome = ServerRunner::default()
                .graceful_timeout_secs(Duration::from_secs(30))
                .server(api_server)
                .server(internal_server)
                .server(function_request_server)
                .server(function_dispatch_server)
                .server(worker_recovery_server)
                .run()
                .await;

            // Close every serving pool's connections explicitly, rather than at the
            // mercy of Drop ordering during tokio runtime teardown.
            serving_pools.shutdown().await;

            outcome
        },
        None,
        None,
    );
}
