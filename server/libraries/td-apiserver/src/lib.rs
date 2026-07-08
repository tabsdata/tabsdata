//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::bridge::ServerBridgeFactory;
use crate::config::{Config, Params};
use crate::scheduler::{FunctionDispatcher, FunctionRequester, SchedulerCtx};
use std::sync::Arc;
use std::time::Duration;
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
mod layers;
mod router;
mod scheduler;

pub const CONFIG_NAME: &str = "apiserver";

/// Run the apiserver. This function is the entry point for the `apiserver` binary.
pub fn run() {
    Cli::<Config, Params>::exec_async(
        CONFIG_NAME,
        |config, params| async move {
            // Resolve config and params
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

            // The apiserver's own pool, for the boot-time wiring below.
            let db = serving_pools
                .get(dbm::APISERVER_COMPONENT_KEY)
                .map_err(|e| {
                    error!("Error resolving apiserver database pool: {e:#?}");
                    ExitStatus::GeneralError
                })?
                .clone();

            // setup IdL Generators
            initialize_logical_ids(&db).await.map_err(|e| {
                error!("Error initializing logical IDs: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Create storage
            let mount_defs = config.storage_mounts().map_err(|e| {
                error!("Error creating storage: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let storage = Storage::from(mount_defs).map_err(|e| {
                error!("Error creating storage: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let storage = Arc::new(storage);

            info!("Orchestrator: {}", config.orchestrator);
            let bridge_provider = ServerBridgeFactory
                .build(config.orchestrator)
                .await
                .map_err(|e| {
                    error!("Error creating bridge provider: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            let bridge = Arc::new(BridgeProviderHandle::new(bridge_provider));

            // Create runtime context
            let runtime_context =
                RuntimeContext::new(bridge.server_catalog())
                    .await
                    .map_err(|e| {
                        error!("Error creating runtime context: {e:#?}");
                        ExitStatus::GeneralError
                    })?;
            let runtime_context = Arc::new(runtime_context);

            // Create shared scheduler notify
            let scheduler_notify = Arc::new(SchedulerNotify::new());

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

            // Create context
            let server_context = ApiServerCtx::new(
                config.clone(),
                serving_pools.clone(),
                storage.clone(),
                runtime_context.clone(),
                scheduler_notify.clone(),
                bridge.clone(),
                authz_registry.clone(),
            )
            .await
            .map_err(|e| {
                error!("Error building API Server context: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Create required servers
            let api_server = ApiServer::new(&server_context).await.map_err(|e| {
                error!("Error creating API Server: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let internal_server = InternalServer::new(&server_context).await.map_err(|e| {
                error!("Error creating Internal Server: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Create scheduler servers
            let internal_addresses = internal_server.addresses().await.map_err(|e| {
                error!("Error retrieving internal addresses: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let internal_addresses = Arc::new(internal_addresses);

            let scheduler_context = SchedulerCtx::new(
                db.clone(),
                storage.clone(),
                bridge.clone(),
                internal_addresses.clone(),
                scheduler_notify,
                Arc::new(config.jwt.clone()),
            );
            let function_request_server = FunctionRequester::new(&scheduler_context).await;
            let function_dispatch_server = FunctionDispatcher::new(&scheduler_context).await;

            // Start servers
            let outcome = ServerRunner::default()
                .graceful_timeout_secs(Duration::from_secs(30))
                .server(api_server)
                .server(internal_server)
                .server(function_request_server)
                .server(function_dispatch_server)
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
