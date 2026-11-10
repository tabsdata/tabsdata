//
// Copyright 2025 Tabsdata Inc.
//

#![cfg_attr(test, allow(linker_messages))]

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::bridge::ServerBridgeFactory;
use crate::config::{Config, Params};
use crate::recovery::WorkerRecoverer;
use crate::scheduler::{FunctionRequester, ProbeDispatcher};
use std::sync::Arc;
use std::time::Duration;
use ta_edition::Edition;
use ta_services::service::Ctx;
use td_bridge::{BridgeProviderFactory, BridgeProviderHandle};
use td_common::signal::Signals;
use td_common::status::ExitStatus;
use td_common::task_group::TaskGroup;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::types::basic::InstanceId;
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
mod recovery;
mod router;
mod scheduler;
pub mod tdmanager;

/// The internal API's route templates, as this server serves them. Exported so
/// the scheduler that builds callbacks against them — and its tests — read the
/// served routes rather than a second copy.
///
/// The templates are exported as bare `&'static str` rather than as the
/// assembled `InternalRoutes`: td-services dev-depends on this crate (a
/// deliberate, test-only cycle), which puts two builds of td-services in that
/// graph, so one of its types cannot cross back. Plain strings can.
pub use apiserver::BASE_URL as INTERNAL_BASE_URL;
pub use router::connection_catalog::CONNECTION_PROBE_RESULT_PATH;
pub use router::internal::{
    DOWNLOAD_STATE_PATH, FUNCTION_RUN_CALLBACK_PATH, FUNCTION_VERSION_EXECUTE_PATH,
    FUNCTION_VERSION_EXECUTION_STATUS_PATH, WORKER_CALLBACK_PATH, WORKER_MESSAGE_PATH,
};

pub const CONFIG_NAME: &str = "apiserver";

/// Within what the pod leaves after its `preStop` pause: a 60s termination
/// grace less the 12s sleep.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

pub fn run(edition: Edition) {
    let registry = edition.database();
    let routes = Arc::new(edition.routes());
    Cli::<Config, Params>::exec_async(
        CONFIG_NAME,
        |config, params| async move {
            let signals = Signals::install();
            let registry = &registry;
            let config = params.resolve(config).map_err(|e| {
                error!("Error resolving API Server configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let database_components = &config.database;
            dbm::validate_components(registry, database_components).map_err(|e| {
                error!("Invalid database configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Connect the database, app-only: one serving pool per registered
            // component so each context grabs the one it needs. The server
            // lifecycle and schema management belong to `td_dbmanager`, never
            // to the apiserver.
            let serving_pools = dbm::connect_app_all(registry, database_components)
                .await
                .map_err(|e| {
                    error!("Error connecting to database: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            // Runtime check runs on the serving pools themselves — no system access.
            dbm::check_pools(registry, &serving_pools)
                .await
                .map_err(|e| {
                    error!("Error checking if database is up to date: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            info!("Database is up to date, starting apiserver");

            let db = serving_pools
                .get(dbm::APISERVER_COMPONENT_KEY)
                .map_err(|e| {
                    error!("Error resolving apiserver database pool: {e:#?}");
                    ExitStatus::GeneralError
                })?
                .clone();

            initialize_logical_ids(&db).await.map_err(|e| {
                error!("Error initializing logical IDs: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let instance_id = db
                .instance_id()
                .await
                .map_err(TdError::from)
                .and_then(InstanceId::try_from)
                .map_err(|e| {
                    error!("Error reading the instance id: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            info!("Instance: {instance_id}");
            let instance_id = Ctx::new(instance_id);

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

            // Bring the authz subsystem online behind one call so run() stays
            // free of authz wiring: build the (extension-augmented) registry,
            // reconcile sys_admin, seed any extension identities and system
            // tokens, and run the simple-mode representability gate. The OS
            // and EE builds each supply their own `te-authz` with this same
            // entry point.
            let authz_registry = te_authz::enable_extensions(
                &DbPool::new(serving_pools.domain_pools(), Global),
                config.security_mode.is_simple(),
                &config.password,
                config.extended_config.seeded_tokens(),
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
                instance_id,
                bridge.clone(),
                authz_registry.clone(),
                Arc::new(internal_endpoint),
                routes.clone(),
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
            let worker_recovery_server = WorkerRecoverer::new(&server_context.services).await;
            let probe_dispatch_server = ProbeDispatcher::new(&server_context.services).await;

            let tasks = TaskGroup::new(SHUTDOWN_GRACE)
                .task(api_server)
                .task(internal_server)
                .task(function_request_server)
                .task(worker_recovery_server)
                .task(probe_dispatch_server);
            let outcome = te_services::extended_tasks(&server_context.extended_services, tasks)
                .run_until(signals.wait())
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
