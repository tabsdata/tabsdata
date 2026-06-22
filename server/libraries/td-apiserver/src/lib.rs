//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::bridge::ServerBridgeFactory;
use crate::config::{Config, Params, SecurityMode};
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

            let database_descriptor = &config.database;
            dbm::validate_descriptor(database_descriptor).map_err(|e| {
                error!("Invalid database configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            // Connect the database, app-only, in every mode: one step builds a serving
            // pool for each registered component so each context grabs the one it needs.
            // The apiserver never boots or manages the database — the in-process embedded
            // server is brought up by `td_dbmanager start` and schemas are
            // provisioned/upgraded by `td_dbmanager create`/`upgrade`, out of band — so it
            // holds no privileged system access.
            let serving_pools = dbm::connect_app_all(database_descriptor)
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

            // Build the base (built-in) registry and let each extension augment
            // it — the OS owns the default; te-authz adds its `ai` kind (and the
            // `manage_ai` permission riding on sys_admin). Reconcile sys_admin's
            // composition with those extras before the representability check
            // reads the same list — the OS schema seed cannot know about
            // extension permissions.
            let authz_registry = Arc::new(te_authz::register_ext(
                td_authz::rbac::AuthzRegistry::default(),
            ));
            {
                // Writes go through the read-write pool's transaction (the
                // read-only `acquire()` would reject the INSERT).
                let mut tx = db.begin().await.map_err(|e| {
                    error!("Error starting transaction for sys_admin reconciliation: {e:#?}");
                    ExitStatus::GeneralError
                })?;
                {
                    let mut conn = tx.as_mut_conn();
                    td_services::security::sys_admin::ensure_sys_admin_permissions(
                        &mut conn,
                        authz_registry.sys_admin_permissions(),
                    )
                    .await
                    .map_err(|e| {
                        error!("Error reconciling sys_admin permissions: {e:#?}");
                        ExitStatus::GeneralError
                    })?;
                }
                tx.commit().await.map_err(|e| {
                    error!("Error committing sys_admin reconciliation: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            }

            // Simple Security Mode requires the current RBAC configuration to be
            // representable on the flat Simple surface. If the operator booted in `simple` mode
            // but the roles/permissions were arranged in RBAC-only ways (custom
            // roles, modified built-in roles, specific-project rows on system
            // roles, ...), Simple View cannot faithfully present them — refuse to
            // start and list the offending rows so the operator can either revert
            // the configuration or boot in `rbac` mode instead.
            if config.security_mode == SecurityMode::Simple {
                match td_services::security::representability::check_representability(
                    &db,
                    authz_registry.sys_admin_permissions(),
                )
                .await
                {
                    Ok(reasons) if reasons.is_empty() => {
                        info!("Security mode 'simple': RBAC configuration is representable");
                    }
                    Ok(reasons) => {
                        error!(
                            "Cannot start in 'simple' security mode: the current RBAC \
                             configuration is not representable on the Simple surface. Revert \
                             the offending configuration or start in 'rbac' security mode. \
                             Reasons ({} found):",
                            reasons.len()
                        );
                        for reason in &reasons {
                            error!("  - {reason}");
                        }
                        Err(ExitStatus::GeneralError)?
                    }
                    Err(e) => {
                        error!("Error running security-mode representability check: {e:#?}");
                        Err(ExitStatus::GeneralError)?
                    }
                }
            } else {
                info!("Security mode 'rbac': RBAC management endpoints enabled");
            }

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
            // mercy of Drop ordering during tokio runtime teardown. The apiserver owns
            // no database server in any mode — the embedded server's lifecycle belongs
            // to `td_dbmanager start`/`stop` — so there is nothing else to tear down here.
            serving_pools.shutdown().await;

            outcome
        },
        None,
        None,
    );
}
