//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::config::{Config, DbSchema, Params, SecurityMode};
use crate::scheduler::{FunctionDispatcher, FunctionRequester, SchedulerCtx};
use std::sync::Arc;
use std::time::Duration;
use td_common::server::{
    BridgeProvider, BridgeProviderHandle, Orchestrator, OrchestratorMessagingProvider,
    SupervisorBridgeProvider, SupervisorMessagingProvider,
};
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_database::config::DatabaseDescriptor;
use td_database::pool::TdPool;
use td_execution::SchedulerNotify;
use td_kubernetes::kube::KubernetesBridgeProvider;
use td_kubernetes::runtime::gateway::messaging::KubernetesOrchestratorMessagingProvider;
use td_process::launcher::cli::Cli;
use td_queries::initialize_logical_ids;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_storage::Storage;
use te_apiserver::schema::SchemaManagerResolver;
use tracing::{error, info};

mod apiserver;
pub mod config;
mod layers;
mod router;
mod scheduler;

const CONFIG_NAME: &str = "apiserver";

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
            te_apiserver::config::validate_database(database_descriptor).map_err(|e| {
                error!("Invalid database configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;

            let system_db = database_descriptor
                .assemble(td_database::config::SYSTEM_SCHEMA_KEY)
                .map_err(|e| {
                    error!("Error resolving system database configuration: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            let system_pool = td_database::db(&system_db).await.map_err(|e| {
                error!("Error connecting to system database: {e:#?}");
                ExitStatus::GeneralError
            })?;

            if let Some(db_schema) = &params.db_schema {
                match db_schema {
                    DbSchema::Create => {
                        info!("Creating database");
                        database_descriptor
                            .provision(&system_pool)
                            .await
                            .map_err(|e| {
                                error!("Error provisioning database: {e:#?}");
                                ExitStatus::GeneralError
                            })?;
                        upgrade_schemas(database_descriptor, &system_pool).await?;
                        info!("Database created");
                        Err(ExitStatus::Success)?
                    }
                    DbSchema::Upgrade => {
                        info!("Upgrading database");
                        upgrade_schemas(database_descriptor, &system_pool).await?;
                        info!("Database upgraded");
                        Err(ExitStatus::Success)?
                    }
                    DbSchema::Auto => {
                        info!("Creating or upgrading database");
                        database_descriptor
                            .provision(&system_pool)
                            .await
                            .map_err(|e| {
                                error!("Error provisioning database: {e:#?}");
                                ExitStatus::GeneralError
                            })?;
                        upgrade_schemas(database_descriptor, &system_pool).await?;
                    }
                }
            }

            // Connect the runtime apiserver pool against the running server.
            let apiserver_db = database_descriptor
                .serving_config(CONFIG_NAME, &system_pool)
                .map_err(|e| {
                    error!("Error resolving apiserver database configuration: {e:#?}");
                    ExitStatus::GeneralError
                })?;
            let db = match td_database::db(&apiserver_db).await {
                Ok(db) => {
                    info!("Connected to database: {}", db.url());
                    let manager =
                        SchemaManagerResolver::for_component(CONFIG_NAME).map_err(|e| {
                            error!("Error resolving apiserver schema manager: {e:#?}");
                            ExitStatus::GeneralError
                        })?;
                    match manager.check(&db).await {
                        Ok(_) => {
                            info!("Database is up to date, starting apiserver");
                        }
                        Err(e) => {
                            error!("Error checking if database is up to date: {e:#?}");
                            Err(ExitStatus::GeneralError)?
                        }
                    }
                    db
                }
                Err(e) => {
                    error!("Error connecting to database: {e:#?}");
                    Err(ExitStatus::GeneralError)?
                }
            };

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

            // Create orchestrator messaging provider and its message queue
            info!("Orchestrator: {}", config.orchestrator);
            let messaging_provider: Box<dyn OrchestratorMessagingProvider> = match config
                .orchestrator
            {
                Orchestrator::Supervisor => Box::new(SupervisorMessagingProvider::new()),
                Orchestrator::Kubernetes => {
                    let provider = KubernetesOrchestratorMessagingProvider::from_pod_environment()
                        .await
                        .map_err(|e| {
                            error!("Error creating kubernetes messaging provider: {e:#?}");
                            ExitStatus::GeneralError
                        })?;
                    Box::new(provider)
                }
            };
            let orchestrator_message_queue = Arc::new(
                messaging_provider
                    .orchestrator_message_queue()
                    .await
                    .map_err(|e| {
                        error!("Error creating orchestrator message queue: {e:#?}");
                        ExitStatus::GeneralError
                    })?,
            );

            let bridge_provider: Arc<dyn BridgeProvider> = match config.orchestrator {
                Orchestrator::Supervisor => Arc::new(SupervisorBridgeProvider::new()),
                Orchestrator::Kubernetes => {
                    let provider = KubernetesBridgeProvider::from_pod_environment()
                        .await
                        .map_err(|e| {
                            error!("Error creating kubernetes bridge provider: {e:#?}");
                            ExitStatus::GeneralError
                        })?;
                    Arc::new(provider)
                }
            };
            let server_catalog = bridge_provider.server_catalog().await.map_err(|e| {
                error!("Error creating server catalog: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let bridge = Arc::new(BridgeProviderHandle::new(bridge_provider));

            // Create runtime context
            let runtime_context = RuntimeContext::new(server_catalog).await.map_err(|e| {
                error!("Error creating runtime context: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let runtime_context = Arc::new(runtime_context);

            // Create shared scheduler notify
            let scheduler_notify = SchedulerNotify::new();

            // Retain a handle to the system pool (which owns the embedded
            // postgres lifecycle) so it can be stopped explicitly on shutdown,
            // independently of the serving pools that only connect to it.
            let system_handle = system_pool.clone();

            // Simple Security Mode requires the current RBAC configuration to be
            // representable on the flat Simple surface (TEDC "What makes
            // switching ... one-way"). If the operator booted in `simple` mode
            // but the roles/permissions were arranged in RBAC-only ways (custom
            // roles, modified built-in roles, specific-project rows on system
            // roles, ...), Simple View cannot faithfully present them — refuse to
            // start and list the offending rows so the operator can either revert
            // the configuration or boot in `rbac` mode instead.
            if config.security_mode == SecurityMode::Simple {
                match td_services::security::representability::check_representability(&db).await {
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
                system_pool,
                db.clone(),
                storage.clone(),
                runtime_context.clone(),
                scheduler_notify.clone(),
                bridge,
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
                orchestrator_message_queue.clone(),
                internal_addresses.clone(),
                scheduler_notify,
                config.orchestrator,
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

            // Explicit shutdown so the embedded postgres lifecycle does not
            // depend on Drop ordering during tokio runtime teardown. Close the
            // serving pool's connections first, then stop the system pool, which
            // owns the embedded server and tears it down with a fast shutdown
            // that disconnects any remaining client sessions.
            db.shutdown().await;
            system_handle.shutdown().await;

            outcome
        },
        None,
        None,
    );
}

/// Connect each component schema's serving pool and upgrade it through the
/// schema manager the extension resolves for that component.
async fn upgrade_schemas(
    database_descriptor: &DatabaseDescriptor,
    system_pool: &TdPool,
) -> Result<(), ExitStatus> {
    for (name, _) in database_descriptor.schemas() {
        let schema_db = database_descriptor
            .serving_config(name, system_pool)
            .map_err(|e| {
                error!("Error resolving '{name}' database configuration: {e:#?}");
                ExitStatus::GeneralError
            })?;
        let pool = td_database::db(&schema_db).await.map_err(|e| {
            error!("Error connecting to '{name}' schema: {e:#?}");
            ExitStatus::GeneralError
        })?;
        let manager = SchemaManagerResolver::for_component(name).map_err(|e| {
            error!("Error resolving '{name}' schema manager: {e:#?}");
            ExitStatus::GeneralError
        })?;
        manager.upgrade(&pool).await.map_err(|e| {
            error!("Error upgrading '{name}' schema: {e:#?}");
            ExitStatus::GeneralError
        })?;
    }
    Ok(())
}
