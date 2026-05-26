//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::config::{Config, DbSchema, Params};
use crate::scheduler::{FunctionDispatcher, FunctionRequester, SchedulerCtx};
use std::sync::Arc;
use std::time::Duration;
use td_common::server::{
    Orchestrator, OrchestratorMessagingProvider, SupervisorMessagingProvider,
};
use td_kubernetes::runtime::gateway::messaging::KubernetesOrchestratorMessagingProvider;
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_database::error::DbError;
use td_execution::SchedulerNotify;
use td_process::launcher::cli::Cli;
use td_queries::initialize_logical_ids;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_storage::Storage;
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

            // Connect to db
            let db = match td_database::db(&config.database).await {
                Ok(db) => {
                    info!("Connected to database: {}", db.url());
                    if let Some(db_schema) = &params.db_schema {
                        match db_schema {
                            DbSchema::Create => {
                                info!("Creating database");
                                match db.check().await {
                                    Ok(_) => {
                                        info!("Database already exists");
                                        Err(ExitStatus::GeneralError)?
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.upgrade().await {
                                            error!("Error creating database: {err:#?}");
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database created");
                                    }
                                    Err(err) => {
                                        error!("Error checking database for creating it: {err:#?}");
                                        Err(ExitStatus::GeneralError)?
                                    }
                                }
                                // After creating the database and/or schema, system exits.
                                // Values 'auto' & 'None' keep the process running.
                                Err(ExitStatus::Success)?
                            }
                            DbSchema::Upgrade => {
                                info!("Upgrading database");
                                match db.check().await {
                                    Ok(_) => {
                                        info!("Database does not need to be upgraded");
                                        Err(ExitStatus::NoAction)?
                                    }
                                    Err(DbError::DatabaseNeedsUpgrade(_)) => {
                                        if let Err(err) = db.upgrade().await {
                                            error!("Error upgrading database: {err:#?}");
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database upgraded");
                                    }
                                    Err(error) => {
                                        error!(
                                            "Unexpected error occurred during the database upgrade check: {error:#?}"
                                        );
                                        Err(ExitStatus::GeneralError)?
                                    }
                                }
                                // After upgrading the database and/or schema, system exits.
                                // Values 'auto' & 'None' keep the process running.
                                Err(ExitStatus::Success)?
                            }
                            DbSchema::Auto => {
                                info!("Creating or upgrading database");
                                match db.check().await {
                                    Ok(_) => {
                                        info!("Database exists and is up to date");
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.upgrade().await {
                                            error!("Error creating database: {err:#?}");
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database created");
                                    }
                                    Err(DbError::DatabaseNeedsUpgrade(_)) => {
                                        if let Err(err) = db.upgrade().await {
                                            error!("Error upgrading database: {err:#?}");
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database upgraded");
                                    }
                                    Err(err) => {
                                        error!(
                                            "Error checking database for creating or upgrading it: {err:#?}"
                                        );
                                        Err(ExitStatus::GeneralError)?
                                    }
                                }
                            }
                        }
                    }
                    match db.check().await {
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
            let messaging_provider: Box<dyn OrchestratorMessagingProvider> =
                match config.orchestrator {
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

            // Create runtime context
            let runtime_context = RuntimeContext::new().await.map_err(|e| {
                error!("Error creating runtime context: {e:#?}");
                ExitStatus::GeneralError
            })?;
            let runtime_context = Arc::new(runtime_context);

            // Create shared scheduler notify
            let scheduler_notify = SchedulerNotify::new();

            // Create context
            let server_context = ApiServerCtx::new(
                config.clone(),
                db.clone(),
                storage.clone(),
                runtime_context.clone(),
                scheduler_notify.clone(),
            );

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
            );
            let function_request_server = FunctionRequester::new(&scheduler_context).await;
            let function_dispatch_server = FunctionDispatcher::new(&scheduler_context).await;

            // Start servers
            ServerRunner::default()
                .graceful_timeout_secs(Duration::from_secs(30))
                .server(api_server)
                .server(internal_server)
                .server(function_request_server)
                .server(function_dispatch_server)
                .run()
                .await
        },
        None,
        None,
    );
}
