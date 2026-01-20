//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::internal::InternalServer;
use crate::apiserver::{ApiServer, ApiServerCtx};
use crate::config::{Config, DbSchema, Params};
use crate::scheduler::{FunctionDispatcher, FunctionRequester, SchedulerCtx};
use std::sync::Arc;
use std::time::Duration;
use td_common::server::FileWorkerMessageQueue;
use td_common::server_runner::ServerRunner;
use td_common::status::ExitStatus;
use td_database::sql::DbError;
use td_objects::sql::DaoQueries;
use td_process::launcher::cli::Cli;
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
                error!("Error resolving API Server configuration: {e}");
                ExitStatus::GeneralError
            })?;

            // Connect to db
            let db = match td_database::db(&config.database).await {
                Ok(db) => {
                    info!(
                        "Connected to Sqlite database: {}",
                        config.database.url.as_ref().unwrap()
                    );
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
                                            error!("Error creating database: {}", err);
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database created");
                                    }
                                    Err(err) => {
                                        error!("Error checking database for creating it: {}", err);
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
                                            error!("Error upgrading database: {}", err);
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database upgraded");
                                    }
                                    Err(error) => {
                                        error!(
                                            "Unexpected error occurred during the database upgrade check: {}",
                                            error
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
                                            error!("Error creating database: {}", err);
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database created");
                                    }
                                    Err(DbError::DatabaseNeedsUpgrade(_)) => {
                                        if let Err(err) = db.upgrade().await {
                                            error!("Error upgrading database: {}", err);
                                            Err(ExitStatus::GeneralError)?
                                        }
                                        info!("Database upgraded");
                                    }
                                    Err(err) => {
                                        error!(
                                            "Error checking database for creating or upgrading it: {}",
                                            err
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
                            error!("Error checking if database is up to date: {e}");
                            Err(ExitStatus::GeneralError)?
                        }
                    }
                    db
                }
                Err(e) => {
                    error!("Error connecting to Sqlite database: {e}");
                    Err(ExitStatus::GeneralError)?
                }
            };

            // Create storage
            let mount_defs = config.storage_mounts().map_err(|e| {
                error!("Error creating storage: {e}");
                ExitStatus::GeneralError
            })?;
            let storage = Storage::from(mount_defs).map_err(|e| {
                error!("Error creating storage: {e}");
                ExitStatus::GeneralError
            })?;
            let storage = Arc::new(storage);

            // Create worker message queue
            let worker_message_queue = FileWorkerMessageQueue::new().await.map_err(|e| {
                error!("Error creating worker message queue: {e}");
                ExitStatus::GeneralError
            })?;
            let worker_message_queue = Arc::new(worker_message_queue);

            // Create runtime context
            let runtime_context = RuntimeContext::new().await.map_err(|e| {
                error!("Error creating runtime context: {e}");
                ExitStatus::GeneralError
            })?;
            let runtime_context = Arc::new(runtime_context);

            // Create queries
            let queries = Arc::new(DaoQueries::default());

            // Create context
            let server_context = ApiServerCtx::new(
                config.clone(),
                db.clone(),
                queries.clone(),
                storage.clone(),
                runtime_context.clone(),
            );

            // Create required servers
            let api_server = ApiServer::new(&server_context).await.map_err(|e| {
                error!("Error creating API Server: {e}");
                ExitStatus::GeneralError
            })?;
            let internal_server = InternalServer::new(&server_context).await.map_err(|e| {
                error!("Error creating Internal Server: {e}");
                ExitStatus::GeneralError
            })?;

            // Create scheduler servers
            let internal_addresses = internal_server.addresses().await.map_err(|e| {
                error!("Error retrieving internal addresses: {e}");
                ExitStatus::GeneralError
            })?;
            let internal_addresses = Arc::new(internal_addresses);

            let scheduler_context = SchedulerCtx::new(
                db.clone(),
                queries.clone(),
                storage.clone(),
                worker_message_queue.clone(),
                internal_addresses.clone(),
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
