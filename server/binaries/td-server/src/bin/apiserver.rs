//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::sync::Arc;
use td_apiserver::config::{Config, DbSchema, Params};
use td_apiserver::router::scheduler_server::SchedulerBuilder;
use td_apiserver::router::ApiServerInstance;
use td_common::attach::attach;
use td_common::cli::Cli;
use td_common::logging;
use td_common::server::FileWorkerMessageQueue;
use td_common::status::ExitStatus;
use td_database::sql::DbError;
use td_storage::Storage;
use tracing::{error, info, Level};

const CONFIG_NAME: &str = "apiserver";

#[attach(signal = "apiserver")]
fn main() {
    logging::start(Level::DEBUG, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apiserver with command: \n{}", command);

    Cli::<Config, Params>::exec_async(
        CONFIG_NAME,
        |config, params| async move {
            // Initialize logging
            logging::start(Level::DEBUG, None, false);

            // Resolve config and params
            let config = match params.resolve(config) {
                Ok(config) => config,
                Err(e) => {
                    error!("Error resolving API Server configuration: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            // Connect to db
            let db = match td_database::db(config.database()).await {
                Ok(db) => {
                    info!(
                        "Connected to Sqlite database: {}",
                        config.database().url().as_ref().unwrap()
                    );
                    if let Some(db_schema) = params.db_schema() {
                        match db_schema {
                            DbSchema::Create => {
                                info!("Creating database");
                                match db.check_db_version().await {
                                    Ok(_) => {
                                        info!("Database already exists");
                                        return ExitStatus::GeneralError;
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.update_db_version().await {
                                            error!("Error creating database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database created");
                                    }
                                    Err(err) => {
                                        error!("Error checking database for creating it: {}", err);
                                        return ExitStatus::GeneralError;
                                    }
                                }
                            }
                            DbSchema::Update => {
                                info!("Updating database");
                                match db.check_db_version().await {
                                    Ok(_) => {
                                        if let Err(err) = db.update_db_version().await {
                                            error!("Error updating database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database updated");
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        error!("Database does not exist, cannot update");
                                        return ExitStatus::GeneralError;
                                    }
                                    Err(err) => {
                                        error!("Error checking database for updating it: {}", err);
                                        return ExitStatus::GeneralError;
                                    }
                                }
                            }
                            DbSchema::Auto => {
                                info!("Creating or updating database");
                                match db.check_db_version().await {
                                    Ok(_) => {
                                        info!("Database exists and is up to date");
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.update_db_version().await {
                                            error!("Error creating database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database created");
                                    }
                                    Err(DbError::DatabaseNeedsUpgrade(_, _)) => {
                                        if let Err(err) = db.update_db_version().await {
                                            error!("Error updating database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database updated");
                                    }
                                    Err(err) => {
                                        error!("Error checking database for creating or updating it: {}", err);
                                        return ExitStatus::GeneralError;
                                    }
                                }
                            }
                        }
                    }
                    match db.check_db_version().await {
                        Ok(_) => {
                            info!("Database is up to date, starting apiserver");
                        }
                        Err(e) => {
                            error!("Error checking if database is up to date: {}", e);
                            return ExitStatus::GeneralError;
                        }
                    }
                    db
                }
                Err(e) => {
                    error!("Error connecting to Sqlite database: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            let mount_defs = match config.storage_mounts() {
                Ok(mount_def) => mount_def,
                Err(e) => {
                    error!("Error creating storage: {}", e);
                    return ExitStatus::GeneralError;
                }
            };
            let storage = match Storage::from(mount_defs).await {
                Ok(storage) => storage,
                Err(e) => {
                    error!("Error creating storage: {}", e);
                    return ExitStatus::GeneralError;
                }
            };
            let storage = Arc::new(storage);

            let worker_message_queue = match FileWorkerMessageQueue::new().await {
                Ok(worker_message_queue) => worker_message_queue,
                Err(e) => {
                    error!("Error creating worker message queue: {}", e);
                    return ExitStatus::GeneralError;
                }
            };
            let worker_message_queue = Arc::new(worker_message_queue);

            // Create execution server
            let execution_server = SchedulerBuilder::new(
                db.clone(),
                storage.clone(),
                worker_message_queue.clone(),
                Arc::new(*config.addresses().first().unwrap()),
            )
            .build()
            .await;

            // Create and run the API server
            let apiserver = ApiServerInstance::new(config, db, storage).build().await;

            tokio::join!(execution_server.run(), apiserver.run());
            ExitStatus::Success
        },
        None,
    );
}
