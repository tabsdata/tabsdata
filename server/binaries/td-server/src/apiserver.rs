//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::process;
use std::sync::Arc;
use td_apiserver::apiserver::ApiServerInstanceBuilder;
use td_apiserver::config::{Config, DbSchema, Params};
use td_apiserver::scheduler_server::SchedulerBuilder;
use td_common::about;
use td_common::attach::attach;
use td_common::logging;
use td_common::server::FileWorkerMessageQueue;
use td_common::status::ExitStatus;
use td_database::sql::DbError;
use td_objects::sql::DaoQueries;
use td_process::launcher::cli::Cli;
use td_process::launcher::hooks;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_storage::Storage;
use tracing::{Level, error, info};

const CONFIG_NAME: &str = "apiserver";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[attach(signal = "apiserver")]
fn main() {
    hooks::panic();

    if env::args().any(|arg| arg == "about") {
        about::tdabout(VERSION);
        process::exit(0);
    }

    logging::start(Level::INFO, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apiserver with command: \n{}", command);

    Cli::<Config, Params>::exec_async(
        CONFIG_NAME,
        |config, params| async move {
            // Initialize logging
            logging::start(Level::INFO, None, false);

            // Resolve config and params
            let config = match params.resolve(config) {
                Ok(config) => config,
                Err(e) => {
                    error!("Error resolving API Server configuration: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

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
                                match db.check_db_version().await {
                                    Ok(_) => {
                                        info!("Database already exists");
                                        return ExitStatus::GeneralError;
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.upgrade_db_version().await {
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
                                // After creating the database and/or schema, system exits.
                                // Values 'auto' & 'None' keep the process running.
                                return ExitStatus::Success;
                            }
                            DbSchema::Upgrade => {
                                info!("Upgrading database");
                                match db.check_db_version().await {
                                    Err(DbError::DatabaseNeedsUpgrade(_, _)) => {
                                        if let Err(err) = db.upgrade_db_version().await {
                                            error!("Error upgrading database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database upgraded");
                                    }
                                    Ok(_) => {
                                        info!("Database does not need to be upgraded");
                                        return ExitStatus::NoAction;
                                    }
                                    Err(error) => {
                                        error!(
                                            "Unexpected error occurred during the database upgrade check: {}",
                                            error
                                        );
                                        return ExitStatus::GeneralError;
                                    }
                                }
                                // After upgrading the database and/or schema, system exits.
                                // Values 'auto' & 'None' keep the process running.
                                return ExitStatus::Success;
                            }
                            DbSchema::Auto => {
                                info!("Creating or upgrading database");
                                match db.check_db_version().await {
                                    Ok(_) => {
                                        info!("Database exists and is up to date");
                                    }
                                    Err(DbError::DatabaseSchemaDoesNotExist) => {
                                        if let Err(err) = db.upgrade_db_version().await {
                                            error!("Error creating database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database created");
                                    }
                                    Err(DbError::DatabaseNeedsUpgrade(_, _)) => {
                                        if let Err(err) = db.upgrade_db_version().await {
                                            error!("Error upgrading database: {}", err);
                                            return ExitStatus::GeneralError;
                                        }
                                        info!("Database upgraded");
                                    }
                                    Err(err) => {
                                        error!(
                                            "Error checking database for creating or upgrading it: {}",
                                            err
                                        );
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
            let storage = match Storage::from(mount_defs) {
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

            let runtime_context = match RuntimeContext::new().await {
                Ok(context) => Arc::new(context),
                Err(e) => {
                    error!("Error creating runtime context: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            // Create queries
            let queries = Arc::new(DaoQueries::default());

            // Create and run the API server
            let api_server = match ApiServerInstanceBuilder::new(
                config,
                db.clone(),
                queries.clone(),
                storage.clone(),
                runtime_context,
            )
            .build()
            .await
            {
                Ok(api_server) => api_server,
                Err(e) => {
                    error!("Error creating API Server: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            // Create execution server
            let internal_addresses = match api_server.internal_addresses().await {
                Ok(addresses) => Arc::new(addresses),
                Err(e) => {
                    error!("Error retrieving internal addresses: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            let execution_server = SchedulerBuilder::new(
                db.clone(),
                queries.clone(),
                storage.clone(),
                worker_message_queue.clone(),
                internal_addresses,
            )
            .build()
            .await;

            match tokio::try_join!(execution_server.run(), api_server.run()) {
                Ok(_) => ExitStatus::Success,
                Err(e) => {
                    error!("Error running API Server: {}", e);
                    ExitStatus::GeneralError
                }
            }
        },
        None,
        None,
    );
}
