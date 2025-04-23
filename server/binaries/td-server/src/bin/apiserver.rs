//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::sync::Arc;
use td_apiserver::config::{Config, Params};
use td_apiserver::router::scheduler_server::SchedulerBuilder;
use td_apiserver::router::ApiServerInstance;
use td_common::attach::attach;
use td_common::cli::Cli;
use td_common::logging;
use td_common::server::FileWorkerMessageQueue;
use td_common::status::ExitStatus;
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
                Ok(db) => db,
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
