//
// Copyright 2024 Tabs Data Inc.
//

use std::env;
use std::sync::Arc;
use tabsdatalib::bin::apisrv::api_server::ApiSrv;
use tabsdatalib::bin::apisrv::config::{Config, Params};
use tabsdatalib::bin::apisrv::scheduler_server::SchedulerBuilder;
use td_attach::attach;
use td_common::cli::Cli;
use td_common::logging;
use td_common::server::FileWorkerMessageQueue;
use td_common::status::ExitStatus;
use td_storage::{MountDef, Storage};
use tracing::{error, info, Level};

const CONFIG_NAME: &str = "apisrv";

#[attach(signal = "apisrv")]
fn main() {
    logging::start(Level::DEBUG, None, false);

    let arguments: Vec<String> = env::args().collect();
    let command = arguments.join(" ");
    info!("Running apisrv with command: \n{}", command);

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

            // Create storage, for now we use a single mount and in local filesystem
            let mount_def = match MountDef::builder()
                .mount_path("/")
                .uri(config.storage_url().as_ref().unwrap()) // at this point we know it's Some
                .build()
            {
                Ok(mount_def) => mount_def,
                Err(e) => {
                    error!("Error creating storage mount definition: {}", e);
                    return ExitStatus::GeneralError;
                }
            };

            let storage = match Storage::from(vec![mount_def]).await {
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
            .build();

            // Create and run the API server
            let api_srv = ApiSrv::new(config, db, storage).build().await;

            tokio::join!(execution_server.run(), api_srv.run());
            ExitStatus::Success
        },
        None,
    );
}
