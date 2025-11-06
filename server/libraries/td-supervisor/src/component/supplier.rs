//
// Copyright 2024 Tabs Data Inc.
//

//! Module that provides all the required resources to run a worker under Tabsdata system.

use crate::component::supplier::SupplierError::*;
use crate::launch::worker::Worker;
use std::fmt::{Debug, Formatter};
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::{fmt, io};
use td_common::logging::LOG_LOCATION;
use td_common::manifest::{Inf, WORKER_INF_FILE};
use td_common::server::SupervisorMessagePayload::SupervisorRequestMessagePayload;
use td_common::server::{REQUEST_FILE, REQUEST_FOLDER};
use thiserror::Error;

/// Provides resources to a worker that can be run under the Tabsdata system.
pub trait WorkerSupplier: Debug {
    fn supply(&self, worker: &dyn Worker) -> Result<(), SupplierError>;
}

#[derive(Default)]
pub struct TabsDataWorkerSupplier;

impl TabsDataWorkerSupplier {
    pub fn new() -> Self {
        Self {}
    }
}

impl Debug for TabsDataWorkerSupplier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TabsDataWorkerSupplier").finish()
    }
}

// Default supplier.
impl WorkerSupplier for TabsDataWorkerSupplier {
    fn supply(&self, worker: &dyn Worker) -> Result<(), SupplierError> {
        if let Err(e) = create_dir_all(worker.describer().config()) {
            return Err(ConfigCreationError {
                describer: worker.describer().to_string(),
                cause: e,
            });
        };
        if let Err(e) = create_dir_all(worker.describer().work()) {
            return Err(WorkCreationError {
                describer: worker.describer().to_string(),
                cause: e,
            });
        };

        let log_path = worker.describer().work().join(LOG_LOCATION);
        create_dir_all(log_path)?;

        let inf = Inf {
            name: worker.describer().name().to_string(),
            config: worker.describer().config().clone(),
            work: worker.describer().work().clone(),
            queue: worker.describer().queue().clone(),
            etc: worker.describer().etc().clone(),
        };
        let inf_path = worker.describer().work().join(WORKER_INF_FILE);
        let mut inf_file = File::create(inf_path)?;
        let inf_yaml = serde_yaml::to_string(&inf)?;
        inf_file.write_all(inf_yaml.as_bytes())?;

        if worker.describer().message().is_some() {
            let request_path = worker.describer().work().join(REQUEST_FOLDER);
            create_dir_all(&request_path)?;
            let request_context_path = request_path.join(REQUEST_FILE);
            let mut request_context_file = File::create(request_context_path)?;
            let message = worker.describer().message().clone().unwrap();
            let request_context_yaml =
                if let SupervisorRequestMessagePayload(payload) = &message.payload {
                    serde_yaml::to_string(&payload.context())?
                } else {
                    return Err(InvalidMessageType);
                };
            request_context_file.write_all(request_context_yaml.as_bytes())?;
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct FunctionWorkerSupplier;

impl FunctionWorkerSupplier {
    pub fn new() -> Self {
        Self {}
    }
}

impl Debug for FunctionWorkerSupplier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionWorkerSupplier").finish()
    }
}

impl WorkerSupplier for FunctionWorkerSupplier {
    fn supply(&self, _worker: &dyn Worker) -> Result<(), SupplierError> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SupplierError {
    #[error("Error registering the config folder for '{describer}': {cause}")]
    ConfigCreationError { describer: String, cause: io::Error },
    #[error("Error registering the work folder for '{describer}': {cause}")]
    WorkCreationError { describer: String, cause: io::Error },
    #[error("Unexpected response message received.")]
    InvalidMessageType,
    #[error("An IO error occurred serializing the inf file: {0}")]
    SerdeError(#[from] serde_yaml::Error),
    #[error("An IO error occurred generating the inf file: {0}")]
    IOError(#[from] io::Error),
}
