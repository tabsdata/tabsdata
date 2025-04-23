//
// Copyright 2024 Tabs Data Inc.
//

//! Module that orchestrates all the components necessary to run a worker under Tabsdata system.

use crate::logic::platform::component::describer::{TabsDataWorkerDescriber, WorkerDescriber};
use crate::logic::platform::component::notifier::WorkerNotifier;
use crate::logic::platform::component::runner::RunnerError::*;
use crate::logic::platform::component::runner::*;
use crate::logic::platform::component::supplier::{TabsDataWorkerSupplier, WorkerSupplier};
use crate::logic::platform::component::tracker::WorkerTracker;
use getset::Getters;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::RwLock;
use td_common::execution_status::FunctionRunUpdateStatus;
use td_common::server::SupervisorMessage;
use td_common::server::SupervisorMessagePayload::{
    SupervisorRequestMessagePayload, SupervisorResponseMessagePayload,
};
use thiserror::Error;
use tokio::process::Child;

/// Worker is the top level component to manage workers executed under the main Tabsdata
/// supervisor or any nested supervisor.
/// It takes care of setting up all the necessary environment and context, starting the worker,
/// managing its lifecycle, and checking it's state, and terminating or relaunching it when
/// necessary.
/// It can also decide whether a worker needs to be restarted upon global system restart. All these
/// data are carried out by specialized components that implement one or more policies that
/// enable flexible and dynamic behavior.
pub trait Worker: Debug {
    fn describer(&self) -> &dyn WorkerDescriber;
    fn supplier(&self) -> &dyn WorkerSupplier;
    fn runner(&self) -> &dyn WorkerRunner;
    fn tracker(&self) -> &RwLock<WorkerTracker>;

    /// Runs the worker as a supervised task.
    fn work(
        &self,
        state: Option<String>,
    ) -> Result<(Child, Option<PathBuf>, Option<PathBuf>), RunnerError>;
}

/// Default implementation of the Worker trait.
#[derive(Debug, Getters)]
#[allow(dead_code)]
pub struct TabsDataWorker {
    /// Component to manage the worker description.
    describer: TabsDataWorkerDescriber,

    /// Component to manage the worker resources.
    supplier: TabsDataWorkerSupplier,

    /// Component to manage the worker execution.
    runner: TabsDataWorkerRunner,

    /// Component to track the worker execution.
    tracker: RwLock<WorkerTracker>,
}

impl TabsDataWorker {
    pub fn new(describer: TabsDataWorkerDescriber) -> Self {
        Self {
            describer: describer.clone(),
            supplier: TabsDataWorkerSupplier::new(),
            runner: TabsDataWorkerRunner::new(),
            tracker: RwLock::new(WorkerTracker::new(describer.work().clone())),
        }
    }
}

impl Worker for TabsDataWorker {
    fn describer(&self) -> &dyn WorkerDescriber {
        &self.describer
    }

    fn supplier(&self) -> &dyn WorkerSupplier {
        &self.supplier
    }

    fn runner(&self) -> &dyn WorkerRunner {
        &self.runner
    }

    fn tracker(&self) -> &RwLock<WorkerTracker> {
        &self.tracker
    }

    fn work(
        &self,
        state: Option<String>,
    ) -> Result<(Child, Option<PathBuf>, Option<PathBuf>), RunnerError> {
        match self.runner.run(self, state) {
            Ok((worker, out, err)) => {
                if let Some(id) = worker.id() {
                    let mut tracker = self.tracker().write().unwrap();
                    tracker.write_worker_pid_file(id as i32)?;
                    Ok((worker, out, err))
                } else {
                    Err(MissingProcessId)
                }
            }
            Err(e) => Err(e),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn notify(
    worker: Option<&TabsDataWorker>,
    request_message: SupervisorMessage,
    start: i64,
    end: Option<i64>,
    status: FunctionRunUpdateStatus,
    execution: i16,
    limit: Option<i16>,
    error: Option<String>,
) -> Result<(), RunnerError> {
    let payload = match request_message.payload() {
        SupervisorRequestMessagePayload(payload) => payload,
        SupervisorResponseMessagePayload(_) => {
            return Err(InvalidMessageType);
        }
    };
    if let Some(callback) = payload.callback() {
        callback
            .clone()
            .notify(
                worker,
                request_message,
                start,
                end,
                status,
                execution,
                limit,
                error,
            )
            .await?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("An error occurred running the worker: {0}")]
    RunnerFailure(#[from] RunnerError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bin::platform::supervisor::WorkerLocation::RELATIVE;
    use crate::logic::platform::component::describer::TabsDataWorkerDescriberBuilder;
    use crate::logic::platform::resource::instance::{CONFIG_FOLDER, MSG_FOLDER, WORK_FOLDER};
    use std::fs::create_dir_all;
    use td_common::env::{get_current_exe_name, get_current_exe_path};
    use td_common::server::WorkerClass::REGULAR;
    use tempfile::tempdir;

    #[test]
    fn test_describer() {
        let workspace_folder = tempdir().unwrap();
        let config_folder = workspace_folder.path().to_path_buf().join(CONFIG_FOLDER);
        create_dir_all(&config_folder).expect("Error creating config folder");
        let work_folder = workspace_folder.path().to_path_buf().join(WORK_FOLDER);
        create_dir_all(&work_folder).expect("Error creating work folder");
        let describer = TabsDataWorkerDescriberBuilder::default()
            .class(REGULAR)
            .name(get_current_exe_name().unwrap())
            .location(RELATIVE)
            .program(get_current_exe_path().expect("Error getting current running program"))
            .set_state(None)
            .get_states(vec![])
            .arguments(Vec::new())
            .config(config_folder)
            .work(work_folder.clone())
            .queue(work_folder.clone().join(MSG_FOLDER))
            .build();
        assert!(describer.is_ok());
        let worker = TabsDataWorker::new(describer.unwrap());
        let describer = &worker.describer;
        let name = describer.name();
        assert_eq!(
            name,
            &get_current_exe_name().unwrap(),
            "Describer error: {:?}",
            name
        );
    }
    #[test]
    fn test_supplier() {
        let workspace_folder = tempdir().unwrap();
        let config_folder = workspace_folder.path().to_path_buf().join(CONFIG_FOLDER);
        create_dir_all(&config_folder).expect("Error creating config folder");
        let work_folder = workspace_folder.path().to_path_buf().join(WORK_FOLDER);
        create_dir_all(&work_folder).expect("Error creating work folder");
        let describer = TabsDataWorkerDescriberBuilder::default()
            .class(REGULAR)
            .name(get_current_exe_name().unwrap())
            .location(RELATIVE)
            .program(get_current_exe_path().expect("Error getting current running program"))
            .set_state(None)
            .get_states(vec![])
            .arguments(Vec::new())
            .config(config_folder)
            .work(work_folder.clone())
            .queue(work_folder.clone().join(MSG_FOLDER))
            .build();
        assert!(describer.is_ok());
        let worker = TabsDataWorker::new(describer.unwrap());
        let supplier = &worker.supplier;
        let result = supplier.supply(&worker);
        assert!(result.is_ok(), "Supplier error: {:?}", result.unwrap_err());
    }
}
