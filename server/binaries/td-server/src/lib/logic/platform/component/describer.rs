//
// Copyright 2024 Tabs Data Inc.
//

//! Module that provides all the properties that describe a worker run under the Tabsdata system.

use crate::bin::supervisor::WorkerLocation;
use crate::logic::platform::component::describer::DescriberError::*;
use crate::logic::platform::resource::instance::{get_program_path, InstanceError};
use derive_builder::{Builder, UninitializedFieldError};
use getset::{Getters, Setters};
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;
use td_common::env::{to_absolute, EnvironmentError};
use td_common::server::SupervisorMessage;
use thiserror::Error;

/// Describes a worker that can be run under the Tabsdata system.
pub trait WorkerDescriber: Display + Debug {
    fn name(&self) -> &String;
    fn location(&self) -> &WorkerLocation;
    fn program(&self) -> &PathBuf;
    fn arguments(&self) -> &Vec<String>;
    fn config(&self) -> &PathBuf;
    fn message(&self) -> &Option<SupervisorMessage>;
    fn work(&self) -> &PathBuf;
    fn queue(&self) -> &PathBuf;
}

// Default worker describer.
#[derive(Clone, Debug, Getters, Setters, Builder)]
#[builder(
    setter(into),
    build_fn(
        error = "DescriberError",
        validate = "Self::validate",
        private,
        name = "build_internal"
    )
)]
#[getset(get = "pub")]
pub struct TabsDataWorkerDescriber {
    /// Name of the worker to run.
    name: String,

    /// Location of the program to run.
    location: WorkerLocation,

    /// Path of the program to run.
    program: PathBuf,

    /// Arguments to pass to the program to run.
    arguments: Vec<String>,

    /// Configuration folder of the worker to run.
    config: PathBuf,

    /// Work folder of the worker to run.
    work: PathBuf,

    /// Supervisor queue folder.
    queue: PathBuf,

    /// Messages that triggers the worker to run execution.
    #[builder(default)]
    message: Option<SupervisorMessage>,
}

impl TabsDataWorkerDescriberBuilder {
    fn validate(&self) -> Result<(), DescriberError> {
        if self.name.as_ref().is_none_or(|n| n.trim().is_empty()) {
            return Err(MissingWorkerName);
        }
        if let Some(program) = &self.program {
            if let Some(WorkerLocation::RELATIVE) = &self.location {
                if program
                    .as_os_str()
                    .to_string_lossy()
                    .to_string()
                    .trim()
                    .is_empty()
                {
                    return Err(MissingProgram);
                }
                get_program_path(program)?;
            }
        }
        if let Some(config) = &self.config {
            if config
                .as_os_str()
                .to_string_lossy()
                .to_string()
                .trim()
                .is_empty()
            {
                return Err(MissingConfigFolder);
            }
            to_absolute(config)?;
        }
        if let Some(work) = &self.work {
            if work.as_os_str().is_empty() {
                return Err(MissingWorkFolder);
            }
            to_absolute(work)?;
        }
        if let Some(queue) = &self.queue {
            if queue.as_os_str().is_empty() {
                return Err(MissingQueueFolder);
            }
            to_absolute(queue)?;
        }
        Ok(())
    }

    pub fn get_name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub fn get_location(&self) -> Option<&WorkerLocation> {
        self.location.as_ref()
    }

    pub fn get_program(&self) -> Option<&PathBuf> {
        self.program.as_ref()
    }

    pub fn get_config(&self) -> Option<&PathBuf> {
        self.config.as_ref()
    }

    pub fn get_work(&self) -> Option<&PathBuf> {
        self.work.as_ref()
    }

    pub fn get_queue(&self) -> Option<&PathBuf> {
        self.queue.as_ref()
    }

    pub fn get_message(&self) -> Option<&Option<SupervisorMessage>> {
        self.message.as_ref()
    }

    pub fn build(&mut self) -> Result<TabsDataWorkerDescriber, DescriberError> {
        if let Some(WorkerLocation::RELATIVE) = &self.location {
            if let Ok(program) = get_program_path(self.get_program().as_ref().unwrap()) {
                self.program(program);
            }
        }
        if let Ok(config) = to_absolute(self.get_config().as_ref().unwrap()) {
            self.config(config);
        }
        if let Ok(work) = to_absolute(self.get_work().as_ref().unwrap()) {
            self.work(work);
        }
        self.build_internal()
    }
}

impl WorkerDescriber for TabsDataWorkerDescriber {
    fn name(&self) -> &String {
        &self.name
    }

    fn location(&self) -> &WorkerLocation {
        &self.location
    }

    fn program(&self) -> &PathBuf {
        &self.program
    }

    fn arguments(&self) -> &Vec<String> {
        &self.arguments
    }

    fn config(&self) -> &PathBuf {
        &self.config
    }

    fn message(&self) -> &Option<SupervisorMessage> {
        &self.message
    }

    fn work(&self) -> &PathBuf {
        &self.work
    }

    fn queue(&self) -> &PathBuf {
        &self.queue
    }
}

impl Display for TabsDataWorkerDescriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Name: {}\n\
             Location: {:?}\n\
             Program: {:?}\n\
             Arguments: {:?}\n\
             Config: {:?}\n\
             Message:\n{}\n\
             Work: {:?}\n\
             Queue: {:?}",
            &self.name,
            &self.location,
            &self.program,
            &self.arguments,
            &self.config,
            match &self.message {
                Some(message) => serde_yaml::to_string(&message)
                    .unwrap_or("<non serializable message>".to_string()),
                None => "No message...".to_string(),
            },
            &self.work,
            &self.queue,
        )
    }
}

#[derive(Debug, Error)]
pub enum DescriberError {
    #[error("Worker name not specified")]
    NoWorkerName,
    #[error("Worker name not provided")]
    MissingWorkerName,
    #[error("Program not specified")]
    NoProgram,
    #[error("Program not provided")]
    MissingProgram,
    #[error("Non existing program: {program}")]
    NonExistingProgram { program: PathBuf },
    #[error("Config folder not specified")]
    NoConfig,
    #[error("Work folder not specified")]
    NoWork,
    #[error("Config folder not provided")]
    MissingConfigFolder,
    #[error("Non existing config: {config}")]
    NonExistingConfig { config: PathBuf },
    #[error("Work folder not provided")]
    MissingWorkFolder,
    #[error("Non existing work: {work}")]
    NonExistingWork { work: PathBuf },
    #[error("Queue folder not provided")]
    MissingQueueFolder,
    #[error("Non existing queue: {queue}")]
    NonExistingQueue { queue: PathBuf },
    #[error("Unexpected error from instance processing: {0}")]
    InstanceFailure(#[from] InstanceError),
    #[error("Unexpected error from environment processing: {0}")]
    EnvironmentFailure(#[from] EnvironmentError),
    #[error("Unexpected field management error: {0}")]
    UninitializedFieldFailure(#[from] UninitializedFieldError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bin::supervisor::WorkerLocation::RELATIVE;
    use crate::logic::platform::resource::instance::{CONFIG_FOLDER, MSG_FOLDER, WORK_FOLDER};
    use std::fs::create_dir_all;
    use std::path::PathBuf;
    use td_common::env::{get_current_exe_name, get_current_exe_path};
    use tempfile::tempdir;

    #[test]
    fn test_valid_data() {
        let workspace_folder = tempdir().unwrap();
        let config_folder = workspace_folder.path().to_path_buf().join(CONFIG_FOLDER);
        create_dir_all(&config_folder).expect("Error creating config folder");
        let work_folder = workspace_folder.path().to_path_buf().join(WORK_FOLDER);
        create_dir_all(&work_folder).expect("Error creating work folder");
        let describer = TabsDataWorkerDescriberBuilder::default()
            .name(get_current_exe_name().unwrap())
            .location(RELATIVE)
            .program(get_current_exe_path().expect("Error getting current running program"))
            .arguments(vec!["--arg1".to_string(), "--arg2".to_string()])
            .config(config_folder)
            .work(work_folder.clone())
            .queue(work_folder.clone().join(MSG_FOLDER))
            .build();
        assert!(describer.is_ok());
    }

    #[test]
    fn test_missing_name() {
        let describer = TabsDataWorkerDescriberBuilder::default()
            .name(" ".to_string())
            .location(RELATIVE)
            .program(get_current_exe_path().expect("Error getting current running program"))
            .arguments(vec!["--arg1".to_string(), "--arg2".to_string()])
            .config("".to_string())
            .work("".to_string())
            .queue("".to_string())
            .build();
        assert!(describer.is_err());
    }

    #[test]
    fn test_non_existing_program() {
        let describer = TabsDataWorkerDescriberBuilder::default()
            .name("non_existing_program".to_string())
            .location(RELATIVE)
            .program(PathBuf::from("/non/existing/program"))
            .arguments(vec!["--arg1".to_string(), "--arg2".to_string()])
            .config("".to_string())
            .work("".to_string())
            .queue("".to_string())
            .build();
        assert!(describer.is_err());
    }

    #[test]
    fn test_no_arguments() {
        let workspace_folder = tempdir().unwrap();
        let config_folder = workspace_folder.path().to_path_buf().join(CONFIG_FOLDER);
        create_dir_all(&config_folder).expect("Error creating config folder");
        let work_folder = workspace_folder.path().to_path_buf().join(WORK_FOLDER);
        create_dir_all(&work_folder).expect("Error creating work folder");
        let describer = TabsDataWorkerDescriberBuilder::default()
            .name(get_current_exe_name().unwrap())
            .location(RELATIVE)
            .program(get_current_exe_path().expect("Error getting current running program"))
            .arguments(Vec::new())
            .config(config_folder)
            .work(work_folder.clone())
            .queue(work_folder.clone().join(MSG_FOLDER))
            .build();
        assert!(describer.is_ok());
    }
}
