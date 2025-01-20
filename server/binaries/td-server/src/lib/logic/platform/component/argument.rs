//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::platform::resource::instance::{
    BIN_FOLDER, INPUT_FOLDER, LOCK_FOLDER, LOG_FOLDER, OUTPUT_FOLDER, REQUEST_FOLDER,
    RESPONSE_FOLDER,
};
use std::ffi::OsString;
use std::path::PathBuf;
use strum_macros::{AsRefStr, EnumIter, EnumString};
use td_common::env::{to_absolute, EnvironmentError};
use thiserror::Error;

#[derive(Debug, Clone, EnumIter, EnumString, AsRefStr)]
#[strum(serialize_all = "kebab-case")]
pub enum InheritedArgumentKey {
    Name,
    Profile,
    Instance,
    Repository,
    Workspace,
    Conf,
    Work,
}

#[derive(Debug, Clone, EnumIter, EnumString, AsRefStr)]
#[strum(serialize_all = "kebab-case")]
pub enum ArgumentKey {
    LocksFolder,
    LogsFolder,
    BinFolder,
    RequestFolder,
    ResponseFolder,
    InputFolder,
    OutputFolder,
    CurrentInstance,
}

impl ArgumentKey {
    pub fn produce(
        &self,
        instance: PathBuf,
        parent_work: PathBuf,
        child_work: PathBuf,
    ) -> Result<String, ArgumentError> {
        match self {
            ArgumentKey::LocksFolder => ArgumentKey::locks_folder(parent_work),
            ArgumentKey::LogsFolder => ArgumentKey::log_folder(child_work),
            ArgumentKey::BinFolder => ArgumentKey::bin_folder(child_work),
            ArgumentKey::RequestFolder => ArgumentKey::request_folder(child_work),
            ArgumentKey::ResponseFolder => ArgumentKey::response_folder(child_work),
            ArgumentKey::InputFolder => ArgumentKey::input_folder(child_work),
            ArgumentKey::OutputFolder => ArgumentKey::output_folder(child_work),
            ArgumentKey::CurrentInstance => ArgumentKey::current_instance(instance),
        }
    }

    fn locks_folder(parent_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&parent_work.join(LOCK_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn log_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(LOG_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn bin_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(BIN_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn request_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(REQUEST_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn response_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(RESPONSE_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn input_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(INPUT_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn output_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&child_work.join(OUTPUT_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn current_instance(instance: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&instance)?
            .file_name()
            .unwrap_or(OsString::new().as_os_str())
            .to_string_lossy()
            .to_string())
    }
}

#[derive(Debug, Error)]
pub enum ArgumentError {
    #[error("Unexpected environment error: {0}")]
    EnvironmentFailure(#[from] EnvironmentError),
}
