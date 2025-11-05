//
// Copyright 2025 Tabs Data Inc.
//

use std::ffi::OsString;
use std::path::PathBuf;
use strum::{AsRefStr, EnumIter, EnumString};
use td_common::env::{EnvironmentError, to_absolute};
use td_common::server::{
    BIN_FOLDER, INPUT_FOLDER, LOCK_FOLDER, LOG_FOLDER, OUTPUT_FOLDER, REQUEST_FOLDER,
    RESPONSE_FOLDER, RequestMessagePayload, SupervisorMessage, base, counter,
};
use td_objects::dxo::request::FunctionInput;
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
    Work,
    LocksFolder,
    LogsFolder,
    BinFolder,
    RequestFolder,
    ResponseFolder,
    InputFolder,
    OutputFolder,
    CurrentInstance,
}

#[derive(Debug, Clone, EnumIter, EnumString, AsRefStr)]
#[strum(serialize_all = "kebab-case")]
pub enum MarkerKey {
    TdCollection,
    TdFunction,
    TdWorker,
    TdAttempt,
}

impl ArgumentKey {
    pub fn produce(
        &self,
        instance: PathBuf,
        parent_work: PathBuf,
        child_work: PathBuf,
    ) -> Result<String, ArgumentError> {
        match self {
            ArgumentKey::Work => ArgumentKey::work(child_work),
            ArgumentKey::LocksFolder => ArgumentKey::locks_folder(parent_work),
            ArgumentKey::LogsFolder => ArgumentKey::logs_folder(child_work),
            ArgumentKey::BinFolder => ArgumentKey::bin_folder(child_work),
            ArgumentKey::RequestFolder => ArgumentKey::request_folder(child_work),
            ArgumentKey::ResponseFolder => ArgumentKey::response_folder(child_work),
            ArgumentKey::InputFolder => ArgumentKey::input_folder(child_work),
            ArgumentKey::OutputFolder => ArgumentKey::output_folder(child_work),
            ArgumentKey::CurrentInstance => ArgumentKey::current_instance(instance),
        }
    }

    fn work(child_work: PathBuf) -> Result<String, ArgumentError> {
        child_work
            .parent()
            .map_or_else(|| Ok("0".to_string()), |p| Ok(counter(p)))
    }

    fn locks_folder(parent_work: PathBuf) -> Result<String, ArgumentError> {
        Ok(to_absolute(&parent_work.join(LOCK_FOLDER))?
            .as_os_str()
            .to_string_lossy()
            .to_string())
    }

    fn logs_folder(child_work: PathBuf) -> Result<String, ArgumentError> {
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

impl MarkerKey {
    pub fn produce<T>(
        &self,
        message: &SupervisorMessage<T>,
        pyload: &RequestMessagePayload<T>,
    ) -> Result<String, ArgumentError>
    where
        T: Clone + TryInto<FunctionInput>,
        <T as TryInto<FunctionInput>>::Error: std::fmt::Display,
    {
        match self {
            MarkerKey::TdCollection => MarkerKey::collection(pyload),
            MarkerKey::TdFunction => MarkerKey::function(pyload),
            MarkerKey::TdWorker => MarkerKey::worker(message),
            MarkerKey::TdAttempt => MarkerKey::attempt(message),
        }
    }

    fn collection<T>(payload: &RequestMessagePayload<T>) -> Result<String, ArgumentError>
    where
        T: Clone + TryInto<FunctionInput>,
    {
        if let Some(context) = payload.context() {
            let Ok(context) = context.clone().try_into() else {
                return Ok("!".to_string());
            };
            match context {
                FunctionInput::V0(_) => Ok("?".to_string()),
                FunctionInput::V2(v2) => Ok(v2.info.collection.to_string()),
            }
        } else {
            Ok("!".to_string())
        }
    }

    fn function<T>(payload: &RequestMessagePayload<T>) -> Result<String, ArgumentError>
    where
        T: Clone + TryInto<FunctionInput>,
    {
        if let Some(context) = payload.context() {
            let Ok(context) = context.clone().try_into() else {
                return Ok("!".to_string());
            };
            match context {
                FunctionInput::V0(_) => Ok("?".to_string()),
                FunctionInput::V2(v2) => Ok(v2.info.function.to_string()),
            }
        } else {
            Ok("!".to_string())
        }
    }

    fn worker<T: Clone>(message: &SupervisorMessage<T>) -> Result<String, ArgumentError> {
        if let Some(file_stem) = message.file.file_stem() {
            Ok(base(file_stem.to_string_lossy().to_string().as_str()))
        } else {
            Ok("!".to_string())
        }
    }

    fn attempt<T: Clone>(message: &SupervisorMessage<T>) -> Result<String, ArgumentError> {
        Ok(counter(&message.file))
    }
}

#[derive(Debug, Error)]
pub enum ArgumentError {
    #[error("Unexpected environment error: {0}")]
    EnvironmentFailure(#[from] EnvironmentError),
}
