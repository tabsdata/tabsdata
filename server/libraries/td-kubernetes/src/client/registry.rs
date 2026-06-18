//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use td_common::env::TABSDATA_HOME_DIR;
use td_common::settings::INSTANCES_FOLDER;
use td_error::TdError;

use crate::base::constants::{INIT_FOLDER, SETTINGS_FILE};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::axes::Locality;

pub fn instances_root() -> CommandResult<PathBuf> {
    let home = homedir::my_home()
        .ok()
        .flatten()
        .ok_or(ProviderError::HomeDirNotFound)?;
    Ok(home.join(TABSDATA_HOME_DIR).join(INSTANCES_FOLDER))
}

pub fn instance_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instances_root()?.join(name))
}

pub fn instance_init_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(INIT_FOLDER))
}

pub fn instance_settings_file(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(SETTINGS_FILE))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSettings {
    pub name: String,
    pub provider: ClusterProvider,
    pub locality: Locality,
}

impl InstanceSettings {
    pub fn load(name: &str) -> CommandResult<Self> {
        let path = instance_settings_file(name)?;
        let data = fs::read_to_string(&path).map_err(|error| {
            TdError::from(CliError::FileReadFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        let settings = serde_json::from_str(&data).map_err(|error| {
            TdError::from(CliError::FileParseFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        Ok(settings)
    }

    pub fn save(&self) -> CommandResult {
        let path = instance_settings_file(&self.name)?;
        let data = serde_json::to_string_pretty(self).map_err(|error| {
            TdError::from(CliError::FileSerializeFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        fs::write(&path, data).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        Ok(())
    }
}
