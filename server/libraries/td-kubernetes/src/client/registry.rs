//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use td_common::env::TABSDATA_HOME_DIR;
use td_common::settings::INSTANCES_FOLDER;
use td_error::TdError;

use td_python::python::get_python_scripts_folder;

#[cfg(feature = "enterprise")]
use crate::base::constants::FOLDER_PYENV_AG;
use crate::base::constants::{
    CLUSTER_CONFIG_FOLDER, CLUSTER_FOLDER, EDIT_FOLDER, FOLDER_KENVS, FOLDER_PYENV_FN,
    FOLDER_PYENV_TD, FOLDER_RUNTIMES, FOLDER_UV, INIT_FOLDER, INPUT_FOLDER, KUBE_FOLDER,
    KUBECONFIG_FILE, LogLevel, SETTINGS_FILE, STAGE_FOLDER, TDKSERVER_LOG_FILE, TRASH_FOLDER,
    VENV_ACTIVATE_FILE,
};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::ports::AssignedPort;
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

pub fn trash_root() -> CommandResult<PathBuf> {
    let home = homedir::my_home()
        .ok()
        .flatten()
        .ok_or(ProviderError::HomeDirNotFound)?;
    Ok(home.join(TABSDATA_HOME_DIR).join(TRASH_FOLDER))
}

pub fn instance_init_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(INIT_FOLDER))
}

pub fn instance_edit_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(EDIT_FOLDER))
}

pub fn instance_init_input_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(INIT_FOLDER).join(INPUT_FOLDER))
}

pub fn instance_init_stage_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(INIT_FOLDER).join(STAGE_FOLDER))
}

pub fn instance_edit_input_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(EDIT_FOLDER).join(INPUT_FOLDER))
}

pub fn instance_edit_stage_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(EDIT_FOLDER).join(STAGE_FOLDER))
}

pub fn init_stage_folder_in(instance_root: &Path) -> PathBuf {
    instance_root.join(INIT_FOLDER).join(STAGE_FOLDER)
}

pub fn instance_settings_file(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(SETTINGS_FILE))
}

pub fn tdkserver_log_file() -> CommandResult<PathBuf> {
    let home = homedir::my_home()
        .ok()
        .flatten()
        .ok_or(ProviderError::HomeDirNotFound)?;
    Ok(home.join(TABSDATA_HOME_DIR).join(TDKSERVER_LOG_FILE))
}

pub fn instance_kube_folder(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(KUBE_FOLDER))
}

fn venv_activate_file(venv_root: &Path) -> CommandResult<PathBuf> {
    let scripts_folder = get_python_scripts_folder()?;
    let scripts_name = scripts_folder.file_name().ok_or_else(|| {
        ProviderError::PythonScriptsFolderNameInvalid(scripts_folder.display().to_string())
    })?;
    Ok(venv_root.join(scripts_name).join(VENV_ACTIVATE_FILE))
}

pub fn venvs_root(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?
        .join(FOLDER_RUNTIMES)
        .join(FOLDER_UV)
        .join(FOLDER_KENVS))
}

pub fn td_venv_activate_file(name: &str) -> CommandResult<PathBuf> {
    let venv_root = instance_folder(name)?
        .join(FOLDER_RUNTIMES)
        .join(FOLDER_UV)
        .join(FOLDER_KENVS)
        .join(FOLDER_PYENV_TD);
    venv_activate_file(&venv_root)
}

pub fn fn_venv_activate_file(name: &str) -> CommandResult<PathBuf> {
    let venv_root = instance_folder(name)?
        .join(FOLDER_RUNTIMES)
        .join(FOLDER_UV)
        .join(FOLDER_KENVS)
        .join(FOLDER_PYENV_FN);
    venv_activate_file(&venv_root)
}

#[cfg(feature = "enterprise")]
pub fn ag_venv_activate_file(name: &str) -> CommandResult<PathBuf> {
    let venv_root = instance_folder(name)?
        .join(FOLDER_RUNTIMES)
        .join(FOLDER_UV)
        .join(FOLDER_KENVS)
        .join(FOLDER_PYENV_AG);
    venv_activate_file(&venv_root)
}

pub fn kminus_kubeconfig_file(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?
        .join(CLUSTER_FOLDER)
        .join(CLUSTER_CONFIG_FOLDER)
        .join(KUBECONFIG_FILE))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterViews {
    pub provided: ClusterProperties,
    pub adjusted: ClusterSettings,
    pub resolved: ClusterDescriptor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Initialized,
    Created,
    Registered,
}

impl InstanceState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Created => "created",
            Self::Registered => "registered",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSettings {
    pub state: InstanceState,
    pub name: String,
    pub provider: ClusterProvider,
    pub locality: Locality,
    pub cluster: ClusterViews,
    pub version: String,
    pub folder: PathBuf,
    #[serde(default)]
    pub ports: Vec<AssignedPort>,
    #[serde(default)]
    pub loglevel: Option<LogLevel>,
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

    pub fn load_from_path(path: &Path) -> CommandResult<Self> {
        let data = fs::read_to_string(path).map_err(|error| {
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

    pub fn load_required(name: &str, expected: &[InstanceState]) -> CommandResult<Self> {
        let settings_file = instance_settings_file(name)?;
        if !settings_file.exists() {
            return Err(ProviderError::InstanceNotInitialized {
                name: name.to_owned(),
                reason: format!("its settings file '{}' is missing", settings_file.display()),
            }
            .into());
        }
        let settings = Self::load(name).map_err(|_| ProviderError::InstanceNotInitialized {
            name: name.to_owned(),
            reason: format!(
                "its settings file '{}' could not be read",
                settings_file.display()
            ),
        })?;
        settings.require_one_of(expected)?;
        Ok(settings)
    }

    pub fn require_one_of(&self, expected: &[InstanceState]) -> CommandResult {
        if expected.contains(&self.state) {
            return Ok(());
        }
        let expected = expected
            .iter()
            .map(|state| state.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        Err(ProviderError::UnexpectedInstanceState {
            name: self.name.clone(),
            found: self.state.as_str().to_owned(),
            expected,
        }
        .into())
    }
}

pub struct InstanceListing {
    pub name: String,
    pub folder: PathBuf,
    pub settings: Option<InstanceSettings>,
}

pub fn list_instances() -> CommandResult<Vec<InstanceListing>> {
    let root = instances_root()?;
    let mut listings = Vec::new();
    if !root.exists() {
        return Ok(listings);
    }
    let entries = fs::read_dir(&root).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: root.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    for entry in entries.flatten() {
        let folder = entry.path();
        if !folder.is_dir() {
            continue;
        }
        let Some(name) = folder.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let settings = InstanceSettings::load(name).ok();
        listings.push(InstanceListing {
            name: name.to_owned(),
            folder,
            settings,
        });
    }
    listings.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(listings)
}
