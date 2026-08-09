//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use td_error::TdError;

use crate::base::constants::VAULT_FILE;
use crate::base::error::cli::CliError;
use crate::client::registry::instance_folder;
use crate::core::system::exit::CommandResult;
use crate::runtime::secrets::generator::Generator;

pub fn instance_vault_file(name: &str) -> CommandResult<PathBuf> {
    Ok(instance_folder(name)?.join(VAULT_FILE))
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct InstanceVault {
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
}

impl InstanceVault {
    pub fn load(name: &str) -> CommandResult<Self> {
        let path = instance_vault_file(name)?;
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(&path).map_err(|error| {
            TdError::from(CliError::FileParseFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        let vault = serde_json::from_str(&data).map_err(|error| {
            TdError::from(CliError::FileParseFailed {
                file: path.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        Ok(vault)
    }

    pub fn save(&self, name: &str) -> CommandResult {
        let path = instance_vault_file(name)?;
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
        restrict(&path)
    }

    pub fn value_for(&mut self, category: &str) -> String {
        if let Some(value) = self.secrets.get(category) {
            return value.clone();
        }
        let value = match Generator::from_name(category) {
            Some(generator) => generator
                .generate(&BTreeMap::new())
                .unwrap_or_else(|_| td_common::random::password()),
            None => td_common::random::password(),
        };
        self.secrets.insert(category.to_owned(), value.clone());
        value
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }
}

#[cfg(unix)]
fn restrict(path: &Path) -> CommandResult {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: path.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict(_path: &Path) -> CommandResult {
    Ok(())
}

#[cfg(test)]
mod tests;
