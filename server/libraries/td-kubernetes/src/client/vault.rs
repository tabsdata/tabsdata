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
use crate::runtime::gateway::resolver::fusion::FusedSecrets;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::placeholder_occurrences;
use crate::runtime::instance::profile::Profile;
use crate::runtime::secrets::generator::Generator;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Vault {
    #[serde(skip)]
    instance: String,
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

impl Vault {
    pub fn of(instance: &str) -> CommandResult<Self> {
        let path = Self::file(instance)?;
        let mut vault = if path.exists() {
            let data = fs::read_to_string(&path).map_err(|error| {
                TdError::from(CliError::FileParseFailed {
                    file: path.display().to_string(),
                    cause: error.to_string(),
                })
            })?;
            serde_json::from_str::<Self>(&data).map_err(|error| {
                TdError::from(CliError::FileParseFailed {
                    file: path.display().to_string(),
                    cause: error.to_string(),
                })
            })?
        } else {
            Self::default()
        };
        vault.instance = instance.to_owned();
        Ok(vault)
    }

    pub fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }

    pub fn provision(&mut self, profile: &Profile) -> CommandResult {
        let mut provisioned = false;
        for content in profile.configs.values() {
            for placeholder in placeholder_occurrences(content) {
                let Ok(kind) = placeholder.kind.parse::<PlaceholderKind>() else {
                    continue;
                };
                if !matches!(kind, PlaceholderKind::Secret | PlaceholderKind::SecretAuto) {
                    continue;
                }
                let Some(category) = placeholder.category.filter(|category| !category.is_empty())
                else {
                    continue;
                };
                provisioned |= self.provision_category(&category);
            }
        }
        if provisioned { self.save() } else { Ok(()) }
    }

    pub fn persist(&mut self, fused: &FusedSecrets) -> CommandResult {
        let mut stored = false;
        for (_, entries) in &fused.by_file {
            for entry in entries {
                let Some(category) = entry.category.as_deref() else {
                    continue;
                };
                stored |= self.store(category, &entry.value);
            }
        }
        if stored { self.save() } else { Ok(()) }
    }

    pub(crate) fn provision_category(&mut self, category: &str) -> bool {
        match Generator::from_name(category) {
            Some(generator) if generator.is_storable() => {
                if self.secrets.contains_key(category) {
                    return false;
                }
                let value = generator.generate();
                self.secrets.insert(category.to_owned(), value);
                true
            }
            _ => false,
        }
    }

    pub(crate) fn store(&mut self, category: &str, value: &str) -> bool {
        if value.is_empty() || !Generator::is_storable_category(category) {
            return false;
        }
        if self.secrets.get(category).is_some_and(|held| held == value) {
            return false;
        }
        self.secrets.insert(category.to_owned(), value.to_owned());
        true
    }

    fn file(instance: &str) -> CommandResult<PathBuf> {
        Ok(instance_folder(instance)?.join(VAULT_FILE))
    }

    fn save(&self) -> CommandResult {
        Ok(())
        /*
        let path = Self::file(&self.instance)?;
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
        */
    }
}

#[allow(dead_code)]
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

#[allow(dead_code)]
#[cfg(not(unix))]
fn restrict(_path: &Path) -> CommandResult {
    Ok(())
}

#[cfg(test)]
mod tests;
