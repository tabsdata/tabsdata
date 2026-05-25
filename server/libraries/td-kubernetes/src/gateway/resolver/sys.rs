//
// Copyright 2026 Tabs Data Inc.
//

//! `${sys:KEY}` resolution — substitutes a predefined set of system values
//! passed by the caller through [`SystemValues`].

use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use strum::{AsRefStr, EnumIter, EnumString, IntoEnumIterator};
use td_error::TdError;

use crate::error::gateway::ResolverError;

static PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{sys:(\w+)}").expect("invalid sys regex"));

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, EnumString, AsRefStr, EnumIter)]
pub enum SystemKey {
    #[strum(serialize = "TDK_REPOSITORY_URL")]
    RepositoryUrl,
    #[strum(serialize = "TDK_REPOSITORY_PATH")]
    RepositoryPath,
    #[strum(serialize = "TDK_DATABASE_URL")]
    DatabaseUrl,
    #[strum(serialize = "TDK_DATABASE_PATH")]
    DatabasePath,
    #[strum(serialize = "TDK_STORAGE_URL")]
    StorageUrl,
    #[strum(serialize = "TDK_STORAGE_PATH")]
    StoragePath,
    #[strum(serialize = "TDK_PYENV_TD_ROOT")]
    PyenvTdRoot,
    #[strum(serialize = "TDK_PYENV_AG_ROOT")]
    PyenvAgRoot,
    #[strum(serialize = "TDK_PYENV_SCRIPTS_FOLDER")]
    PyenvScriptsFolder,
}

impl SystemKey {
    fn supported() -> String {
        Self::iter()
            .map(|key| key.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, Clone, Default)]
pub struct SystemValues {
    pub repository_url: Option<String>,
    pub repository_path: Option<String>,
    pub database_url: Option<String>,
    pub database_path: Option<String>,
    pub storage_url: Option<String>,
    pub storage_path: Option<String>,
    pub pyenv_td_root: Option<String>,
    pub pyenv_ag_root: Option<String>,
    pub pyenv_scripts_folder: Option<String>,
}

impl SystemValues {
    fn lookup(&self, key: SystemKey) -> Option<&str> {
        match key {
            SystemKey::RepositoryUrl => self.repository_url.as_deref(),
            SystemKey::RepositoryPath => self.repository_path.as_deref(),
            SystemKey::DatabaseUrl => self.database_url.as_deref(),
            SystemKey::DatabasePath => self.database_path.as_deref(),
            SystemKey::StorageUrl => self.storage_url.as_deref(),
            SystemKey::StoragePath => self.storage_path.as_deref(),
            SystemKey::PyenvTdRoot => self.pyenv_td_root.as_deref(),
            SystemKey::PyenvAgRoot => self.pyenv_ag_root.as_deref(),
            SystemKey::PyenvScriptsFolder => self.pyenv_scripts_folder.as_deref(),
        }
    }
}

pub(super) fn resolve(leaf: &str, values: &SystemValues) -> Result<String, TdError> {
    let mut current = leaf.to_string();
    while let Some(captures) = PATTERN.captures(&current) {
        let placeholder = captures.get(0).unwrap().as_str().to_string();
        let raw_key = captures.get(1).unwrap().as_str();
        let key =
            SystemKey::from_str(raw_key).map_err(|_| ResolverError::UnsupportedSystemKey {
                key: raw_key.to_string(),
                supported: SystemKey::supported(),
            })?;
        let value = values
            .lookup(key)
            .ok_or_else(|| ResolverError::MissingSystemValue {
                key: key.as_ref().to_string(),
            })?;
        current = current.replace(&placeholder, value);
    }
    Ok(current)
}
