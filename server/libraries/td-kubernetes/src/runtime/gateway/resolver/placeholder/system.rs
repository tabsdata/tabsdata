//
// Copyright 2026 Tabs Data Inc.
//

//! System values: the [`SystemKey`] catalog, the caller-supplied [`SystemValues`],
//! and the [`SystemResolver`] that resolves a `${{system:KEY}}` placeholder against
//! them.

use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use strum::{AsRefStr, EnumIter, EnumString, IntoEnumIterator};
use td_error::TdError;

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, PATH_SEPARATOR, Resolution, ResolveContext,
};

static PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{sys:(\w+)}").expect("The system-value placeholder pattern is invalid.")
});

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, EnumString, AsRefStr, EnumIter)]
pub enum SystemKey {
    #[strum(serialize = "tdk_repository_url")]
    RepositoryUrl,
    #[strum(serialize = "tdk_repository_path")]
    RepositoryPath,
    #[strum(serialize = "tdk_database_url")]
    DatabaseUrl,
    #[strum(serialize = "tdk_database_path")]
    DatabasePath,
    #[strum(serialize = "tdk_storage_url")]
    StorageUrl,
    #[strum(serialize = "tdk_storage_path")]
    StoragePath,
    #[strum(serialize = "tdk_pyenv_td_root")]
    PyenvTdRoot,
    #[strum(serialize = "tdk_pyenv_ag_root")]
    PyenvAgRoot,
    #[strum(serialize = "tdk_pyenv_scripts_folder")]
    PyenvScriptsFolder,
    #[strum(serialize = "tdk_pyenv_libexec_folder")]
    PyenvLibexecFolder,
    #[strum(serialize = "tdk_instance_name")]
    InstanceName,
    #[strum(serialize = "tdk_apiserver_public_port")]
    ApiserverPublicPort,
    #[strum(serialize = "tdk_apiserver_private_port")]
    ApiserverPrivatePort,
    #[cfg(feature = "enterprise")]
    #[strum(serialize = "tdk_aiagent_public_port")]
    AiagentPublicPort,
    #[strum(serialize = "tdk_database_port")]
    DatabasePort,
    #[strum(serialize = "tdk_kminus_api_port")]
    KminusApiPort,
}

impl SystemKey {
    pub(crate) fn supported() -> String {
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
    pub pyenv_libexec_folder: Option<String>,
    pub instance_name: Option<String>,
    pub apiserver_public_port: Option<String>,
    pub apiserver_private_port: Option<String>,
    #[cfg(feature = "enterprise")]
    pub aiagent_public_port: Option<String>,
    pub database_port: Option<String>,
    pub kminus_api_port: Option<String>,
}

impl SystemValues {
    pub(crate) fn lookup(&self, key: SystemKey) -> Option<&str> {
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
            SystemKey::PyenvLibexecFolder => self.pyenv_libexec_folder.as_deref(),
            SystemKey::InstanceName => self.instance_name.as_deref(),
            SystemKey::ApiserverPublicPort => self.apiserver_public_port.as_deref(),
            SystemKey::ApiserverPrivatePort => self.apiserver_private_port.as_deref(),
            #[cfg(feature = "enterprise")]
            SystemKey::AiagentPublicPort => self.aiagent_public_port.as_deref(),
            SystemKey::DatabasePort => self.database_port.as_deref(),
            SystemKey::KminusApiPort => self.kminus_api_port.as_deref(),
        }
    }
}

pub(crate) fn resolve(leaf: &str, values: &SystemValues) -> Result<String, TdError> {
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

pub(crate) struct SystemResolver {
    auto: bool,
}

impl SystemResolver {
    pub(crate) fn new(auto: bool) -> Self {
        Self { auto }
    }

    fn lookup_value(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let path = context.path.join(PATH_SEPARATOR);
        let category = context.placeholder.category.as_deref().ok_or_else(|| {
            ResolverError::MissingPlaceholderCategory {
                path: path.clone(),
                kind: self.kind().as_str().to_owned(),
            }
        })?;
        let key =
            SystemKey::from_str(category).map_err(|_| ResolverError::UnsupportedSystemKey {
                key: category.to_owned(),
                supported: SystemKey::supported(),
            })?;
        let value =
            context
                .system
                .lookup(key)
                .ok_or_else(|| ResolverError::MissingSystemValue {
                    key: category.to_owned(),
                })?;
        Ok(Resolution {
            rendered: value.to_owned(),
            secret: None,
        })
    }
}

impl ConcreteResolver for SystemResolver {
    fn kind(&self) -> PlaceholderKind {
        if self.auto {
            PlaceholderKind::SystemAuto
        } else {
            PlaceholderKind::System
        }
    }

    fn resolve(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        self.lookup_value(context)
    }

    fn materialize(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        self.lookup_value(context)
    }
}
