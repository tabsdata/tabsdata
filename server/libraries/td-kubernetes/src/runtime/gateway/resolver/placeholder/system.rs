//
// Copyright 2026 Tabsdata Inc.
//

//! System values: the [`SystemKey`] catalog, the caller-supplied [`SystemValues`],
//! and the [`SystemResolver`] that resolves a `${{system:KEY}}` placeholder against
//! them.

use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use strum::{AsRefStr, EnumIter, EnumString, IntoEnumIterator};
use td_error::TdError;

use crate::base::constants::IndexReach;
use crate::base::constants::UvMode;
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
    #[strum(serialize = "tdk_database_path")]
    DatabasePath,
    #[strum(serialize = "tdk_storage_url")]
    StorageUrl,
    #[strum(serialize = "tdk_storage_path")]
    StoragePath,
    #[strum(serialize = "tdk_runtimes_root")]
    RuntimesRoot,
    #[strum(serialize = "tdk_pyenv_base_root")]
    PyenvBaseRoot,
    #[strum(serialize = "tdk_pyenv_td_root")]
    PyenvTdRoot,
    #[strum(serialize = "tdk_pyenv_ag_root")]
    PyenvAgRoot,
    #[strum(serialize = "tdk_pyenv_scripts_folder")]
    PyenvScriptsFolder,
    #[strum(serialize = "tdk_pyenv_libexec_folder")]
    PyenvLibexecFolder,
    #[strum(serialize = "tdk_exe_suffix")]
    ExeSuffix,
    #[strum(serialize = "tdk_instance_name")]
    InstanceName,
    #[strum(serialize = "tdk_namespace_name")]
    NamespaceName,
    #[strum(serialize = "tdk_cluster_name")]
    ClusterName,
    #[strum(serialize = "tdk_apiserver_public_port")]
    ApiserverPublicPort,
    #[strum(serialize = "tdk_apiserver_private_port")]
    ApiserverPrivatePort,
    #[cfg(feature = "enterprise")]
    #[strum(serialize = "tdk_aiagent_public_port")]
    AiagentPublicPort,
    #[strum(serialize = "tdk_database_host")]
    DatabaseHost,
    #[strum(serialize = "tdk_database_port")]
    DatabasePort,
    #[strum(serialize = "tdk_database_name")]
    DatabaseName,
    #[strum(serialize = "tdk_database_ssl_mode")]
    DatabaseSslMode,
    #[strum(serialize = "tdk_database_ssl_root_cert")]
    DatabaseSslRootCert,
    #[strum(serialize = "tdk_kminus_api_port")]
    KminusApiPort,
    #[strum(serialize = "tdk_connection_configmap_name")]
    ConnectionConfigmapName,
    #[strum(serialize = "tdk_connection_secret_name")]
    ConnectionSecretName,
    #[strum(serialize = "tdk_cron_schedule")]
    CronSchedule,
    #[strum(serialize = "tdk_uv_default_index")]
    UvDefaultIndex,
    #[strum(serialize = "tdk_uv_extra_index_url")]
    UvExtraIndexUrl,
    #[strum(serialize = "tdk_uv_index_strategy")]
    UvIndexStrategy,
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
pub struct WorkerSettings {
    pub cron_schedule: Option<String>,
    pub connection_configmap_name: Option<String>,
    pub connection_secret_name: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SystemValues {
    pub repository_url: Option<String>,
    pub repository_path: Option<String>,
    pub database_path: Option<String>,
    pub storage_url: Option<String>,
    pub storage_path: Option<String>,
    pub runtimes_root: Option<String>,
    pub pyenv_base_root: Option<String>,
    pub pyenv_td_root: Option<String>,
    pub pyenv_ag_root: Option<String>,
    pub pyenv_scripts_folder: Option<String>,
    pub pyenv_libexec_folder: Option<String>,
    pub exe_suffix: Option<String>,
    pub instance_name: Option<String>,
    pub namespace_name: Option<String>,
    pub cluster_name: Option<String>,
    pub apiserver_public_port: Option<String>,
    pub apiserver_private_port: Option<String>,
    #[cfg(feature = "enterprise")]
    pub aiagent_public_port: Option<String>,
    pub database_host: Option<String>,
    pub database_port: Option<String>,
    pub database_name: Option<String>,
    pub database_ssl_mode: Option<String>,
    pub database_ssl_root_cert: Option<String>,
    pub kminus_api_port: Option<String>,
    pub connection_configmap_name: Option<String>,
    pub connection_secret_name: Option<String>,
    pub cron_schedule: Option<String>,
    pub uv_default_index: Option<String>,
    pub uv_extra_index_url: Option<String>,
    pub uv_index_strategy: Option<String>,
}

impl SystemValues {
    pub(crate) fn lookup(&self, key: SystemKey) -> Option<&str> {
        match key {
            SystemKey::RepositoryUrl => self.repository_url.as_deref(),
            SystemKey::RepositoryPath => self.repository_path.as_deref(),
            SystemKey::DatabasePath => self.database_path.as_deref(),
            SystemKey::StorageUrl => self.storage_url.as_deref(),
            SystemKey::StoragePath => self.storage_path.as_deref(),
            SystemKey::RuntimesRoot => self.runtimes_root.as_deref(),
            SystemKey::PyenvBaseRoot => self.pyenv_base_root.as_deref(),
            SystemKey::PyenvTdRoot => self.pyenv_td_root.as_deref(),
            SystemKey::PyenvAgRoot => self.pyenv_ag_root.as_deref(),
            SystemKey::PyenvScriptsFolder => self.pyenv_scripts_folder.as_deref(),
            SystemKey::PyenvLibexecFolder => self.pyenv_libexec_folder.as_deref(),
            SystemKey::ExeSuffix => self.exe_suffix.as_deref(),
            SystemKey::InstanceName => self.instance_name.as_deref(),
            SystemKey::NamespaceName => self.namespace_name.as_deref(),
            SystemKey::ClusterName => self.cluster_name.as_deref(),
            SystemKey::ApiserverPublicPort => self.apiserver_public_port.as_deref(),
            SystemKey::ApiserverPrivatePort => self.apiserver_private_port.as_deref(),
            #[cfg(feature = "enterprise")]
            SystemKey::AiagentPublicPort => self.aiagent_public_port.as_deref(),
            SystemKey::DatabaseHost => self.database_host.as_deref(),
            SystemKey::DatabasePort => self.database_port.as_deref(),
            SystemKey::DatabaseName => self.database_name.as_deref(),
            SystemKey::DatabaseSslMode => self.database_ssl_mode.as_deref(),
            SystemKey::DatabaseSslRootCert => self.database_ssl_root_cert.as_deref(),
            SystemKey::KminusApiPort => self.kminus_api_port.as_deref(),
            SystemKey::ConnectionConfigmapName => self.connection_configmap_name.as_deref(),
            SystemKey::ConnectionSecretName => self.connection_secret_name.as_deref(),
            SystemKey::CronSchedule => self.cron_schedule.as_deref(),
            SystemKey::UvDefaultIndex => self.uv_default_index.as_deref(),
            SystemKey::UvExtraIndexUrl => self.uv_extra_index_url.as_deref(),
            SystemKey::UvIndexStrategy => self.uv_index_strategy.as_deref(),
        }
    }

    pub fn uv_default_index() -> Result<String, TdError> {
        Ok(uv_mode()?.default_index())
    }

    pub fn uv_extra_index_url(reach: IndexReach) -> Result<String, TdError> {
        Ok(uv_mode()?.extra_index_url(reach))
    }

    pub fn uv_index_strategy() -> Result<String, TdError> {
        Ok(uv_mode()?.index_strategy())
    }

    pub fn apply_worker_settings(&mut self, settings: &WorkerSettings) {
        self.cron_schedule = settings.cron_schedule.clone();
        self.connection_configmap_name = settings.connection_configmap_name.clone();
        self.connection_secret_name = settings.connection_secret_name.clone();
    }
}

fn uv_mode() -> Result<UvMode, TdError> {
    UvMode::from_environment().map_err(TdError::from)
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
