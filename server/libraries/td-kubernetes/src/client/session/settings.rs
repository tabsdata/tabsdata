//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use td_common::env::TABSDATA_HOME_DIR;
use td_common::status::ExitStatus::GeneralError;
use tracing::info;

use crate::cerror;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::core::system::exit::{CommandResult, abort};
use crate::runtime::provider::ClusterProvider;

const SERVER_SESSION_FILE: &str = ".tdserver-session.json";

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KminusInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DockerInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "core::ops::Not::not")]
    pub current: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EksInputs {}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AksInputs {}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GkeInputs {}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FargateInputs {}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum Session {
    Kminus {
        path: PathBuf,
        kubeconfig: PathBuf,
        context: String,
        cluster: String,
        namespace: String,
        #[serde(default)]
        inputs: KminusInputs,
    },
    Docker {
        kubeconfig: PathBuf,
        context: String,
        cluster: String,
        namespace: String,
        #[serde(default)]
        inputs: DockerInputs,
    },
    Eks {
        #[serde(default)]
        inputs: EksInputs,
    },
    Aks {
        #[serde(default)]
        inputs: AksInputs,
    },
    Gke {
        #[serde(default)]
        inputs: GkeInputs,
    },
    Fargate {
        #[serde(default)]
        inputs: FargateInputs,
    },
}

#[derive(Debug, Clone)]
pub struct Resolved<T> {
    pub value: T,
    pub in_settings: bool,
}

pub fn resolve<T>(user: Option<T>, in_settings: Option<T>) -> Resolved<Option<T>> {
    match user {
        Some(value) => Resolved {
            value: Some(value),
            in_settings: false,
        },
        None => match in_settings {
            Some(value) => Resolved {
                value: Some(value),
                in_settings: true,
            },
            None => Resolved {
                value: None,
                in_settings: false,
            },
        },
    }
}

pub fn resolve_flag(
    user: Option<bool>,
    in_settings: Option<bool>,
    fallback: bool,
) -> Resolved<bool> {
    match user {
        Some(value) => Resolved {
            value,
            in_settings: false,
        },
        None => match in_settings {
            Some(value) => Resolved {
                value,
                in_settings: true,
            },
            None => Resolved {
                value: fallback,
                in_settings: false,
            },
        },
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ClusterProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub save: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoconfirm: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SettingsContext {
    #[serde(default, skip_serializing_if = "Settings::is_empty")]
    pub settings: Settings,
}

#[derive(Debug, Clone, Default)]
pub struct SettingsSelection {
    pub instance: bool,
    pub provider: bool,
    pub kubeconfig: bool,
    pub context: bool,
    pub namespace: bool,
    pub save: bool,
    pub details: bool,
    pub autoconfirm: bool,
}

impl SettingsSelection {
    pub fn any(&self) -> bool {
        self.instance
            || self.provider
            || self.kubeconfig
            || self.context
            || self.namespace
            || self.save
            || self.details
            || self.autoconfirm
    }
}

impl Settings {
    pub fn is_empty(&self) -> bool {
        self.instance.is_none()
            && self.provider.is_none()
            && self.kubeconfig.is_none()
            && self.context.is_none()
            && self.namespace.is_none()
            && self.save.is_none()
            && self.details.is_none()
            && self.autoconfirm.is_none()
    }

    pub fn report(&self, heading: &str) {
        info!("{heading}");
        if let Some(instance) = &self.instance {
            info!("  - instance.....: {instance}");
        }
        if let Some(provider) = &self.provider {
            info!("  - provider.....: {}", provider.as_str());
        }
        if let Some(kubeconfig) = &self.kubeconfig {
            info!("  - kubeconfig...: {}", kubeconfig.display());
        }
        if let Some(context) = &self.context {
            info!("  - context......: {context}");
        }
        if let Some(namespace) = &self.namespace {
            info!("  - namespace....: {namespace}");
        }
        if let Some(save) = self.save {
            info!("  - save.........: {save}");
        }
        if let Some(details) = self.details {
            info!("  - details......: {details}");
        }
        if let Some(autoconfirm) = self.autoconfirm {
            info!("  - autoconfirm..: {autoconfirm}");
        }
    }

    fn merge(&mut self, values: Settings) {
        if values.instance.is_some() {
            self.instance = values.instance;
        }
        if values.provider.is_some() {
            self.provider = values.provider;
        }
        if values.kubeconfig.is_some() {
            self.kubeconfig = values.kubeconfig;
        }
        if values.context.is_some() {
            self.context = values.context;
        }
        if values.namespace.is_some() {
            self.namespace = values.namespace;
        }
        if values.save.is_some() {
            self.save = values.save;
        }
        if values.details.is_some() {
            self.details = values.details;
        }
        if values.autoconfirm.is_some() {
            self.autoconfirm = values.autoconfirm;
        }
    }

    fn clear(&mut self, selection: &SettingsSelection) -> bool {
        let mut cleared = false;
        if selection.instance && self.instance.take().is_some() {
            cleared = true;
        }
        if selection.provider && self.provider.take().is_some() {
            cleared = true;
        }
        if selection.kubeconfig && self.kubeconfig.take().is_some() {
            cleared = true;
        }
        if selection.context && self.context.take().is_some() {
            cleared = true;
        }
        if selection.namespace && self.namespace.take().is_some() {
            cleared = true;
        }
        if selection.save && self.save.take().is_some() {
            cleared = true;
        }
        if selection.details && self.details.take().is_some() {
            cleared = true;
        }
        if selection.autoconfirm && self.autoconfirm.take().is_some() {
            cleared = true;
        }
        cleared
    }
}

pub fn tdksettings_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(TABSDATA_HOME_DIR)
        .join(SERVER_SESSION_FILE)
}

impl SettingsContext {
    pub fn load() -> CommandResult<Self> {
        let path = tdksettings_path();
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        match serde_json::from_str(&data) {
            Ok(settings_context) => Ok(settings_context),
            Err(error) => {
                cerror!(
                    "Failed to parse '{}': {error}. Run 'tdkserver unset' to clear the tabsdata session settings.",
                    path.display()
                );
                abort(GeneralError)
            }
        }
    }

    pub fn read() -> Self {
        let path = tdksettings_path();
        fs::read_to_string(&path)
            .ok()
            .and_then(|data| serde_json::from_str(&data).ok())
            .unwrap_or_default()
    }

    fn save(&self) -> CommandResult {
        let path = tdksettings_path();
        if let Some(parent) = path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            cerror!("Failed to create '{}': {error}", parent.display());
            return abort(GeneralError);
        }
        let data = match serde_json::to_string_pretty(self) {
            Ok(data) => data,
            Err(error) => {
                cerror!("Failed to serialize the tabsdata session settings: {error}");
                return abort(GeneralError);
            }
        };
        if let Err(error) = fs::write(&path, data) {
            cerror!("Failed to write '{}': {error}", path.display());
            return abort(GeneralError);
        }
        Ok(())
    }

    fn delete_if_empty(&self) -> CommandResult {
        if self.settings.is_empty() {
            let path = tdksettings_path();
            if path.exists()
                && let Err(error) = fs::remove_file(&path)
            {
                cerror!("Failed to delete '{}': {error}", path.display());
                return abort(GeneralError);
            }
            return Ok(());
        }
        self.save()
    }

    pub fn set_values(values: Settings) -> CommandResult {
        let mut set = Self::load()?;
        set.settings.merge(values);
        set.save()
    }

    pub fn unset_values(selection: &SettingsSelection) -> CommandResult<bool> {
        let mut set = Self::load()?;
        let cleared = set.settings.clear(selection);
        set.delete_if_empty()?;
        Ok(cleared)
    }

    pub fn unset_all() -> CommandResult<bool> {
        let mut set = Self::load()?;
        let existed = !set.settings.is_empty();
        set.settings = Settings::default();
        set.delete_if_empty()?;
        Ok(existed)
    }

    pub fn settings() -> Settings {
        Self::read().settings
    }

    pub fn set_instance(name: String) -> CommandResult {
        let mut set = Self::load()?;
        set.settings.instance = Some(name);
        set.save()
    }

    pub fn settings_instance() -> Option<String> {
        Self::read().settings.instance
    }
}

impl Session {
    pub fn provider(&self) -> ClusterProvider {
        match self {
            Session::Kminus { .. } => ClusterProvider::Kminus,
            Session::Docker { .. } => ClusterProvider::Docker,
            Session::Eks { .. } => ClusterProvider::Eks,
            Session::Aks { .. } => ClusterProvider::Aks,
            Session::Gke { .. } => ClusterProvider::Gke,
            Session::Fargate { .. } => ClusterProvider::Fargate,
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            Session::Kminus { path, .. } => {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                format!("{}:{name}", ClusterProvider::Kminus.as_str())
            }
            Session::Docker {
                context, namespace, ..
            } => format!("{}:{context}/{namespace}", ClusterProvider::Docker.as_str()),
            Session::Eks { .. } => ClusterProvider::Eks.as_str().to_string(),
            Session::Aks { .. } => ClusterProvider::Aks.as_str().to_string(),
            Session::Gke { .. } => ClusterProvider::Gke.as_str().to_string(),
            Session::Fargate { .. } => ClusterProvider::Fargate.as_str().to_string(),
        }
    }

    pub fn report_active(&self) {
        info!("Active tabsdata session:");
        match self {
            Session::Kminus {
                path,
                kubeconfig,
                context,
                cluster,
                namespace,
                ..
            } => {
                let name = path
                    .file_name()
                    .and_then(|component| component.to_str())
                    .unwrap_or("?");
                info!("  - provider.....: {}", ClusterProvider::Kminus.as_str());
                info!("  - instance name: {name}");
                info!("  - instance path: {}", path.display());
                info!("  - kubeconfig...: {}", kubeconfig.display());
                info!("  - context......: {context}");
                info!("  - cluster......: {cluster}");
                info!("  - namespace....: {namespace}");
            }
            Session::Docker {
                kubeconfig,
                context,
                cluster,
                namespace,
                ..
            } => {
                info!("  - provider.....: {}", ClusterProvider::Docker.as_str());
                info!("  - kubeconfig...: {}", kubeconfig.display());
                info!("  - context......: {context}");
                info!("  - cluster......: {cluster}");
                info!("  - namespace....: {namespace}");
            }
            Session::Eks { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Eks.as_str());
            }
            Session::Aks { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Aks.as_str());
            }
            Session::Gke { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Gke.as_str());
            }
            Session::Fargate { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Fargate.as_str());
            }
        }
    }

    pub fn to_args(&self) -> (ClusterArguments, InstanceArguments) {
        match self {
            Session::Kminus { path, .. } => {
                let name = path
                    .file_name()
                    .and_then(|component| component.to_str())
                    .map(str::to_owned);
                (
                    ClusterArguments::new(Some(ClusterProvider::Kminus), None, None, false, None),
                    InstanceArguments::new(name),
                )
            }
            Session::Docker {
                kubeconfig,
                context,
                namespace,
                ..
            } => (
                ClusterArguments::new(
                    Some(ClusterProvider::Docker),
                    Some(context.clone()),
                    Some(kubeconfig.clone()),
                    false,
                    Some(namespace.clone()),
                ),
                InstanceArguments::new(None),
            ),
            Session::Eks { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Eks), None, None, false, None),
                InstanceArguments::new(None),
            ),
            Session::Aks { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Aks), None, None, false, None),
                InstanceArguments::new(None),
            ),
            Session::Gke { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Gke), None, None, false, None),
                InstanceArguments::new(None),
            ),
            Session::Fargate { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Fargate), None, None, false, None),
                InstanceArguments::new(None),
            ),
        }
    }
}

#[cfg(test)]
mod tests;
