//
// Copyright 2026 Tabs Data Inc.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
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
    Fargate {
        #[serde(default)]
        inputs: FargateInputs,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Autoconfirm {
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstancePin {
    pub name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pins {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstancePin>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinContext {
    #[serde(default, skip_serializing_if = "Pins::is_empty")]
    pub pins: Pins,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoconfirm: Option<Autoconfirm>,
}

impl Pins {
    fn is_empty(&self) -> bool {
        self.instance.is_none()
    }
}

pub fn tdkpin_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(TABSDATA_HOME_DIR)
        .join(SERVER_SESSION_FILE)
}

impl PinContext {
    pub fn load() -> CommandResult<Self> {
        let path = tdkpin_path();
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
            Ok(pin_context) => Ok(pin_context),
            Err(error) => {
                cerror!(
                    "Failed to parse '{}': {error}. Run 'tdkserver unpin' to clear the pinned tabsdata instance.",
                    path.display()
                );
                abort(GeneralError)
            }
        }
    }

    pub fn read() -> Self {
        let path = tdkpin_path();
        fs::read_to_string(&path)
            .ok()
            .and_then(|data| serde_json::from_str(&data).ok())
            .unwrap_or_default()
    }

    fn save(&self) -> CommandResult {
        let path = tdkpin_path();
        if let Some(parent) = path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            cerror!("Failed to create '{}': {error}", parent.display());
            return abort(GeneralError);
        }
        let data = match serde_json::to_string_pretty(self) {
            Ok(data) => data,
            Err(error) => {
                cerror!("Failed to serialize the tabsdata pin: {error}");
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
        if self.pins.is_empty() && self.autoconfirm.is_none() {
            let path = tdkpin_path();
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

    pub fn pin_instance(name: String) -> CommandResult {
        let mut pin = Self::load()?;
        pin.pins.instance = Some(InstancePin { name });
        pin.save()
    }

    pub fn unpin_instance() -> CommandResult<bool> {
        let mut pin = Self::load()?;
        let existed = pin.pins.instance.is_some();
        pin.pins.instance = None;
        pin.delete_if_empty()?;
        Ok(existed)
    }

    pub fn pinned_instance() -> Option<String> {
        Self::read().pins.instance.map(|instance| instance.name)
    }

    pub fn pin_autoconfirm(enabled: bool) -> CommandResult {
        let mut pin = Self::load()?;
        pin.autoconfirm = Some(Autoconfirm { enabled });
        pin.save()
    }

    pub fn unpin_autoconfirm() -> CommandResult<bool> {
        let mut pin = Self::load()?;
        let existed = pin.autoconfirm.is_some();
        pin.autoconfirm = None;
        pin.delete_if_empty()?;
        Ok(existed)
    }

    pub fn autoconfirm_enabled() -> bool {
        Self::read()
            .autoconfirm
            .map(|autoconfirm| autoconfirm.enabled)
            .unwrap_or(false)
    }
}

impl Session {
    pub fn provider(&self) -> ClusterProvider {
        match self {
            Session::Kminus { .. } => ClusterProvider::Kminus,
            Session::Docker { .. } => ClusterProvider::Docker,
            Session::Eks { .. } => ClusterProvider::Eks,
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
            Session::Fargate { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Fargate.as_str());
            }
        }
    }

    pub fn to_args(&self) -> (ClusterArguments, InstanceArguments) {
        match self {
            Session::Kminus { path, .. } => (
                ClusterArguments::new(Some(ClusterProvider::Kminus), None, None, false, None),
                InstanceArguments::new(None, Some(path.clone())),
            ),
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
                InstanceArguments::new(None, None),
            ),
            Session::Eks { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Eks), None, None, false, None),
                InstanceArguments::new(None, None),
            ),
            Session::Fargate { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Fargate), None, None, false, None),
                InstanceArguments::new(None, None),
            ),
        }
    }
}

#[cfg(test)]
mod tests;
