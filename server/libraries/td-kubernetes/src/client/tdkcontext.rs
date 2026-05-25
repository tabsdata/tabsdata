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
use crate::provider::ClusterProvider;
use crate::system::exit::{Result, abort};

const TDKCONTEXT_FILE: &str = "tdkcontext.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KubernetesInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "core::ops::Not::not")]
    pub current: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KminusInputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EksInputs {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FargateInputs {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum TdkContext {
    Kubernetes {
        kubeconfig: PathBuf,
        context: String,
        cluster: String,
        namespace: String,
        #[serde(default)]
        inputs: KubernetesInputs,
    },
    Kminus {
        path: PathBuf,
        kubeconfig: PathBuf,
        context: String,
        cluster: String,
        namespace: String,
        #[serde(default)]
        inputs: KminusInputs,
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

pub fn tdkcontext_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(TABSDATA_HOME_DIR)
        .join(TDKCONTEXT_FILE)
}

impl TdkContext {
    pub fn load() -> Result<Option<Self>> {
        let path = tdkcontext_path();
        if !path.exists() {
            return Ok(None);
        }
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        match serde_json::from_str(&data) {
            Ok(tdkcontext) => Ok(Some(tdkcontext)),
            Err(error) => {
                cerror!(
                    "Failed to parse '{}': {error}. Run 'tdkserver unuse' to clear the active context.",
                    path.display()
                );
                abort(GeneralError)
            }
        }
    }

    pub fn read() -> Option<Self> {
        let path = tdkcontext_path();
        let data = fs::read_to_string(&path).ok()?;
        serde_json::from_str(&data).ok()
    }

    pub fn save(&self) -> Result {
        let path = tdkcontext_path();
        if let Some(parent) = path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            cerror!("Failed to create '{}': {error}", parent.display());
            return abort(GeneralError);
        }
        let data = match serde_json::to_string_pretty(self) {
            Ok(data) => data,
            Err(error) => {
                cerror!("Failed to serialize the active context: {error}");
                return abort(GeneralError);
            }
        };
        if let Err(error) = fs::write(&path, data) {
            cerror!("Failed to write '{}': {error}", path.display());
            return abort(GeneralError);
        }
        Ok(())
    }

    pub fn delete() -> Result<bool> {
        let path = tdkcontext_path();
        if !path.exists() {
            return Ok(false);
        }
        if let Err(error) = fs::remove_file(&path) {
            cerror!("Failed to delete '{}': {error}", path.display());
            return abort(GeneralError);
        }
        Ok(true)
    }

    pub fn provider(&self) -> ClusterProvider {
        match self {
            TdkContext::Kubernetes { .. } => ClusterProvider::Kubernetes,
            TdkContext::Kminus { .. } => ClusterProvider::Kminus,
            TdkContext::Eks { .. } => ClusterProvider::Eks,
            TdkContext::Fargate { .. } => ClusterProvider::Fargate,
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            TdkContext::Kubernetes {
                context, namespace, ..
            } => format!(
                "{}:{context}/{namespace}",
                ClusterProvider::Kubernetes.as_str()
            ),
            TdkContext::Kminus { path, .. } => {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                format!("{}:{name}", ClusterProvider::Kminus.as_str())
            }
            TdkContext::Eks { .. } => ClusterProvider::Eks.as_str().to_string(),
            TdkContext::Fargate { .. } => ClusterProvider::Fargate.as_str().to_string(),
        }
    }

    pub fn report_using(&self) {
        info!("Active context:");
        match self {
            TdkContext::Kubernetes {
                kubeconfig,
                context,
                cluster,
                namespace,
                ..
            } => {
                info!("  - provider.....: {}", ClusterProvider::Kubernetes.as_str());
                info!("  - kubeconfig...: {}", kubeconfig.display());
                info!("  - context......: {context}");
                info!("  - cluster......: {cluster}");
                info!("  - namespace....: {namespace}");
            }
            TdkContext::Kminus {
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
            TdkContext::Eks { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Eks.as_str());
            }
            TdkContext::Fargate { .. } => {
                info!("  - provider.....: {}", ClusterProvider::Fargate.as_str());
            }
        }
    }

    pub fn to_args(&self) -> (ClusterArguments, InstanceArguments) {
        match self {
            TdkContext::Kubernetes {
                kubeconfig,
                context,
                namespace,
                ..
            } => (
                ClusterArguments::new(
                    Some(ClusterProvider::Kubernetes),
                    Some(context.clone()),
                    Some(kubeconfig.clone()),
                    false,
                    Some(namespace.clone()),
                ),
                InstanceArguments::new(None, None),
            ),
            TdkContext::Kminus { path, .. } => (
                ClusterArguments::new(Some(ClusterProvider::Kminus), None, None, false, None),
                InstanceArguments::new(None, Some(path.clone())),
            ),
            TdkContext::Eks { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Eks), None, None, false, None),
                InstanceArguments::new(None, None),
            ),
            TdkContext::Fargate { .. } => (
                ClusterArguments::new(Some(ClusterProvider::Fargate), None, None, false, None),
                InstanceArguments::new(None, None),
            ),
        }
    }
}

#[cfg(test)]
mod tests;
