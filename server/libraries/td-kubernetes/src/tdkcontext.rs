//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use td_common::env::TABSDATA_HOME_DIR;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::exit::{Outcome, abort};
use crate::fabric::ClusterFabric;

const TDKCONTEXT_FILE: &str = "tdkcontext.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "fabric", rename_all = "lowercase")]
pub enum TdkContext {
    Kubernetes {
        #[serde(skip_serializing_if = "Option::is_none")]
        kubeconfig: Option<PathBuf>,
        #[serde(skip_serializing_if = "Option::is_none")]
        context: Option<String>,
        namespace: String,
    },
    Kminus {
        path: PathBuf,
    },
}

pub fn tdkcontext_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(TABSDATA_HOME_DIR)
        .join(TDKCONTEXT_FILE)
}

impl TdkContext {
    pub fn load() -> Outcome<Option<Self>> {
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

    pub fn save(&self) -> Outcome {
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

    pub fn delete() -> Outcome<bool> {
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

    pub fn fabric(&self) -> ClusterFabric {
        match self {
            TdkContext::Kubernetes { .. } => ClusterFabric::Kubernetes,
            TdkContext::Kminus { .. } => ClusterFabric::Kminus,
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            TdkContext::Kubernetes {
                context, namespace, ..
            } => {
                let context = context.as_deref().unwrap_or("?");
                format!("kubernetes:{context}/{namespace}")
            }
            TdkContext::Kminus { path } => {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                format!("kminus:{name}")
            }
        }
    }

    pub fn to_args(&self) -> (ClusterArguments, InstanceArguments) {
        match self {
            TdkContext::Kubernetes {
                kubeconfig,
                context,
                namespace,
            } => (
                ClusterArguments::new(
                    Some(ClusterFabric::Kubernetes),
                    context.clone(),
                    kubeconfig.clone(),
                    false,
                    Some(namespace.clone()),
                ),
                InstanceArguments::new(None, None),
            ),
            TdkContext::Kminus { path } => (
                ClusterArguments::new(Some(ClusterFabric::Kminus), None, None, false, None),
                InstanceArguments::new(None, Some(path.clone())),
            ),
        }
    }
}
