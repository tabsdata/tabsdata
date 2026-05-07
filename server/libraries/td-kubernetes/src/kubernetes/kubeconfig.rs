//
// Copyright 2026 Tabs Data Inc.
//

use std::path::{Path, PathBuf};

use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::constants::{ENV_KUBECONFIG, FILE_KUBECONFIG, FOLDER_KUBE};
use crate::system::exit::{Result, abort};

pub fn resolve_kubefile(kubefile: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = kubefile {
        return Some(path.to_path_buf());
    }
    if let Ok(value) = std::env::var(ENV_KUBECONFIG) {
        let separator = if cfg!(windows) { ';' } else { ':' };
        if let Some(first) = value.split(separator).find(|entry| !entry.is_empty()) {
            return Some(PathBuf::from(first));
        }
    }
    let default = dirs::home_dir()?.join(FOLDER_KUBE).join(FILE_KUBECONFIG);
    if default.exists() {
        Some(default)
    } else {
        None
    }
}

pub fn resolve_current_context(kubefile: Option<&Path>) -> Option<String> {
    let kubeconfig = match kubefile {
        Some(path) => Kubeconfig::read_from(path).ok()?,
        None => Kubeconfig::read().ok()?,
    };
    kubeconfig.current_context
}

pub fn resolve_cluster_name(kubefile: Option<&Path>, context: Option<&str>) -> Option<String> {
    let kubeconfig = match kubefile {
        Some(path) => Kubeconfig::read_from(path).ok()?,
        None => Kubeconfig::read().ok()?,
    };
    let context_name = context
        .map(str::to_owned)
        .or_else(|| kubeconfig.current_context.clone())?;
    kubeconfig
        .contexts
        .iter()
        .find(|named| named.name == context_name)
        .and_then(|named| named.context.as_ref())
        .map(|context| context.cluster.clone())
}

pub fn resolve_namespace(kubefile: Option<&Path>, context: Option<&str>) -> Option<String> {
    let kubeconfig = match kubefile {
        Some(path) => Kubeconfig::read_from(path).ok()?,
        None => Kubeconfig::read().ok()?,
    };
    let context_name = context
        .map(str::to_owned)
        .or_else(|| kubeconfig.current_context.clone())?;
    kubeconfig
        .contexts
        .iter()
        .find(|named| named.name == context_name)
        .and_then(|named| named.context.as_ref())
        .and_then(|context| context.namespace.clone())
}

pub fn resolve_cluster_url(kubefile: Option<&Path>, context: Option<&str>) -> Option<String> {
    let kubeconfig = match kubefile {
        Some(path) => Kubeconfig::read_from(path).ok()?,
        None => Kubeconfig::read().ok()?,
    };
    let context_name = context
        .map(str::to_owned)
        .or_else(|| kubeconfig.current_context.clone())?;
    let cluster_name = kubeconfig
        .contexts
        .iter()
        .find(|named| named.name == context_name)
        .and_then(|named| named.context.as_ref())
        .map(|context| context.cluster.clone())?;
    kubeconfig
        .clusters
        .iter()
        .find(|named| named.name == cluster_name)
        .and_then(|named| named.cluster.as_ref())
        .and_then(|cluster| cluster.server.clone())
}

pub async fn build_kubeconfig(
    kubefile: Option<&Path>,
    context: Option<&str>,
    namespace_override: Option<&str>,
) -> Result<Config> {
    let kubeconfig = match kubefile {
        Some(path) => match Kubeconfig::read_from(path) {
            Ok(kubeconfig) => kubeconfig,
            Err(error) => {
                cerror!("Failed to read kubeconfig at {}: {error}", path.display());
                return abort(GeneralError);
            }
        },
        None => match Kubeconfig::read() {
            Ok(kubeconfig) => kubeconfig,
            Err(error) => {
                cerror!("Failed to read kubeconfig: {error}");
                return abort(GeneralError);
            }
        },
    };
    let options = KubeConfigOptions {
        context: context.map(str::to_owned),
        cluster: None,
        user: None,
    };
    let mut config = match Config::from_custom_kubeconfig(kubeconfig, &options).await {
        Ok(config) => config,
        Err(error) => {
            cerror!("Failed to load kubernetes configuration: {error}");
            return abort(GeneralError);
        }
    };
    if let Some(namespace) = namespace_override {
        config.default_namespace = namespace.to_owned();
    }
    Ok(config)
}
