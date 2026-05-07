//
// Copyright 2026 Tabs Data Inc.
//

use std::path::{Path, PathBuf};
use std::process::exit;

use kube::config::{Config, KubeConfigOptions, Kubeconfig, KubeconfigError};
use td_common::status::ExitStatus::GeneralError;
use tracing::error;

use crate::constants::{ENV_KUBECONFIG, FILE_KUBECONFIG, FOLDER_KUBE};

pub fn resolve_kubeconfig_path(kubeconfig_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = kubeconfig_path {
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

fn read_kubeconfig(kubeconfig_path: Option<&Path>) -> Result<Kubeconfig, KubeconfigError> {
    match kubeconfig_path {
        Some(path) => Kubeconfig::read_from(path),
        None => Kubeconfig::read(),
    }
}

pub fn resolve_current_context(kubeconfig_path: Option<&Path>) -> Option<String> {
    read_kubeconfig(kubeconfig_path).ok()?.current_context
}

pub fn resolve_cluster_name(
    kubeconfig_path: Option<&Path>,
    context: Option<&str>,
) -> Option<String> {
    let kubeconfig = read_kubeconfig(kubeconfig_path).ok()?;
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

pub async fn build_kubeconfig(
    kubeconfig_path: Option<&Path>,
    context: Option<&str>,
    namespace_override: Option<&str>,
) -> Config {
    let kubeconfig = read_kubeconfig(kubeconfig_path).unwrap_or_else(|error| {
        match kubeconfig_path {
            Some(path) => error!("Failed to read kubeconfig at {}: {error}", path.display()),
            None => error!("Failed to read kubeconfig: {error}"),
        }
        exit(GeneralError.code());
    });
    let options = KubeConfigOptions {
        context: context.map(str::to_owned),
        cluster: None,
        user: None,
    };
    let mut config = Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .unwrap_or_else(|error| {
            error!("Failed to load Kubernetes configuration: {error}");
            exit(GeneralError.code());
        });
    if let Some(namespace) = namespace_override {
        config.default_namespace = namespace.to_owned();
    }
    config
}
