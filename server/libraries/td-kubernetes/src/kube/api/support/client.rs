//
// Copyright 2026 Tabsdata Inc.
//

use std::env;

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};

use crate::base::codec::decode;
use crate::base::error::kubernetes::{KubeClientError, KubernetesError};
use crate::kube::platform::{auth_failure, credential_command};

pub const POD_KUBECONFIG_ENV: &str = "TDK_KUBECONFIG";

pub fn build_kube_client(config: &Config) -> Result<Client, KubernetesError> {
    Client::try_from(config.clone()).map_err(|error| client_error(&error, config).into())
}

pub async fn build_incluster_kube_client() -> Result<Client, KubernetesError> {
    let config = Config::incluster_env()
        .map_err(|error| KubeClientError::InclusterConfigFailed(error.to_string()))?;
    Client::try_from(config.clone()).map_err(|error| client_error(&error, &config).into())
}

fn client_error(error: &kube::Error, config: &Config) -> KubeClientError {
    match auth_failure(error, credential_command(config).as_deref()) {
        Some(detail) => KubeClientError::AuthFailed(detail),
        None => KubeClientError::BuildFailed(error.to_string()),
    }
}

pub async fn build_pod_kube_client() -> Result<Client, KubernetesError> {
    match env::var(POD_KUBECONFIG_ENV) {
        Ok(value) if !value.is_empty() => {
            let bytes =
                decode(&value).map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let yaml = String::from_utf8(bytes)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let kubeconfig = Kubeconfig::from_yaml(&yaml)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let config = Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default())
                .await
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            Client::try_from(config.clone()).map_err(|error| client_error(&error, &config).into())
        }
        _ => build_incluster_kube_client().await,
    }
}
