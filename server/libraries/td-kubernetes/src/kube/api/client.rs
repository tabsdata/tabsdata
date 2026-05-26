//
// Copyright 2026 Tabs Data Inc.
//

use std::env;

use kube::Client;
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use rustls::crypto::{aws_lc_rs, ring};
use tracing::debug;

use crate::base::codec::decode;
use crate::base::error::kubernetes::{KubeClientError, KubernetesError};

pub const POD_KUBECONFIG_ENV: &str = "TDK_KUBECONFIG";

pub fn install_crypto_provider() {
    if let Err(error) = aws_lc_rs::default_provider().install_default() {
        debug!(
            "Failed to install the aws-lc-rs tls cryptographic provider: {error:?}. Falling back to the ring tls cryptographic provider."
        );
        if let Err(error) = ring::default_provider().install_default() {
            debug!("Failed to install the ring tls cryptographic provider: {error:?}.");
        } else {
            debug!("Successfully installed the ring tls cryptographic provider.");
        }
    } else {
        debug!("Successfully installed the aws-lc-rs tls cryptographic provider.");
    }
}

pub fn build_kube_client(config: &Config) -> Result<Client, KubernetesError> {
    Client::try_from(config.clone())
        .map_err(|error| KubeClientError::BuildFailed(error.to_string()).into())
}

pub async fn build_incluster_kube_client() -> Result<Client, KubernetesError> {
    let config = Config::incluster_env()
        .map_err(|error| KubeClientError::InclusterConfigFailed(error.to_string()))?;
    Client::try_from(config)
        .map_err(|error| KubeClientError::BuildFailed(error.to_string()).into())
}

pub async fn build_pod_kube_client() -> Result<Client, KubernetesError> {
    match env::var(POD_KUBECONFIG_ENV) {
        Ok(value) if !value.is_empty() => {
            let bytes = decode(&value)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let yaml = String::from_utf8(bytes)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let kubeconfig = Kubeconfig::from_yaml(&yaml)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            let config =
                Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default())
                    .await
                    .map_err(|error| KubeClientError::BuildFailed(error.to_string()))?;
            Client::try_from(config)
                .map_err(|error| KubeClientError::BuildFailed(error.to_string()).into())
        }
        _ => build_incluster_kube_client().await,
    }
}
