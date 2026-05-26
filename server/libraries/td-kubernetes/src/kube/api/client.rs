//
// Copyright 2026 Tabs Data Inc.
//

use kube::Client;
use kube::config::Config;
use rustls::crypto::{aws_lc_rs, ring};
use tracing::debug;

use crate::base::error::kubernetes::{KubeClientError, KubernetesError};

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
