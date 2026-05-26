//
// Copyright 2026 Tabs Data Inc.
//

//! Runtime-layer wrappers over [`crate::kube::api`] that surface typed
//! Kubernetes errors to the operator via `cerror!` before propagating.
//!
//! The api/ layer stays pure (no CLI coupling). Callers in the tdkserver
//! CLI flow use these wrappers so that errors are visible inline with
//! the rest of the CLI output rather than appearing only as a bare
//! "Command aborted." footer at process exit.

use kube::Client;
use kube::config::Config;

use crate::base::error::kubernetes::KubernetesError;
use crate::cerror;
use crate::kube::api;

pub fn build_kube_client(config: &Config) -> Result<Client, KubernetesError> {
    api::client::build_kube_client(config).inspect_err(|error| {
        cerror!("Failed to build kubernetes client: {error}");
    })
}

pub async fn build_incluster_kube_client() -> Result<Client, KubernetesError> {
    api::client::build_incluster_kube_client()
        .await
        .inspect_err(|error| {
            cerror!("Failed to build in-cluster kubernetes client: {error}");
        })
}
