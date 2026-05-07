//
// Copyright 2026 Tabs Data Inc.
//

use std::process::exit;

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client};
use td_common::status::ExitStatus::GeneralError;
use tracing::error;

pub async fn ensure_cluster_reachable(client: &Client) {
    if let Err(error) = client.apiserver_version().await {
        error!("Kubernetes cluster is not reachable: {error}");
        exit(GeneralError.code());
    }
}

pub async fn ensure_namespace_exists(client: &Client, namespace: &str) {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get(namespace).await {
        Ok(_) => {}
        Err(kube::Error::Api(api_error)) if api_error.code == 404 => {
            error!("Namespace '{namespace}' does not exist in the cluster.");
            exit(GeneralError.code());
        }
        Err(error) => {
            error!("Failed to verify namespace '{namespace}': {error}");
            exit(GeneralError.code());
        }
    }
}
