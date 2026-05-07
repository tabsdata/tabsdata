//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client};
use td_common::status::ExitStatus::GeneralError;
use tracing::error;

use crate::exit::{Outcome, abort};

pub async fn ensure_cluster_reachable(client: &Client) -> Outcome {
    if let Err(error) = client.apiserver_version().await {
        error!("Kubernetes cluster is not reachable: {error}");
        return abort(GeneralError);
    }
    Ok(())
}

pub async fn ensure_namespace_exists(client: &Client, namespace: &str) -> Outcome {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get(namespace).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == 404 => {
            error!("Namespace '{namespace}' does not exist in the cluster.");
            abort(GeneralError)
        }
        Err(error) => {
            error!("Failed to verify namespace '{namespace}': {error}");
            abort(GeneralError)
        }
    }
}
