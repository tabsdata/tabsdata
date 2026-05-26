//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client};

use crate::base::error::kubernetes::{KubeNamespaceError, KubernetesError};

pub async fn ensure_namespace_exists(
    client: &Client,
    namespace: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get(namespace).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == 404 => {
            Err(KubeNamespaceError::NotFound(namespace.to_owned()).into())
        }
        Err(error) => Err(KubeNamespaceError::QueryFailed(
            namespace.to_owned(),
            error.to_string(),
        )
        .into()),
    }
}
