//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Namespace;
use kube::Api;

use crate::base::error::kubernetes::{KubeNamespaceError, KubernetesError};
use crate::kube::context::KubeContext;

pub async fn ensure_namespace_exists(context: &KubeContext) -> Result<(), KubernetesError> {
    let api: Api<Namespace> = Api::all(context.client().clone());
    match api.get(context.namespace()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == 404 => {
            Err(KubeNamespaceError::NotFound(context.namespace().to_owned()).into())
        }
        Err(error) => Err(KubeNamespaceError::QueryFailed(
            context.namespace().to_owned(),
            error.to_string(),
        )
        .into()),
    }
}
