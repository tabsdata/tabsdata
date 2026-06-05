//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Api;
use kube::api::PostParams;

use crate::base::error::kubernetes::{KubeConfigMapError, KubeSecretError, KubernetesError};
use crate::kube::api::label_tabsdata;
use crate::kube::context::KubeContext;

pub(super) async fn attach_companions(
    context: &KubeContext,
    owner: OwnerReference,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
) -> Result<(), KubernetesError> {
    let configmap_api: Api<ConfigMap> =
        Api::namespaced(context.client().clone(), context.namespace());
    let secret_api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    for configmap in configmaps {
        attach_configmap(context, &configmap_api, prepare_configmap(configmap, &owner)).await?;
    }
    for secret in secrets {
        attach_secret(context, &secret_api, prepare_secret(secret, &owner)).await?;
    }
    if let Some(latch) = latch {
        attach_configmap(context, &configmap_api, prepare_configmap(latch, &owner)).await?;
    }
    Ok(())
}

fn prepare_configmap(mut configmap: ConfigMap, owner: &OwnerReference) -> ConfigMap {
    stamp_owner(&mut configmap.metadata, owner);
    label_tabsdata(&mut configmap.metadata, &[]);
    configmap
}

fn prepare_secret(mut secret: Secret, owner: &OwnerReference) -> Secret {
    stamp_owner(&mut secret.metadata, owner);
    label_tabsdata(&mut secret.metadata, &[]);
    secret
}

fn stamp_owner(metadata: &mut ObjectMeta, owner: &OwnerReference) {
    metadata
        .owner_references
        .get_or_insert_with(Vec::new)
        .push(owner.clone());
}

async fn attach_configmap(
    context: &KubeContext,
    api: &Api<ConfigMap>,
    configmap: ConfigMap,
) -> Result<(), KubernetesError> {
    let name = configmap.metadata.name.clone().unwrap_or_default();
    api.create(&PostParams::default(), &configmap)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeConfigMapError::CreateFailed {
                namespace: context.namespace().to_owned(),
                name,
                cause: error.to_string(),
            }
            .into()
        })
}

async fn attach_secret(
    context: &KubeContext,
    api: &Api<Secret>,
    secret: Secret,
) -> Result<(), KubernetesError> {
    let name = secret.metadata.name.clone().unwrap_or_default();
    api.create(&PostParams::default(), &secret)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeSecretError::CreateFailed {
                namespace: context.namespace().to_owned(),
                name,
                cause: error.to_string(),
            }
            .into()
        })
}
