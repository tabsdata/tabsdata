//
// Copyright 2026 Tabsdata Inc.
//

use std::future::Future;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Api;
use kube::api::PostParams;
use tracing::{error, warn};

use crate::base::error::kubernetes::{
    KubeCompanionsError, KubeConfigMapError, KubeSecretError, KubernetesError,
};
use crate::kube::api::domain::labels::label_tabsdata;
use crate::kube::context::KubeContext;

pub(in crate::kube::api) async fn as_one_unit<
    K,
    Create,
    Attach,
    Remove,
    CreateFut,
    AttachFut,
    RemoveFut,
>(
    namespace: String,
    name: String,
    create: Create,
    attach: Attach,
    remove: Remove,
) -> Result<K, KubernetesError>
where
    K: Send + 'static,
    Create: FnOnce() -> CreateFut + Send + 'static,
    Attach: FnOnce(K) -> AttachFut + Send + 'static,
    Remove: FnOnce() -> RemoveFut + Send + 'static,
    CreateFut: Future<Output = Result<K, KubernetesError>> + Send,
    AttachFut: Future<Output = (K, Result<(), KubernetesError>)> + Send,
    RemoveFut: Future<Output = Result<(), KubernetesError>> + Send,
{
    let reported_namespace = namespace.clone();
    let reported = name.clone();
    let task = tokio::spawn(async move {
        let created = create().await?;
        let (created, attached) = attach(created).await;
        match attached {
            Ok(()) => Ok(created),
            Err(failure) => {
                warn!(
                    "Could not attach the configmaps and secrets of '{name}' in \
                     namespace '{namespace}', so it is being removed: {failure}"
                );
                if let Err(cause) = remove().await {
                    error!(
                        "Could not remove '{name}' in namespace '{namespace}' after \
                         its configmaps and secrets failed to attach: {cause}"
                    );
                    return Err(KubeCompanionsError::CompensationFailed {
                        namespace,
                        name,
                        cause: cause.to_string(),
                    }
                    .into());
                }
                Err(failure)
            }
        }
    });
    match task.await {
        Ok(outcome) => outcome,
        Err(cause) => Err(KubeCompanionsError::CreationAbandoned {
            namespace: reported_namespace,
            name: reported,
            cause: cause.to_string(),
        }
        .into()),
    }
}

pub(in crate::kube::api) async fn attach_companions(
    context: &KubeContext,
    owner: OwnerReference,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
    stamp: Option<&str>,
) -> Result<(), KubernetesError> {
    let configmap_api: Api<ConfigMap> =
        Api::namespaced(context.client().clone(), context.namespace());
    let secret_api: Api<Secret> = Api::namespaced(context.client().clone(), context.namespace());
    for configmap in configmaps {
        attach_configmap(
            context,
            &configmap_api,
            prepare_configmap(context, configmap, &owner, stamp).await?,
        )
        .await?;
    }
    for secret in secrets {
        attach_secret(
            context,
            &secret_api,
            prepare_secret(context, secret, &owner, stamp).await?,
        )
        .await?;
    }
    if let Some(latch) = latch {
        attach_configmap(
            context,
            &configmap_api,
            prepare_configmap(context, latch, &owner, stamp).await?,
        )
        .await?;
    }
    Ok(())
}

async fn prepare_configmap(
    context: &KubeContext,
    mut configmap: ConfigMap,
    owner: &OwnerReference,
    stamp: Option<&str>,
) -> Result<ConfigMap, KubernetesError> {
    stamp_owner(&mut configmap.metadata, owner);
    label_tabsdata(context, &mut configmap.metadata, &[], stamp).await?;
    Ok(configmap)
}

async fn prepare_secret(
    context: &KubeContext,
    mut secret: Secret,
    owner: &OwnerReference,
    stamp: Option<&str>,
) -> Result<Secret, KubernetesError> {
    stamp_owner(&mut secret.metadata, owner);
    label_tabsdata(context, &mut secret.metadata, &[], stamp).await?;
    Ok(secret)
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

#[cfg(test)]
mod tests;
