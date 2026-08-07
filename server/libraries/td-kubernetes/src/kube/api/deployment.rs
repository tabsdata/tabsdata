//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::core::Selector;
use kube::runtime::wait::Condition;
use tracing::debug;

use crate::base::constants::HTTP_STATUS_NOT_FOUND;
use crate::base::error::kubernetes::{KubeDeploymentError, KubernetesError};
use crate::kube::api::companions::attach_companions;
use crate::kube::api::{
    AwaitPatience, await_condition_forever, label_tabsdata, owner_ref_for_deployment,
    tabsdata_non_ephemeral_selector, tabsdata_selector,
};
use crate::kube::context::KubeContext;

pub async fn create_deployment(
    context: &KubeContext,
    deployment: &Deployment,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
) -> Result<Deployment, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let mut deployment = deployment.clone();
    label_tabsdata(&mut deployment.metadata, &[]);
    let created = api
        .create(&PostParams::default(), &deployment)
        .await
        .map_err(|error| {
            KubernetesError::from(KubeDeploymentError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() && latch.is_none() {
        return Ok(created);
    }
    let owner = owner_ref_for_deployment(&created)?;
    attach_companions(context, owner, configmaps, secrets, latch).await?;
    Ok(created)
}

pub async fn delete_deployment(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    match api.delete(name, &delete_params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => {
            debug!(
                deployment = name,
                namespace = %context.namespace(),
                "deployment.delete: already deleted"
            );
            Ok(())
        }
        Err(error) => Err(KubeDeploymentError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_deployments(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_deployments_matching(context, tabsdata_selector(None)).await
}

pub async fn delete_deployments_non_ephemeral(
    context: &KubeContext,
) -> Result<(), KubernetesError> {
    delete_deployments_matching(context, tabsdata_non_ephemeral_selector()).await
}

async fn delete_deployments_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<(), KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    api.delete_collection(&delete_params, &list_params)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeDeploymentError::DeleteCollectionFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_deployments(context: &KubeContext) -> Result<Vec<Deployment>, KubernetesError> {
    list_deployments_matching(context, tabsdata_selector(None)).await
}

pub async fn list_deployments_non_ephemeral(
    context: &KubeContext,
) -> Result<Vec<Deployment>, KubernetesError> {
    list_deployments_matching(context, tabsdata_non_ephemeral_selector()).await
}

async fn list_deployments_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<Vec<Deployment>, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeDeploymentError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn await_deployment_condition(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
) -> Result<Option<Deployment>, KubernetesError> {
    await_deployment_condition_with(context, name, condition, AwaitPatience::Bounded).await
}

pub async fn await_deployment_condition_forever(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
) -> Result<Option<Deployment>, KubernetesError> {
    await_deployment_condition_with(context, name, condition, AwaitPatience::Forever).await
}

async fn await_deployment_condition_with(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
    patience: AwaitPatience,
) -> Result<Option<Deployment>, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition_forever(api, name, condition, patience)
        .await
        .map_err(|error| {
            KubeDeploymentError::AwaitFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub fn is_deployment_available() -> impl Condition<Deployment> + Clone + Send + Sync + 'static {
    |deployment: Option<&Deployment>| {
        let Some(deployment) = deployment else {
            return false;
        };
        let desired = deployment
            .spec
            .as_ref()
            .and_then(|spec| spec.replicas)
            .unwrap_or(1);
        let ready = deployment
            .status
            .as_ref()
            .and_then(|status| status.ready_replicas)
            .unwrap_or(0);
        ready >= desired
    }
}

pub async fn pods_of_deployment(
    context: &KubeContext,
    name: &str,
) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let deployment = api.get(name).await.map_err(|error| {
        KubernetesError::from(KubeDeploymentError::PodResolutionFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        })
    })?;

    let labels = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .filter(|labels| !labels.is_empty())
        .ok_or_else(|| KubeDeploymentError::MissingSelector {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
        })?;

    let expression = labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    pods.list(&ListParams::default().labels(&expression))
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeDeploymentError::PodResolutionFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
