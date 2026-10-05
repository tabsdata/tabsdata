//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, PropagationPolicy};
use kube::core::Selector;
use kube::runtime::wait::Condition;
use tracing::debug;

use crate::base::constants::{HTTP_STATUS_NOT_FOUND, KUBE_SPEC_FIELD, KUBE_SPEC_FIELD_PAUSED};
use crate::base::error::kubernetes::{KubeDeploymentError, KubernetesError};
use crate::kube::api::domain::companions::{as_one_unit, attach_companions};
use crate::kube::api::domain::labels::{
    label_tabsdata, tabsdata_non_ephemeral_selector, tabsdata_selector,
};
use crate::kube::api::support::owner::owner_ref_for_deployment;
use crate::kube::api::support::wait::{AwaitGoal, AwaitPatience, await_condition_with_patience};
use crate::kube::context::KubeContext;

pub async fn create_deployment(
    context: &KubeContext,
    deployment: &Deployment,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
    stamp: Option<&str>,
) -> Result<Deployment, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let mut deployment = deployment.clone();
    label_tabsdata(context, &mut deployment.metadata, &[], stamp).await?;
    let companions = !configmaps.is_empty() || !secrets.is_empty() || latch.is_some();
    if !companions {
        return api
            .create(&PostParams::default(), &deployment)
            .await
            .map_err(|error| {
                KubernetesError::from(KubeDeploymentError::ApplyFailed {
                    namespace: context.namespace().to_owned(),
                    cause: error.to_string(),
                })
            });
    }

    deployment.spec.get_or_insert_with(Default::default).paused = Some(true);
    let name = deployment.metadata.name.clone().unwrap_or_default();

    let creating = context.clone();
    let attaching = context.clone();
    let removing = context.clone();
    let removed = name.clone();
    as_one_unit(
        context.namespace().to_owned(),
        name,
        move || async move {
            let api: Api<Deployment> =
                Api::namespaced(creating.client().clone(), creating.namespace());
            api.create(&PostParams::default(), &deployment)
                .await
                .map_err(|error| {
                    KubernetesError::from(KubeDeploymentError::ApplyFailed {
                        namespace: creating.namespace().to_owned(),
                        cause: error.to_string(),
                    })
                })
        },
        move |created: Deployment| async move {
            let outcome = attach_and_resume(&attaching, &created, configmaps, secrets, latch).await;
            (created, outcome)
        },
        move || async move { delete_deployment(&removing, &removed).await },
    )
    .await
}

async fn attach_and_resume(
    context: &KubeContext,
    created: &Deployment,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
) -> Result<(), KubernetesError> {
    let owner = owner_ref_for_deployment(created)?;
    attach_companions(context, owner, configmaps, secrets, latch, None).await?;
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    let name = created.metadata.name.clone().unwrap_or_default();
    api.patch(
        &name,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({KUBE_SPEC_FIELD: {KUBE_SPEC_FIELD_PAUSED: false}})),
    )
    .await
    .map(|_: Deployment| ())
    .map_err(|error| {
        KubeDeploymentError::ApplyFailed {
            namespace: context.namespace().to_owned(),
            cause: error.to_string(),
        }
        .into()
    })
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

pub async fn delete_deployments_matching(
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

pub async fn list_deployments_matching(
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
    await_deployment_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Bounded,
        AwaitGoal::Presence,
    )
    .await
}

pub async fn await_deployment_condition_resumable(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
) -> Result<Option<Deployment>, KubernetesError> {
    await_deployment_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Resumable,
        AwaitGoal::Presence,
    )
    .await
}

pub async fn await_deployment_absent_resumable(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
) -> Result<Option<Deployment>, KubernetesError> {
    await_deployment_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Resumable,
        AwaitGoal::Absence,
    )
    .await
}

async fn await_deployment_condition_with(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Deployment> + Clone + Send + Sync + 'static,
    patience: AwaitPatience,
    goal: AwaitGoal,
) -> Result<Option<Deployment>, KubernetesError> {
    let api: Api<Deployment> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition_with_patience(context, api, name, condition, patience, goal)
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
