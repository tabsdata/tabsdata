//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::error::kubernetes::{KubePodError, KubernetesError};
use crate::kube::context::KubeContext;

pub async fn create_pod(context: &KubeContext, pod: &Pod) -> Result<Pod, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    api.create(&PostParams::default(), pod)
        .await
        .map_err(|error| {
            KubePodError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn await_pod_condition(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Pod> + Send + Sync + 'static,
) -> Result<Option<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition(api, name, condition)
        .await
        .map_err(|error| {
            KubePodError::AwaitFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_pod(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubePodError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_pods_by_labels(
    context: &KubeContext,
    label_selector: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels(label_selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Background),
        ..DeleteParams::default()
    };
    api.delete_collection(&delete_params, &list_params)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubePodError::DeleteCollectionFailed {
                namespace: context.namespace().to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_pods_by_labels(
    context: &KubeContext,
    label_selector: &str,
) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels(label_selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubePodError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
