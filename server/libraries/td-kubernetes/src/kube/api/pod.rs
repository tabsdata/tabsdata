//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};
use kube::{Api, Client};

use crate::base::error::kubernetes::{KubePodError, KubernetesError};

pub async fn create_pod(
    client: &Client,
    namespace: &str,
    pod: &Pod,
) -> Result<Pod, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    api.create(&PostParams::default(), pod)
        .await
        .map_err(|error| {
            KubePodError::ApplyFailed {
                namespace: namespace.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn await_pod_condition(
    client: &Client,
    namespace: &str,
    name: &str,
    condition: impl Condition<Pod> + Send + Sync + 'static,
) -> Result<Option<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    await_condition(api, name, condition)
        .await
        .map_err(|error| {
            KubePodError::AwaitFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_pod(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubePodError::DeleteFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_pods_by_labels(
    client: &Client,
    namespace: &str,
    label_selector: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
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
                namespace: namespace.to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_pods_by_labels(
    client: &Client,
    namespace: &str,
    label_selector: &str,
) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list_params = ListParams::default().labels(label_selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubePodError::ListFailed {
                namespace: namespace.to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
