//
// Copyright 2026 Tabs Data Inc.
//

use std::time::Instant;

use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::constants::{DELETE_POD_POLL_INTERVAL, DELETE_POD_WAIT_TIMEOUT};
use crate::base::error::kubernetes::{KubePodError, KubernetesError};
use crate::kube::api::companions::attach_companions;
use crate::kube::api::{label_tabsdata, owner_ref_for_pod, tabsdata_selector};
use crate::kube::context::KubeContext;

pub async fn create_pod(
    context: &KubeContext,
    pod: &Pod,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
) -> Result<Pod, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let mut pod = pod.clone();
    label_tabsdata(&mut pod.metadata, &[]);
    let created = api
        .create(&PostParams::default(), &pod)
        .await
        .map_err(|error| {
            KubernetesError::from(KubePodError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() && latch.is_none() {
        return Ok(created);
    }
    let owner = owner_ref_for_pod(&created)?;
    attach_companions(context, owner, configmaps, secrets, latch).await?;
    Ok(created)
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

pub async fn delete_pods(context: &KubeContext) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Background),
        ..DeleteParams::default()
    };
    api.delete_collection(&delete_params, &list_params)
        .await
        .map(|_| ())
        .map_err(|error| KubePodError::DeleteCollectionFailed {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    let deadline = Instant::now() + DELETE_POD_WAIT_TIMEOUT;
    loop {
        let remaining = api
            .list(&list_params)
            .await
            .map(|list| list.items.len())
            .map_err(|error| KubePodError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            })?;
        if remaining == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(KubePodError::DeleteCollectionFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: format!(
                    "There is/are {remaining} pod(s) still present after {}s",
                    DELETE_POD_WAIT_TIMEOUT.as_secs()
                ),
            }
            .into());
        }
        tokio::time::sleep(DELETE_POD_POLL_INTERVAL).await;
    }
}

pub async fn list_pods(context: &KubeContext) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let list_params = ListParams::default().labels_from(&selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubePodError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
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
