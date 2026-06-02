//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::error::kubernetes::{KubePodError, KubernetesError};
use crate::base::naming::to_kube_identifier;
use crate::kube::api::secret::create_secret;
use crate::kube::api::{
    ConfigMapSpec, SecretSpec, component_from_pod, create_configmap, label_tabsdata,
    owner_ref_for_pod, part_of_tabsdata,
};
use crate::kube::context::KubeContext;

pub async fn create_pod(
    context: &KubeContext,
    pod: &Pod,
    template: &str,
    runid: Option<&str>,
    configmaps: Vec<ConfigMapSpec>,
    secrets: Vec<SecretSpec>,
) -> Result<Pod, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let mut pod = pod.clone();
    label_tabsdata(&mut pod.metadata);
    let created = api
        .create(&PostParams::default(), &pod)
        .await
        .map_err(|error| {
            KubernetesError::from(KubePodError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() {
        return Ok(created);
    }
    let component = component_from_pod(&created, template)?;
    let owner = owner_ref_for_pod(&created, template)?;
    for spec in configmaps {
        let name = to_kube_identifier(&component, &spec.name, runid)?;
        create_configmap(context, &name, spec.data, Some(owner.clone())).await?;
    }
    for spec in secrets {
        let name = to_kube_identifier(&component, &spec.name, runid)?;
        create_secret(context, &name, spec.data, Some(owner.clone())).await?;
    }
    Ok(created)
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

pub async fn delete_pods(context: &KubeContext) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = part_of_tabsdata();
    let list_params = ListParams::default().labels_from(&selector);
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
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_pods(context: &KubeContext) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = part_of_tabsdata();
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
