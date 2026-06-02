//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::batch::v1::Job;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::error::kubernetes::{KubeJobError, KubernetesError};
use crate::base::naming::to_kube_identifier;
use crate::kube::api::secret::create_secret;
use crate::kube::api::{
    ConfigMapSpec, SecretSpec, component_from_job, create_configmap, label_tabsdata,
    owner_ref_for_job, tabsdata_selector,
};
use crate::kube::context::KubeContext;

pub async fn create_job(
    context: &KubeContext,
    job: &Job,
    template: &str,
    runid: Option<&str>,
    configmaps: Vec<ConfigMapSpec>,
    secrets: Vec<SecretSpec>,
) -> Result<Job, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let mut job = job.clone();
    label_tabsdata(&mut job.metadata, &[]);
    let created = api
        .create(&PostParams::default(), &job)
        .await
        .map_err(|error| {
            KubernetesError::from(KubeJobError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() {
        return Ok(created);
    }
    let component = component_from_job(&created, template)?;
    let owner = owner_ref_for_job(&created, template)?;
    for spec in configmaps {
        let name = to_kube_identifier(&component, &spec.name, runid)?;
        create_configmap(context, &name, spec.data, Some(owner.clone()), None).await?;
    }
    for spec in secrets {
        let name = to_kube_identifier(&component, &spec.name, runid)?;
        create_secret(context, &name, spec.data, Some(owner.clone())).await?;
    }
    Ok(created)
}

pub async fn await_job_condition(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Job> + Send + Sync + 'static,
) -> Result<Option<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition(api, name, condition)
        .await
        .map_err(|error| {
            KubeJobError::AwaitFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_job(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeJobError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_jobs(context: &KubeContext) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Background),
        ..DeleteParams::default()
    };
    api.delete_collection(&delete_params, &list_params)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeJobError::DeleteCollectionFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_jobs(context: &KubeContext) -> Result<Vec<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(None);
    let list_params = ListParams::default().labels_from(&selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeJobError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}
