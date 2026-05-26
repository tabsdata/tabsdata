//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::batch::v1::Job;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::error::kubernetes::{KubeJobError, KubernetesError};
use crate::kube::context::KubeContext;

pub async fn create_job(context: &KubeContext, job: &Job) -> Result<Job, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    api.create(&PostParams::default(), job)
        .await
        .map_err(|error| {
            KubeJobError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
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

pub async fn delete_jobs_by_labels(
    context: &KubeContext,
    label_selector: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels(label_selector);
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
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_jobs_by_labels(
    context: &KubeContext,
    label_selector: &str,
) -> Result<Vec<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels(label_selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeJobError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
