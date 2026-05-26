//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::batch::v1::Job;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};
use kube::{Api, Client};

use crate::base::error::kubernetes::{KubeJobError, KubernetesError};

pub async fn create_job(
    client: &Client,
    namespace: &str,
    job: &Job,
) -> Result<Job, KubernetesError> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    api.create(&PostParams::default(), job)
        .await
        .map_err(|error| {
            KubeJobError::ApplyFailed {
                namespace: namespace.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn await_job_condition(
    client: &Client,
    namespace: &str,
    name: &str,
    condition: impl Condition<Job> + Send + Sync + 'static,
) -> Result<Option<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    await_condition(api, name, condition)
        .await
        .map_err(|error| {
            KubeJobError::AwaitFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_job(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    api.delete(name, &DeleteParams::default())
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeJobError::DeleteFailed {
                namespace: namespace.to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_jobs_by_labels(
    client: &Client,
    namespace: &str,
    label_selector: &str,
) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
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
                namespace: namespace.to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_jobs_by_labels(
    client: &Client,
    namespace: &str,
    label_selector: &str,
) -> Result<Vec<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    let list_params = ListParams::default().labels(label_selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeJobError::ListFailed {
                namespace: namespace.to_owned(),
                label_selector: label_selector.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
