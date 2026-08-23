//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::Condition;
use tracing::debug;

use crate::base::constants::HTTP_STATUS_NOT_FOUND;
use crate::base::error::kubernetes::{KubeJobError, KubernetesError};
use crate::kube::api::companions::attach_companions;
use crate::kube::api::{
    AwaitPatience, await_condition_with_patience, label_tabsdata, owner_ref_for_job,
    tabsdata_non_ephemeral_selector, tabsdata_selector, tabsdata_worker_selector,
};
use crate::kube::context::KubeContext;
use kube::core::Selector;

pub async fn create_job(
    context: &KubeContext,
    job: &Job,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
    stamp: Option<&str>,
) -> Result<Job, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let mut job = job.clone();
    label_tabsdata(context, &mut job.metadata, &[], stamp).await?;
    let created = api
        .create(&PostParams::default(), &job)
        .await
        .map_err(|error| {
            KubernetesError::from(KubeJobError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() && latch.is_none() {
        return Ok(created);
    }
    let owner = owner_ref_for_job(&created)?;
    attach_companions(context, owner, configmaps, secrets, latch, None).await?;
    Ok(created)
}

pub async fn get_job(context: &KubeContext, name: &str) -> Result<Option<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    api.get_opt(name).await.map_err(|error| {
        KubeJobError::GetFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()
    })
}

pub async fn delete_job(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    match api.delete(name, &delete_params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => {
            debug!(
                job = name,
                namespace = %context.namespace(),
                "job.delete: already deleted"
            );
            Ok(())
        }
        Err(error) => Err(KubeJobError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_jobs(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_jobs_matching(context, tabsdata_selector(None)).await
}

pub async fn delete_jobs_non_ephemeral(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_jobs_matching(context, tabsdata_non_ephemeral_selector()).await
}

pub async fn delete_jobs_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<(), KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
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
    list_jobs_matching(context, tabsdata_selector(None)).await
}

pub async fn list_jobs_of_workers(context: &KubeContext) -> Result<Vec<Job>, KubernetesError> {
    list_jobs_matching(context, tabsdata_worker_selector()).await
}

pub async fn list_jobs_non_ephemeral(context: &KubeContext) -> Result<Vec<Job>, KubernetesError> {
    list_jobs_matching(context, tabsdata_non_ephemeral_selector()).await
}

pub async fn list_jobs_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<Vec<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
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

pub async fn await_job_condition(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Job> + Clone + Send + Sync + 'static,
) -> Result<Option<Job>, KubernetesError> {
    await_job_condition_with(context, name, condition, AwaitPatience::Bounded).await
}

pub async fn await_job_condition_resumable(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Job> + Clone + Send + Sync + 'static,
) -> Result<Option<Job>, KubernetesError> {
    await_job_condition_with(context, name, condition, AwaitPatience::Resumable).await
}

async fn await_job_condition_with(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Job> + Clone + Send + Sync + 'static,
    patience: AwaitPatience,
) -> Result<Option<Job>, KubernetesError> {
    let api: Api<Job> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition_with_patience(api, name, condition, patience)
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
