//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::batch::v1::CronJob;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams, PropagationPolicy};
use kube::runtime::wait::{Condition, await_condition};

use crate::base::error::kubernetes::{KubeCronJobError, KubernetesError};
use crate::kube::api::companions::attach_companions;
use crate::kube::api::{
    label_tabsdata, owner_ref_for_cronjob, tabsdata_non_ephemeral_selector, tabsdata_selector,
};
use crate::kube::context::KubeContext;
use kube::core::Selector;

pub async fn create_cronjob(
    context: &KubeContext,
    cronjob: &CronJob,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
) -> Result<CronJob, KubernetesError> {
    let api: Api<CronJob> = Api::namespaced(context.client().clone(), context.namespace());
    let mut cronjob = cronjob.clone();
    label_tabsdata(&mut cronjob.metadata, &[]);
    let created = api
        .create(&PostParams::default(), &cronjob)
        .await
        .map_err(|error| {
            KubernetesError::from(KubeCronJobError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            })
        })?;
    if configmaps.is_empty() && secrets.is_empty() && latch.is_none() {
        return Ok(created);
    }
    let owner = owner_ref_for_cronjob(&created)?;
    attach_companions(context, owner, configmaps, secrets, latch).await?;
    Ok(created)
}

pub async fn delete_cronjob(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<CronJob> = Api::namespaced(context.client().clone(), context.namespace());
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    api.delete(name, &delete_params)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeCronJobError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn delete_cronjobs(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_cronjobs_matching(context, tabsdata_selector(None)).await
}

pub async fn delete_cronjobs_non_ephemeral(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_cronjobs_matching(context, tabsdata_non_ephemeral_selector()).await
}

async fn delete_cronjobs_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<(), KubernetesError> {
    let api: Api<CronJob> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    api.delete_collection(&delete_params, &list_params)
        .await
        .map(|_| ())
        .map_err(|error| {
            KubeCronJobError::DeleteCollectionFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn list_cronjobs(context: &KubeContext) -> Result<Vec<CronJob>, KubernetesError> {
    list_cronjobs_matching(context, tabsdata_selector(None)).await
}

pub async fn list_cronjobs_non_ephemeral(
    context: &KubeContext,
) -> Result<Vec<CronJob>, KubernetesError> {
    list_cronjobs_matching(context, tabsdata_non_ephemeral_selector()).await
}

async fn list_cronjobs_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<Vec<CronJob>, KubernetesError> {
    let api: Api<CronJob> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    api.list(&list_params)
        .await
        .map(|list| list.items)
        .map_err(|error| {
            KubeCronJobError::ListFailed {
                namespace: context.namespace().to_owned(),
                label_selector: selector.to_string(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub async fn await_cronjob_condition(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<CronJob> + Send + Sync + 'static,
) -> Result<Option<CronJob>, KubernetesError> {
    let api: Api<CronJob> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition(api, name, condition)
        .await
        .map_err(|error| {
            KubeCronJobError::AwaitFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}

pub fn cronjob_scheduled() -> impl Condition<CronJob> {
    |cronjob: Option<&CronJob>| {
        cronjob
            .and_then(|current| current.status.as_ref())
            .is_some_and(|status| status.last_schedule_time.is_some())
    }
}

pub fn cronjob_succeeded() -> impl Condition<CronJob> {
    |cronjob: Option<&CronJob>| {
        cronjob
            .and_then(|current| current.status.as_ref())
            .is_some_and(|status| status.last_successful_time.is_some())
    }
}

pub fn cronjob_active() -> impl Condition<CronJob> {
    |cronjob: Option<&CronJob>| {
        cronjob
            .and_then(|current| current.status.as_ref())
            .and_then(|status| status.active.as_ref())
            .is_some_and(|active| !active.is_empty())
    }
}
