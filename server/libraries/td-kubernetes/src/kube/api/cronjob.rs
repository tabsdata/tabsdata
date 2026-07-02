//
// Copyright 2026 Tabs Data Inc.
//

use k8s_openapi::api::batch::v1::CronJob;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::Api;
use kube::api::{DeleteParams, PostParams, PropagationPolicy};

use crate::base::error::kubernetes::{KubeJobError, KubernetesError};
use crate::kube::api::companions::attach_companions;
use crate::kube::api::{label_tabsdata, owner_ref_for_cronjob};
use crate::kube::context::KubeContext;

// CronJob is a batch resource like Job; it reuses `KubeJobError` for apply/delete
// failures rather than introducing a parallel error enum.

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
            KubernetesError::from(KubeJobError::ApplyFailed {
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
            KubeJobError::DeleteFailed {
                namespace: context.namespace().to_owned(),
                name: name.to_owned(),
                cause: error.to_string(),
            }
            .into()
        })
}
