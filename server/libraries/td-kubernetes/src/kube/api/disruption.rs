//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::Api;
use kube::api::PostParams;

use crate::base::error::kubernetes::{KubeDisruptionError, KubernetesError};
use crate::kube::api::label_tabsdata;
use crate::kube::context::KubeContext;

pub async fn upsert_disruption_budget(
    context: &KubeContext,
    budget: &PodDisruptionBudget,
) -> Result<PodDisruptionBudget, KubernetesError> {
    let api: Api<PodDisruptionBudget> =
        Api::namespaced(context.client().clone(), context.namespace());
    let mut budget = budget.clone();
    label_tabsdata(&mut budget.metadata, &[]);
    let name = budget
        .metadata
        .name
        .clone()
        .ok_or(KubeDisruptionError::MissingName)?;
    match api.get_opt(&name).await {
        Ok(Some(existing)) => {
            budget.metadata.resource_version = existing.metadata.resource_version;
            api.replace(&name, &PostParams::default(), &budget)
                .await
                .map_err(|error| {
                    KubeDisruptionError::UpsertFailed {
                        name: name.clone(),
                        cause: error.to_string(),
                    }
                    .into()
                })
        }
        Ok(None) => api
            .create(&PostParams::default(), &budget)
            .await
            .map_err(|error| {
                KubeDisruptionError::UpsertFailed {
                    name: name.clone(),
                    cause: error.to_string(),
                }
                .into()
            }),
        Err(error) => Err(KubeDisruptionError::UpsertFailed {
            name,
            cause: error.to_string(),
        }
        .into()),
    }
}
