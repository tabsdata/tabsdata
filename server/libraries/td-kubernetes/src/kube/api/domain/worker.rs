//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams, PropagationPolicy};
use td_objects::types::basic::WorkerId;

use crate::base::error::kubernetes::{
    KubeCronJobError, KubeJobError, KubePodError, KubernetesError,
};
use crate::base::labels::LABEL_WORKER_ID;
use crate::base::naming::worker_id_text;
use crate::kube::api::domain::labels::{LabelFilter, tabsdata_selector};
use crate::kube::context::KubeContext;

pub async fn delete_worker(
    context: &KubeContext,
    worker_id: &WorkerId,
) -> Result<(), KubernetesError> {
    let worker_id = worker_id_text(worker_id);
    let selector = tabsdata_selector(Some(LabelFilter::new(LABEL_WORKER_ID, &worker_id)));
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        grace_period_seconds: Some(0),
        propagation_policy: Some(PropagationPolicy::Background),
        ..DeleteParams::default()
    };
    let namespace = context.namespace();
    let client = context.client().clone();

    Api::<CronJob>::namespaced(client.clone(), namespace)
        .delete_collection(&delete_params, &list_params)
        .await
        .map_err(|error| KubeCronJobError::DeleteCollectionFailed {
            namespace: namespace.to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    Api::<Job>::namespaced(client.clone(), namespace)
        .delete_collection(&delete_params, &list_params)
        .await
        .map_err(|error| KubeJobError::DeleteCollectionFailed {
            namespace: namespace.to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    Api::<Pod>::namespaced(client, namespace)
        .delete_collection(&delete_params, &list_params)
        .await
        .map_err(|error| KubePodError::DeleteCollectionFailed {
            namespace: namespace.to_owned(),
            label_selector: selector.to_string(),
            cause: error.to_string(),
        })?;
    Ok(())
}
