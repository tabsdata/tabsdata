//
// Copyright 2026 Tabsdata Inc.
//

pub mod status;

use std::time::Instant;

use futures::AsyncBufRead;
use k8s_openapi::api::core::v1::{ConfigMap, Pod, Secret};
use kube::Api;
use kube::api::{DeleteParams, ListParams, LogParams, PostParams, PropagationPolicy};
use kube::runtime::wait::Condition;
use tracing::debug;

use crate::base::constants::{
    DELETE_WORKLOAD_POLL_INTERVAL, DELETE_WORKLOAD_WAIT_TIMEOUT, HTTP_STATUS_FORBIDDEN,
    HTTP_STATUS_NOT_FOUND, KUBE_METADATA_FIELD_NAME, POD_API_VERSION_V1, POD_KIND,
    POD_RESTART_POLICY_NEVER, POD_YAML_FIELD_API_VERSION, POD_YAML_FIELD_COMMAND,
    POD_YAML_FIELD_CONTAINERS, POD_YAML_FIELD_CPU, POD_YAML_FIELD_IMAGE, POD_YAML_FIELD_KIND,
    POD_YAML_FIELD_MEMORY, POD_YAML_FIELD_METADATA, POD_YAML_FIELD_NAMESPACE,
    POD_YAML_FIELD_REQUESTS, POD_YAML_FIELD_RESOURCES, POD_YAML_FIELD_RESTART_POLICY,
    POD_YAML_FIELD_SPEC, PROBE_COMMAND, PROBE_CONTAINER, PROBE_CPU, PROBE_IMAGE, PROBE_MEMORY,
    PROBE_POD,
};
use crate::base::error::kubernetes::{KubePodError, KubernetesError};
use crate::kube::api::domain::companions::{as_one_unit, attach_companions};
use crate::kube::api::domain::labels::{
    LabelFilter, label_tabsdata, tabsdata_ephemeral_selector, tabsdata_non_ephemeral_selector,
    tabsdata_selector, tabsdata_worker_selector,
};
use crate::kube::api::support::owner::owner_ref_for_pod;
use crate::kube::api::support::wait::{AwaitGoal, AwaitPatience, await_condition_with_patience};
use crate::kube::context::KubeContext;
use kube::core::Selector;

pub async fn probe_scheduler(context: &KubeContext) -> Result<Option<String>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let parameters = PostParams {
        dry_run: true,
        ..PostParams::default()
    };
    let admitted = match api
        .create(&parameters, &probe_pod(context.namespace()))
        .await
    {
        Ok(admitted) => admitted,
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_FORBIDDEN => {
            debug!(
                namespace = context.namespace(),
                "kube.pod: cannot probe the scheduler; creating pods is not allowed"
            );
            return Ok(None);
        }
        Err(error) => {
            return Err(KubePodError::ApplyFailed {
                namespace: context.namespace().to_owned(),
                cause: error.to_string(),
            }
            .into());
        }
    };
    Ok(admitted.spec.and_then(|spec| spec.scheduler_name))
}

fn probe_pod(namespace: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        POD_YAML_FIELD_API_VERSION: POD_API_VERSION_V1,
        POD_YAML_FIELD_KIND: POD_KIND,
        POD_YAML_FIELD_METADATA: {
            KUBE_METADATA_FIELD_NAME: PROBE_POD,
            POD_YAML_FIELD_NAMESPACE: namespace,
        },
        POD_YAML_FIELD_SPEC: {
            POD_YAML_FIELD_RESTART_POLICY: POD_RESTART_POLICY_NEVER,
            POD_YAML_FIELD_CONTAINERS: [{
                KUBE_METADATA_FIELD_NAME: PROBE_CONTAINER,
                POD_YAML_FIELD_IMAGE: PROBE_IMAGE,
                POD_YAML_FIELD_COMMAND: [PROBE_COMMAND],
                POD_YAML_FIELD_RESOURCES: {
                    POD_YAML_FIELD_REQUESTS: {
                        POD_YAML_FIELD_CPU: PROBE_CPU,
                        POD_YAML_FIELD_MEMORY: PROBE_MEMORY,
                    },
                },
            }],
        },
    }))
    .expect("the scheduler probe pod is a valid pod spec")
}

pub async fn create_pod(
    context: &KubeContext,
    pod: &Pod,
    configmaps: Vec<ConfigMap>,
    secrets: Vec<Secret>,
    latch: Option<ConfigMap>,
    stamp: Option<&str>,
) -> Result<Pod, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let mut pod = pod.clone();
    label_tabsdata(context, &mut pod.metadata, &[], stamp).await?;
    let companions = !configmaps.is_empty() || !secrets.is_empty() || latch.is_some();
    if !companions {
        return api
            .create(&PostParams::default(), &pod)
            .await
            .map_err(|error| {
                KubernetesError::from(KubePodError::ApplyFailed {
                    namespace: context.namespace().to_owned(),
                    cause: error.to_string(),
                })
            });
    }

    let name = pod.metadata.name.clone().unwrap_or_default();
    let creating = context.clone();
    let attaching = context.clone();
    let removing = context.clone();
    let removed = name.clone();
    as_one_unit(
        context.namespace().to_owned(),
        name,
        move || async move {
            let api: Api<Pod> = Api::namespaced(creating.client().clone(), creating.namespace());
            api.create(&PostParams::default(), &pod)
                .await
                .map_err(|error| {
                    KubernetesError::from(KubePodError::ApplyFailed {
                        namespace: creating.namespace().to_owned(),
                        cause: error.to_string(),
                    })
                })
        },
        move |created: Pod| async move {
            let outcome = match owner_ref_for_pod(&created) {
                Ok(owner) => {
                    attach_companions(&attaching, owner, configmaps, secrets, latch, None).await
                }
                Err(error) => Err(error.into()),
            };
            (created, outcome)
        },
        move || async move { delete_pod(&removing, &removed).await },
    )
    .await
}

pub async fn delete_pod(context: &KubeContext, name: &str) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
        ..DeleteParams::default()
    };
    match api.delete(name, &delete_params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(api_error)) if api_error.code == HTTP_STATUS_NOT_FOUND => {
            debug!(
                pod = name,
                namespace = %context.namespace(),
                "pod.delete: already deleted"
            );
            Ok(())
        }
        Err(error) => Err(KubePodError::DeleteFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()),
    }
}

pub async fn delete_pods(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_pods_matching(context, tabsdata_selector(None)).await
}

pub async fn delete_pods_non_ephemeral(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_pods_matching(context, tabsdata_non_ephemeral_selector()).await
}

pub async fn delete_pods_ephemeral(context: &KubeContext) -> Result<(), KubernetesError> {
    delete_pods_matching(context, tabsdata_ephemeral_selector()).await
}

pub async fn delete_pods_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<(), KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let list_params = ListParams::default().labels_from(&selector);
    let delete_params = DeleteParams {
        propagation_policy: Some(PropagationPolicy::Foreground),
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
    let deadline = Instant::now() + DELETE_WORKLOAD_WAIT_TIMEOUT;
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
                    DELETE_WORKLOAD_WAIT_TIMEOUT.as_secs()
                ),
            }
            .into());
        }
        tokio::time::sleep(DELETE_WORKLOAD_POLL_INTERVAL).await;
    }
}

pub async fn list_pods(context: &KubeContext) -> Result<Vec<Pod>, KubernetesError> {
    list_pods_matching(context, tabsdata_selector(None)).await
}

pub async fn list_pods_of_workers(context: &KubeContext) -> Result<Vec<Pod>, KubernetesError> {
    list_pods_matching(context, tabsdata_worker_selector()).await
}

pub async fn list_pods_non_ephemeral(context: &KubeContext) -> Result<Vec<Pod>, KubernetesError> {
    list_pods_matching(context, tabsdata_non_ephemeral_selector()).await
}

pub async fn list_pods_matching(
    context: &KubeContext,
    selector: Selector,
) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
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
    condition: impl Condition<Pod> + Clone + Send + Sync + 'static,
) -> Result<Option<Pod>, KubernetesError> {
    await_pod_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Bounded,
        AwaitGoal::Presence,
    )
    .await
}

pub async fn await_pod_condition_resumable(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Pod> + Clone + Send + Sync + 'static,
) -> Result<Option<Pod>, KubernetesError> {
    await_pod_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Resumable,
        AwaitGoal::Presence,
    )
    .await
}

pub async fn await_pod_absent_resumable(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Pod> + Clone + Send + Sync + 'static,
) -> Result<Option<Pod>, KubernetesError> {
    await_pod_condition_with(
        context,
        name,
        condition,
        AwaitPatience::Resumable,
        AwaitGoal::Absence,
    )
    .await
}

async fn await_pod_condition_with(
    context: &KubeContext,
    name: &str,
    condition: impl Condition<Pod> + Clone + Send + Sync + 'static,
    patience: AwaitPatience,
    goal: AwaitGoal,
) -> Result<Option<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    await_condition_with_patience(context, api, name, condition, patience, goal)
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

pub async fn get_pods(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Vec<Pod>, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let selector = tabsdata_selector(label);
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

pub async fn require_pod(
    context: &KubeContext,
    label: Option<LabelFilter<'_>>,
) -> Result<Pod, KubernetesError> {
    let selector = tabsdata_selector(label);
    let mut pods = get_pods(context, label).await?;
    match pods.len() {
        1 => Ok(pods.remove(0)),
        0 => Err(KubePodError::NotFound {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
        }
        .into()),
        count => Err(KubePodError::MultipleFound {
            namespace: context.namespace().to_owned(),
            label_selector: selector.to_string(),
            count,
        }
        .into()),
    }
}

pub async fn stream_pod_log(
    context: &KubeContext,
    name: &str,
    container: &str,
    follow: bool,
) -> Result<impl AsyncBufRead, KubernetesError> {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let log_params = LogParams {
        container: Some(container.to_owned()),
        follow,
        ..LogParams::default()
    };
    api.log_stream(name, &log_params).await.map_err(|error| {
        KubePodError::LogStreamFailed {
            namespace: context.namespace().to_owned(),
            name: name.to_owned(),
            cause: error.to_string(),
        }
        .into()
    })
}
