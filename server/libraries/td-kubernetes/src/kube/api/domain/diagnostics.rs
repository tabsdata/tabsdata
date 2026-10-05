//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};
use tokio::time::timeout;
use tracing::debug;

use crate::base::constants::{WORKLOAD_LOG_TIMEOUT, WORKLOAD_LOG_WAIT};
use crate::kube::api::entity::pod::get_pods;
use crate::kube::context::KubeContext;

pub async fn report_workload(context: &KubeContext, workload: &str) {
    if timeout(WORKLOAD_LOG_TIMEOUT, gather(context, workload))
        .await
        .is_err()
    {
        debug!("Gathering diagnostics for '{workload}' timed out.");
    }
}

async fn gather(context: &KubeContext, workload: &str) {
    let pods = match get_pods(context, None).await {
        Ok(pods) => pods,
        Err(error) => {
            debug!("Could not list the pods of '{workload}': {error}.");
            return;
        }
    };
    let owned: Vec<&Pod> = pods.iter().filter(|pod| owned_by(pod, workload)).collect();
    if owned.is_empty() {
        debug!("No pod of '{workload}' is present to diagnose.");
        return;
    }
    for pod in owned {
        report_pod(context, workload, pod).await;
    }
}

fn owned_by(pod: &Pod, workload: &str) -> bool {
    pod.metadata
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with(workload))
}

async fn report_pod(context: &KubeContext, workload: &str, pod: &Pod) {
    let Some(name) = pod.metadata.name.as_deref() else {
        return;
    };
    report_state(workload, pod, name);
    for container in containers_of(pod) {
        report_container_log(context, name, &container).await;
    }
}

fn report_state(workload: &str, pod: &Pod, name: &str) {
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .unwrap_or("unknown");
    debug!("Diagnostics for '{workload}': pod '{name}' is {phase}.");
    let Some(status) = pod.status.as_ref() else {
        return;
    };
    for container in status.container_statuses.iter().flatten() {
        let waiting = container
            .state
            .as_ref()
            .and_then(|state| state.waiting.as_ref())
            .and_then(|waiting| waiting.reason.as_deref());
        let terminated = container
            .state
            .as_ref()
            .and_then(|state| state.terminated.as_ref());
        if let Some(reason) = waiting {
            debug!("  container '{}' is waiting: {reason}.", container.name);
        }
        if let Some(terminated) = terminated {
            debug!(
                "  container '{}' terminated with code {:?}: {}.",
                container.name,
                terminated.exit_code,
                terminated.reason.as_deref().unwrap_or("no reason given")
            );
        }
    }
}

fn containers_of(pod: &Pod) -> Vec<String> {
    pod.spec
        .as_ref()
        .map(|spec| {
            spec.init_containers
                .iter()
                .flatten()
                .chain(spec.containers.iter())
                .map(|container| container.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

async fn report_container_log(context: &KubeContext, pod: &str, container: &str) {
    let api: Api<Pod> = Api::namespaced(context.client().clone(), context.namespace());
    let params = LogParams {
        container: Some(container.to_owned()),
        ..LogParams::default()
    };
    let fetched = timeout(WORKLOAD_LOG_WAIT, api.logs(pod, &params)).await;
    let text = match fetched {
        Ok(Ok(text)) => text,
        Ok(Err(error)) => {
            debug!("  no log for '{pod}/{container}': {error}.");
            return;
        }
        Err(_) => {
            debug!("  fetching the log of '{pod}/{container}' timed out.");
            return;
        }
    };
    if text.trim().is_empty() {
        debug!("  the log of '{pod}/{container}' is empty.");
        return;
    }
    for line in text.lines() {
        debug!("  [{pod}/{container}] {line}");
    }
}

#[cfg(test)]
mod tests;
