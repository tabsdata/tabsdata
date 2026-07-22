//
// Copyright 2026 Tabsdata Inc.
//

use k8s_openapi::api::core::v1::{ContainerStatus, Pod};

use crate::base::constants::{POD_PHASE_FAILED, POD_PHASE_SUCCEEDED};

pub fn is_container_started(status: &ContainerStatus) -> bool {
    status
        .state
        .as_ref()
        .map(|state| state.running.is_some() || state.terminated.is_some())
        .unwrap_or(false)
}

pub fn started_container_names(pod: &Pod) -> Vec<String> {
    let status = pod.status.as_ref();
    let init = status
        .and_then(|status| status.init_container_statuses.as_ref())
        .map(Vec::as_slice)
        .unwrap_or_default();
    let main = status
        .and_then(|status| status.container_statuses.as_ref())
        .map(Vec::as_slice)
        .unwrap_or_default();
    init.iter()
        .chain(main.iter())
        .filter(|status| is_container_started(status))
        .map(|status| status.name.clone())
        .collect()
}

pub fn is_pod_terminal(pod: &Pod) -> bool {
    matches!(
        pod.status
            .as_ref()
            .and_then(|status| status.phase.as_deref()),
        Some(POD_PHASE_SUCCEEDED) | Some(POD_PHASE_FAILED)
    )
}
