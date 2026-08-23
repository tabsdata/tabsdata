//
// Copyright 2026 Tabsdata Inc.
//

//! Kubernetes [`WorkloadObserver`]: one LIST per workload kind; owned
//! workloads (a CronJob's child Jobs, a Job's pods) fold into their owner.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::HashMap;
use td_bridge::observer::{
    ObservedWorkload, ObserverError, WorkloadKind, WorkloadObservation, WorkloadObserver,
    WorkloadPhase,
};
use td_objects::types::basic::{AtTime, WorkerId};

use crate::base::constants::{
    POD_PHASE_FAILED, POD_PHASE_PENDING, POD_PHASE_RUNNING, POD_PHASE_SUCCEEDED,
};
use crate::base::error::kubernetes::KubernetesError;
use crate::base::labels::LABEL_WORKER_ID;
use crate::kube::api::cronjob::{delete_cronjob, list_cronjobs};
use crate::kube::api::deployment::delete_deployment;
use crate::kube::api::job::{delete_job, list_jobs};
use crate::kube::api::pod::{delete_pod, list_pods};
use crate::kube::context::KubeContext;

pub struct KubernetesWorkloadObserver {
    context: KubeContext,
}

impl KubernetesWorkloadObserver {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }
}

fn backend(error: KubernetesError) -> ObserverError {
    ObserverError::BackendError {
        cause: error.to_string(),
    }
}

fn worker_id_of(metadata: &ObjectMeta) -> Option<WorkerId> {
    metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(LABEL_WORKER_ID))
        .and_then(|value| WorkerId::try_from(value.as_str()).ok())
}

fn owned_by(metadata: &ObjectMeta, kind: &str) -> bool {
    metadata
        .owner_references
        .as_ref()
        .map(|owners| owners.iter().any(|owner| owner.kind == kind))
        .unwrap_or(false)
}

fn owner_job_name(metadata: &ObjectMeta) -> Option<&str> {
    metadata
        .owner_references
        .as_ref()
        .and_then(|owners| owners.iter().find(|owner| owner.kind == "Job"))
        .map(|owner| owner.name.as_str())
}

fn at_time(time: Option<&k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>) -> Option<AtTime> {
    time.and_then(|t| {
        let dt: DateTime<Utc> = t.0;
        AtTime::try_from(dt).ok()
    })
}

fn creation(metadata: &ObjectMeta) -> Option<AtTime> {
    at_time(metadata.creation_timestamp.as_ref())
}

fn main_container_started(pod: &Pod) -> Option<AtTime> {
    pod.status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|containers| {
            containers.iter().find_map(|container| {
                container
                    .state
                    .as_ref()
                    .and_then(|state| state.running.as_ref())
                    .and_then(|running| at_time(running.started_at.as_ref()))
            })
        })
}

fn unready_since(pod: &Pod) -> Option<AtTime> {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Ready" && condition.status != "True")
        })
        .and_then(|condition| at_time(condition.last_transition_time.as_ref()))
}

fn pod_phase(pod: &Pod) -> (WorkloadPhase, Option<AtTime>, Option<String>) {
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    let message = pod
        .status
        .as_ref()
        .and_then(|status| status.message.clone());
    match phase {
        Some(POD_PHASE_PENDING) => (WorkloadPhase::Pending, creation(&pod.metadata), message),
        Some(POD_PHASE_RUNNING) => (
            WorkloadPhase::Running,
            main_container_started(pod).or_else(|| creation(&pod.metadata)),
            message,
        ),
        Some(POD_PHASE_SUCCEEDED) => (WorkloadPhase::Succeeded, None, message),
        Some(POD_PHASE_FAILED) => (WorkloadPhase::Failed, None, message),
        // Dated from unreadiness, never creation: a long-lived pod must not
        // look "long Unknown" on the first blip.
        _ => (WorkloadPhase::Unknown, unready_since(pod), message),
    }
}

fn job_failed_condition(job: &Job) -> Option<(Option<AtTime>, Option<String>)> {
    job.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Failed" && condition.status == "True")
        })
        .map(|condition| {
            (
                at_time(condition.last_transition_time.as_ref()),
                condition.message.clone(),
            )
        })
}

fn observe_job(job: &Job, pods_by_owner: &HashMap<String, &Pod>) -> ObservedWorkload {
    let name = job.metadata.name.clone().unwrap_or_default();
    let status = job.status.as_ref();
    let (phase, phase_since, finished_at, message) =
        if status.and_then(|s| s.succeeded).unwrap_or(0) > 0 {
            (
                WorkloadPhase::Succeeded,
                None,
                at_time(status.and_then(|s| s.completion_time.as_ref())),
                None,
            )
        } else if let Some((finished_at, message)) = job_failed_condition(job) {
            (WorkloadPhase::Failed, None, finished_at, message)
        } else if status.and_then(|s| s.failed).unwrap_or(0) > 0 {
            // backoffLimit 0: a failed pod is terminal before the Failed
            // condition materializes.
            (WorkloadPhase::Failed, None, None, None)
        } else if let Some(pod) = pods_by_owner.get(&name) {
            let (phase, phase_since, message) = pod_phase(pod);
            (phase, phase_since, None, message)
        } else {
            (WorkloadPhase::Pending, creation(&job.metadata), None, None)
        };
    ObservedWorkload {
        worker_id: worker_id_of(&job.metadata),
        kind: WorkloadKind::Job,
        name,
        phase,
        phase_since,
        finished_at,
        message,
    }
}

#[async_trait]
impl WorkloadObserver for KubernetesWorkloadObserver {
    async fn observe(&self) -> Result<WorkloadObservation, ObserverError> {
        let jobs = list_jobs(&self.context).await.map_err(backend)?;
        let cronjobs = list_cronjobs(&self.context).await.map_err(backend)?;
        let pods = list_pods(&self.context).await.map_err(backend)?;

        let pods_by_owner: HashMap<String, &Pod> = pods
            .iter()
            .filter_map(|pod| owner_job_name(&pod.metadata).map(|owner| (owner.to_owned(), pod)))
            .collect();

        let mut workloads = Vec::new();

        for cronjob in &cronjobs {
            let Some(worker_id) = worker_id_of(&cronjob.metadata) else {
                continue;
            };
            workloads.push(observe_cronjob(cronjob, worker_id));
        }

        for job in &jobs {
            // A CronJob's child jobs (and their pods) fold into the CronJob.
            if owned_by(&job.metadata, "CronJob") {
                continue;
            }
            let observed = observe_job(job, &pods_by_owner);
            if observed.worker_id.is_some() {
                workloads.push(observed);
            }
        }

        for pod in &pods {
            // Job-owned pods folded above; bare pods are workloads themselves.
            if owner_job_name(&pod.metadata).is_some() {
                continue;
            }
            let Some(worker_id) = worker_id_of(&pod.metadata) else {
                continue;
            };
            let (phase, phase_since, message) = pod_phase(pod);
            workloads.push(ObservedWorkload {
                worker_id: Some(worker_id),
                kind: WorkloadKind::Pod,
                name: pod.metadata.name.clone().unwrap_or_default(),
                phase,
                phase_since,
                finished_at: None,
                message,
            });
        }

        Ok(WorkloadObservation { workloads })
    }

    async fn remove(&self, kind: &WorkloadKind, name: &str) -> Result<(), ObserverError> {
        match kind {
            WorkloadKind::Pod => delete_pod(&self.context, name).await.map_err(backend),
            WorkloadKind::Job => delete_job(&self.context, name).await.map_err(backend),
            WorkloadKind::CronJob => delete_cronjob(&self.context, name).await.map_err(backend),
            WorkloadKind::Deployment => delete_deployment(&self.context, name)
                .await
                .map_err(backend),
        }
    }
}

fn observe_cronjob(cronjob: &CronJob, worker_id: WorkerId) -> ObservedWorkload {
    ObservedWorkload {
        worker_id: Some(worker_id),
        kind: WorkloadKind::CronJob,
        name: cronjob.metadata.name.clone().unwrap_or_default(),
        phase: WorkloadPhase::Present,
        phase_since: creation(&cronjob.metadata),
        finished_at: None,
        message: None,
    }
}
