//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use td_bridge::observer::{
    ObservedWorkload, ObserverError, WorkloadKind, WorkloadObservation, WorkloadObserver,
    WorkloadPhase,
};
use td_objects::types::basic::{AtTime, WorkerId};

use crate::base::constants::{
    DECORATOR_CRON_PUBLISHER, DECORATOR_PUBLISHER, DECORATOR_STREAM_PUBLISHER,
    DECORATOR_SUBSCRIBER, DECORATOR_TRANSFORMER, JOB_CONDITION_FAILED, JOB_CONDITION_STATUS_TRUE,
    KUBE_KIND_CRONJOB, KUBE_KIND_JOB, POD_CONDITION_READY, POD_CONDITION_STATUS_TRUE,
    POD_PHASE_FAILED, POD_PHASE_PENDING, POD_PHASE_RUNNING, POD_PHASE_SUCCEEDED,
    POD_YAML_DECORATOR_LABEL,
};
use crate::base::labels::LABEL_WORKER_ID;
use crate::kube::api::cronjob::{delete_cronjob, list_cronjobs_of_workers};
use crate::kube::api::job::{delete_job, list_jobs_of_workers};
use crate::kube::api::pod::{delete_pod, list_pods_of_workers};
use crate::kube::context::KubeContext;

pub struct KubernetesWorkloadObserver {
    context: KubeContext,
}

impl KubernetesWorkloadObserver {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }
}

fn at_time(time: Option<&Time>) -> Option<AtTime> {
    time.map(|stamp| AtTime::from(stamp.0))
}

trait WorkloadMetadata {
    fn name(&self) -> String;
    fn label(&self, label: &str) -> Option<&str>;
    fn unmapped(&self, label: &str, value: Option<&str>) -> ObserverError;
    fn worker_id(&self) -> Result<WorkerId, ObserverError>;
    fn workload_kind(&self) -> Result<WorkloadKind, ObserverError>;
    fn created(&self) -> Option<AtTime>;
    fn owned_by(&self, kind: &str) -> bool;
    fn owner_name(&self, kind: &str) -> Option<&str>;
}

impl WorkloadMetadata for ObjectMeta {
    fn name(&self) -> String {
        self.name.clone().unwrap_or_default()
    }

    fn label(&self, label: &str) -> Option<&str> {
        self.labels
            .as_ref()
            .and_then(|labels| labels.get(label))
            .map(String::as_str)
    }

    fn unmapped(&self, label: &str, value: Option<&str>) -> ObserverError {
        ObserverError::UnmappedWorkload {
            workload: self.name(),
            label: label.to_owned(),
            value: value.unwrap_or_default().to_owned(),
        }
    }

    fn worker_id(&self) -> Result<WorkerId, ObserverError> {
        let value = self.label(LABEL_WORKER_ID);
        value
            .and_then(|value| WorkerId::try_from(value).ok())
            .ok_or_else(|| self.unmapped(LABEL_WORKER_ID, value))
    }

    fn workload_kind(&self) -> Result<WorkloadKind, ObserverError> {
        match self.label(POD_YAML_DECORATOR_LABEL) {
            Some(DECORATOR_PUBLISHER) => Ok(WorkloadKind::Function),
            Some(DECORATOR_SUBSCRIBER) => Ok(WorkloadKind::Function),
            Some(DECORATOR_TRANSFORMER) => Ok(WorkloadKind::Function),
            Some(DECORATOR_CRON_PUBLISHER) => Ok(WorkloadKind::CronTrigger),
            Some(DECORATOR_STREAM_PUBLISHER) => Ok(WorkloadKind::StageTrigger),
            other => Err(self.unmapped(POD_YAML_DECORATOR_LABEL, other)),
        }
    }

    fn created(&self) -> Option<AtTime> {
        at_time(self.creation_timestamp.as_ref())
    }

    fn owned_by(&self, kind: &str) -> bool {
        self.owner_name(kind).is_some()
    }

    fn owner_name(&self, kind: &str) -> Option<&str> {
        self.owner_references
            .as_ref()
            .and_then(|owners| owners.iter().find(|owner| owner.kind == kind))
            .map(|owner| owner.name.as_str())
    }
}

trait PodProgress {
    fn phase(&self) -> WorkloadPhase;
    fn phase_since(&self) -> Option<AtTime>;
    fn message(&self) -> Option<String>;
    fn started(&self) -> Option<AtTime>;
    fn unready_since(&self) -> Option<AtTime>;
}

impl PodProgress for Pod {
    fn phase(&self) -> WorkloadPhase {
        match self
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref())
        {
            Some(POD_PHASE_PENDING) => WorkloadPhase::Pending,
            Some(POD_PHASE_RUNNING) => WorkloadPhase::Running,
            Some(POD_PHASE_SUCCEEDED) => WorkloadPhase::Succeeded,
            Some(POD_PHASE_FAILED) => WorkloadPhase::Failed,
            _ => WorkloadPhase::Unknown,
        }
    }

    fn phase_since(&self) -> Option<AtTime> {
        match self.phase() {
            WorkloadPhase::Pending => self.metadata.created(),
            WorkloadPhase::Running => self.started().or_else(|| self.metadata.created()),
            WorkloadPhase::Unknown => self.unready_since(),
            _ => None,
        }
    }

    fn message(&self) -> Option<String> {
        self.status
            .as_ref()
            .and_then(|status| status.message.clone())
    }

    fn started(&self) -> Option<AtTime> {
        self.status
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

    fn unready_since(&self) -> Option<AtTime> {
        self.status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.type_ == POD_CONDITION_READY
                        && condition.status != POD_CONDITION_STATUS_TRUE
                })
            })
            .and_then(|condition| at_time(condition.last_transition_time.as_ref()))
    }
}

trait JobOutcome {
    fn succeeded(&self) -> bool;
    fn has_failed_pod(&self) -> bool;
    fn completed_at(&self) -> Option<AtTime>;
    fn failure(&self) -> Option<(Option<AtTime>, Option<String>)>;
}

impl JobOutcome for Job {
    fn succeeded(&self) -> bool {
        self.status
            .as_ref()
            .and_then(|status| status.succeeded)
            .unwrap_or(0)
            > 0
    }

    fn has_failed_pod(&self) -> bool {
        self.status
            .as_ref()
            .and_then(|status| status.failed)
            .unwrap_or(0)
            > 0
    }

    fn completed_at(&self) -> Option<AtTime> {
        at_time(
            self.status
                .as_ref()
                .and_then(|status| status.completion_time.as_ref()),
        )
    }

    fn failure(&self) -> Option<(Option<AtTime>, Option<String>)> {
        self.status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.type_ == JOB_CONDITION_FAILED
                        && condition.status == JOB_CONDITION_STATUS_TRUE
                })
            })
            .map(|condition| {
                (
                    at_time(condition.last_transition_time.as_ref()),
                    condition.message.clone(),
                )
            })
    }
}

trait FromKubernetes: Sized {
    fn from_job(job: &Job, pods_by_owner: &HashMap<String, &Pod>) -> Result<Self, ObserverError>;
    fn from_cronjob(cronjob: &CronJob) -> Result<Self, ObserverError>;
    fn from_pod(pod: &Pod) -> Result<Self, ObserverError>;
}

impl FromKubernetes for ObservedWorkload {
    fn from_job(job: &Job, pods_by_owner: &HashMap<String, &Pod>) -> Result<Self, ObserverError> {
        let name = job.metadata.name();
        let (phase, phase_since, finished_at, message) = if job.succeeded() {
            (WorkloadPhase::Succeeded, None, job.completed_at(), None)
        } else if let Some((finished_at, message)) = job.failure() {
            (WorkloadPhase::Failed, None, finished_at, message)
        } else if job.has_failed_pod() {
            (WorkloadPhase::Failed, None, None, None)
        } else if let Some(pod) = pods_by_owner.get(&name) {
            (pod.phase(), pod.phase_since(), None, pod.message())
        } else {
            (WorkloadPhase::Pending, job.metadata.created(), None, None)
        };
        Ok(Self {
            worker_id: Some(job.metadata.worker_id()?),
            kind: job.metadata.workload_kind()?,
            name,
            phase,
            phase_since,
            finished_at,
            message,
        })
    }

    fn from_cronjob(cronjob: &CronJob) -> Result<Self, ObserverError> {
        Ok(Self {
            worker_id: Some(cronjob.metadata.worker_id()?),
            kind: cronjob.metadata.workload_kind()?,
            name: cronjob.metadata.name(),
            phase: WorkloadPhase::Present,
            phase_since: cronjob.metadata.created(),
            finished_at: None,
            message: None,
        })
    }

    fn from_pod(pod: &Pod) -> Result<Self, ObserverError> {
        Ok(Self {
            worker_id: Some(pod.metadata.worker_id()?),
            kind: pod.metadata.workload_kind()?,
            name: pod.metadata.name(),
            phase: pod.phase(),
            phase_since: pod.phase_since(),
            finished_at: None,
            message: pod.message(),
        })
    }
}

#[async_trait]
impl WorkloadObserver for KubernetesWorkloadObserver {
    async fn observe(&self) -> Result<WorkloadObservation, ObserverError> {
        let jobs = list_jobs_of_workers(&self.context).await?;
        let cronjobs = list_cronjobs_of_workers(&self.context).await?;
        let pods = list_pods_of_workers(&self.context).await?;

        let pods_by_owner: HashMap<String, &Pod> = pods
            .iter()
            .filter_map(|pod| {
                pod.metadata
                    .owner_name(KUBE_KIND_JOB)
                    .map(|owner| (owner.to_owned(), pod))
            })
            .collect();

        let mut workloads = Vec::new();

        for cronjob in &cronjobs {
            workloads.push(ObservedWorkload::from_cronjob(cronjob)?);
        }

        for job in &jobs {
            if job.metadata.owned_by(KUBE_KIND_CRONJOB) {
                continue;
            }
            workloads.push(ObservedWorkload::from_job(job, &pods_by_owner)?);
        }

        for pod in &pods {
            if pod.metadata.owned_by(KUBE_KIND_JOB) {
                continue;
            }
            workloads.push(ObservedWorkload::from_pod(pod)?);
        }

        Ok(WorkloadObservation { workloads })
    }

    async fn remove(&self, kind: &WorkloadKind, name: &str) -> Result<(), ObserverError> {
        match kind {
            WorkloadKind::CronTrigger => delete_cronjob(&self.context, name).await?,
            WorkloadKind::Function | WorkloadKind::StageTrigger => {
                delete_job(&self.context, name).await?;
                delete_pod(&self.context, name).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
