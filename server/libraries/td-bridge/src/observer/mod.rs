//
// Copyright 2026 Tabsdata Inc.
//

//! Workload observation: the read-side port the recovery loop reconciles
//! through. Orchestrators that manage worker liveness themselves expose no
//! observer, and recovery falls back to its time-based sweep.

use async_trait::async_trait;
use td_common::worker::WorkerName;
use td_error::td_error;
use td_objects::types::basic::{AtTime, FunctionId, FunctionName, WorkerId};

#[td_error]
pub enum ObserverError {
    #[error("Workload observation backend error: {cause}")]
    BackendError { cause: String } = 0,
    #[error(
        "Workload '{workload}' carries '{label}: {value}', which the observer has no mapping \
         for; it must be taught how to observe it"
    )]
    UnmappedWorkload {
        workload: String,
        label: String,
        value: String,
    } = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadKind {
    Function,
    CronTrigger,
    StageTrigger,
}

impl WorkloadKind {
    pub fn holds_a_schedule(self) -> bool {
        matches!(self, Self::CronTrigger)
    }
}

impl From<&WorkerName> for WorkloadKind {
    fn from(worker: &WorkerName) -> Self {
        match worker {
            WorkerName::Function => Self::Function,
            WorkerName::CronTrigger => Self::CronTrigger,
            WorkerName::StageTrigger => Self::StageTrigger,
        }
    }
}

/// `Present` is the phase-less state of schedule-holding workloads
/// (a cron trigger's runs come and go; the schedule itself has no phase).
///
/// `Suspended` and `Paused` are a workload created but deliberately not
/// started: the creation path applies it held, attaches its companions, then
/// releases it. A job or cron job spells that `suspend`, a deployment spells
/// it `paused`. One that stays held is a creation that never finished, not a
/// workload waiting to be scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Suspended,
    Paused,
    Unknown,
    Present,
}

#[derive(Debug, Clone)]
pub struct ObservedWorkload {
    /// `None` when the worker-id label is missing or does not round-trip.
    pub worker_id: Option<WorkerId>,
    /// `None` when the function-id label is missing or does not round-trip.
    pub function_id: Option<FunctionId>,
    /// `None` when the function annotation is missing or is not a valid name.
    pub function: Option<FunctionName>,
    pub kind: WorkloadKind,
    pub name: String,
    pub phase: WorkloadPhase,
    /// When the workload entered its current phase if the backend can date it.
    pub phase_since: Option<AtTime>,
    pub finished_at: Option<AtTime>,
    pub message: Option<String>,
    /// The backend is already deleting it.
    pub deleting: bool,
}

#[derive(Debug, Clone, Default)]
pub struct WorkloadObservation {
    pub workloads: Vec<ObservedWorkload>,
}

#[async_trait]
pub trait WorkloadObserver: Send + Sync {
    /// Workloads owned by another observed workload fold into their owner, not listed
    /// (e.g., the runs a cron trigger spawns).
    async fn observe(&self) -> Result<WorkloadObservation, ObserverError>;

    /// Idempotent: removing an already-gone workload succeeds.
    async fn remove(&self, kind: &WorkloadKind, name: &str) -> Result<(), ObserverError>;
}
