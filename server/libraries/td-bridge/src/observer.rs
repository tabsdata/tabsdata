//
// Copyright 2026 Tabsdata Inc.
//

//! Workload observation: the read-side port the recovery loop reconciles
//! through. Orchestrators that manage worker liveness themselves expose no
//! observer and recovery falls back to its time-based sweep.

use async_trait::async_trait;
use td_error::td_error;
use td_objects::types::basic::{AtTime, WorkerId};

#[td_error]
pub enum ObserverError {
    #[error("Workload observation backend error: {cause}")]
    BackendError { cause: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Pod,
    Job,
    CronJob,
    Deployment,
}

/// `Present` is the phase-less state of schedule-holding workloads (a
/// CronJob's children run and finish; it has no phase of its own).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
    Present,
}

#[derive(Debug, Clone)]
pub struct ObservedWorkload {
    /// `None` when the worker-id label is missing or does not round-trip.
    pub worker_id: Option<WorkerId>,
    pub kind: WorkloadKind,
    pub name: String,
    pub phase: WorkloadPhase,
    /// When the workload entered its current phase, if the backend can date
    /// it.
    pub phase_since: Option<AtTime>,
    pub finished_at: Option<AtTime>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkloadObservation {
    pub workloads: Vec<ObservedWorkload>,
}

#[async_trait]
pub trait WorkloadObserver: Send + Sync {
    /// Workloads owned by another observed workload (e.g. a CronJob's
    /// children) fold into their owner, not listed.
    async fn observe(&self) -> Result<WorkloadObservation, ObserverError>;

    /// Idempotent: removing an already-gone workload succeeds.
    async fn remove(&self, kind: &WorkloadKind, name: &str) -> Result<(), ObserverError>;
}
