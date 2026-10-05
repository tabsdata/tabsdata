//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use humantime::format_duration;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Resource as KubeResource;
use kube::core::ErrorResponse;
use kube::runtime::wait::{Condition, Error as WaitError, await_condition};
use kube::runtime::watcher::Error as WatcherError;
use serde::de::DeserializeOwned;
use tokio::time::{Instant, timeout};
use tracing::{debug, info};

use crate::base::constants::{JOB_CONDITION_FAILED, JOB_CONDITION_STATUS_TRUE, POD_PHASE_FAILED};
use crate::client::tdkserver::common::confirm_resume;
use crate::kube::api::domain::diagnostics::report_workload;
use crate::kube::context::KubeContext;

const WATCH_EXPIRED_REASON: &str = "Expired";

const WATCH_FAILURE_STATUS: &str = "Failure";

const WATCH_TIMEOUT_REASON: &str = "Timeout";

pub const AWAIT_CONDITION_TIMEOUT: Duration = Duration::from_secs(15 * 60);

const AWAIT_CONDITION_HEARTBEAT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwaitGoal {
    Presence,
    Absence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwaitPatience {
    Bounded,
    Resumable,
    Forever,
}

pub trait Failable {
    fn has_failed(&self) -> bool;
}

impl Failable for Job {
    fn has_failed(&self) -> bool {
        self.status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.type_ == JOB_CONDITION_FAILED
                        && condition.status == JOB_CONDITION_STATUS_TRUE
                })
            })
    }
}

impl Failable for Pod {
    fn has_failed(&self) -> bool {
        self.status
            .as_ref()
            .and_then(|status| status.phase.as_deref())
            == Some(POD_PHASE_FAILED)
    }
}

impl Failable for Deployment {
    fn has_failed(&self) -> bool {
        false
    }
}

impl Failable for CronJob {
    fn has_failed(&self) -> bool {
        false
    }
}

pub async fn await_condition_with_patience<K>(
    context: &KubeContext,
    api: Api<K>,
    name: &str,
    condition: impl Condition<K> + Clone,
    patience: AwaitPatience,
    goal: AwaitGoal,
) -> Result<Option<K>, WaitError>
where
    K: Clone + Debug + DeserializeOwned + KubeResource + Failable + Send + 'static,
{
    let settled = await_condition_inner(api, name, condition, patience).await;
    let failed = match (&settled, goal) {
        (Err(_), _) => true,
        (Ok(None), AwaitGoal::Presence) => true,
        (Ok(None), AwaitGoal::Absence) => false,
        (Ok(Some(resource)), _) => resource.has_failed(),
    };
    if failed {
        report_workload(context, name).await;
    }
    settled
}

async fn await_condition_inner<K>(
    api: Api<K>,
    name: &str,
    condition: impl Condition<K> + Clone,
    patience: AwaitPatience,
) -> Result<Option<K>, WaitError>
where
    K: Clone + Debug + DeserializeOwned + KubeResource + Send + 'static,
{
    let started = Instant::now();
    let mut budget = AWAIT_CONDITION_TIMEOUT;
    loop {
        let waiting = await_condition(api.clone(), name, condition.clone());
        let remaining = match patience {
            AwaitPatience::Bounded | AwaitPatience::Resumable => {
                Some(budget.saturating_sub(started.elapsed()))
            }
            AwaitPatience::Forever => None,
        };
        let outcome = match heartbeat(waiting, name, started, remaining).await {
            Some(outcome) => outcome,
            None => {
                if patience == AwaitPatience::Resumable
                    && confirm_resume(&format!("'{name}'"), started.elapsed()).await
                {
                    budget = started.elapsed() + AWAIT_CONDITION_TIMEOUT;
                    continue;
                }
                return Err(WaitError::ProbeFailed(WatcherError::WatchError(
                    ErrorResponse {
                        status: WATCH_FAILURE_STATUS.to_owned(),
                        message: format!(
                            "The wait for '{name}' exceeded the limit of {}",
                            format_duration(AWAIT_CONDITION_TIMEOUT)
                        ),
                        reason: WATCH_TIMEOUT_REASON.to_owned(),
                        code: 504,
                    },
                )));
            }
        };
        match outcome {
            Err(WaitError::ProbeFailed(WatcherError::WatchError(response)))
                if response.reason == WATCH_EXPIRED_REASON
                    && patience != AwaitPatience::Bounded =>
            {
                debug!("The watch on '{name}' expired; re-establishing it");
            }
            outcome => return outcome,
        }
    }
}

async fn heartbeat<F, T>(
    waiting: F,
    name: &str,
    started: Instant,
    budget: impl Into<Option<Duration>>,
) -> Option<T>
where
    F: Future<Output = T>,
{
    let budget = budget.into();
    let deadline = budget.map(|budget| Instant::now() + budget);
    let mut waiting = pin!(waiting);
    loop {
        let tick = match deadline {
            Some(deadline) => {
                AWAIT_CONDITION_HEARTBEAT.min(deadline.saturating_duration_since(Instant::now()))
            }
            None => AWAIT_CONDITION_HEARTBEAT,
        };
        match timeout(tick, &mut waiting).await {
            Ok(outcome) => return Some(outcome),
            Err(_) => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return None;
                }
                info!(
                    "Awaiting '{name}'. Elapsed time:{}",
                    format_duration(Duration::from_secs(started.elapsed().as_secs()))
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
