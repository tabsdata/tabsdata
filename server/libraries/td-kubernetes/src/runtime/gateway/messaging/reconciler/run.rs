//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::Arc;
use std::time::{Duration, Instant};

use humantime::format_duration;
use td_bridge::observer::WorkloadObserver;
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::kube::api::domain::worker::delete_worker;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::factory::WorkloadFactory;
use crate::runtime::gateway::messaging::reconciler::Reconciler;
use crate::runtime::gateway::messaging::reconciler::plan::{Action, FORCE_AFTER};
use crate::runtime::gateway::observer::KubernetesWorkloadObserver;

const BUSY_INTERVAL: Duration = Duration::from_secs(1);
const IDLE_INTERVAL: Duration = Duration::from_secs(30);
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn run(
    reconciler: Arc<Reconciler>,
    factory: Arc<WorkloadFactory>,
    observer: KubernetesWorkloadObserver,
    context: KubeContext,
) {
    let mut pause = Duration::ZERO;
    loop {
        let _ = timeout(pause, reconciler.wake.notified()).await;
        reconcile(&reconciler, &factory, &observer, &context).await;
        pause = if reconciler.state().is_idle() {
            IDLE_INTERVAL
        } else {
            BUSY_INTERVAL
        };
    }
}

async fn reconcile(
    reconciler: &Reconciler,
    factory: &WorkloadFactory,
    observer: &KubernetesWorkloadObserver,
    context: &KubeContext,
) {
    let observation = match timeout(CALL_TIMEOUT, observer.observe()).await {
        Ok(Ok(observation)) => observation,
        Ok(Err(error)) => {
            warn!("Could not observe the workloads; retrying: {error}");
            return;
        }
        Err(_) => {
            warn!("Observing the workloads timed out; retrying");
            return;
        }
    };
    let actions = reconciler
        .state()
        .plan(&observation.workloads, Instant::now());
    for action in actions {
        match action {
            Action::Delete { kind, name } => {
                let outcome = timeout(CALL_TIMEOUT, observer.remove(&kind, &name)).await;
                if !matches!(outcome, Ok(Ok(()))) {
                    warn!("Could not delete '{name}'; retrying: {outcome:?}");
                    reconciler.state().reissue(&name);
                }
            }
            Action::Warn { name, elapsed } => {
                warn!(
                    "The deletion of '{name}' has not completed after {}; the system keeps retrying",
                    format_duration(Duration::from_secs(elapsed.as_secs()))
                );
            }
            Action::Force { name, worker_id } => {
                warn!(
                    "The deletion of '{name}' has not completed after {}; forcing it",
                    format_duration(FORCE_AFTER)
                );
                let outcome = timeout(CALL_TIMEOUT, delete_worker(context, &worker_id)).await;
                if !matches!(outcome, Ok(Ok(()))) {
                    warn!("Could not force the deletion of '{name}': {outcome:?}");
                }
            }
            Action::Create { key, submission } => {
                let outcome = timeout(
                    CALL_TIMEOUT,
                    factory.create(&submission.worker_id, &submission.payload),
                )
                .await;
                if matches!(outcome, Ok(Ok(()))) {
                    info!("Created '{}'", submission.name);
                } else {
                    warn!(
                        "Could not create '{}'; retrying: {outcome:?}",
                        submission.name
                    );
                    if let Some(abandoned) = reconciler.state().restore(key, submission) {
                        error!(
                            "Giving up creating '{}' after {} attempts; recovery takes over its run",
                            abandoned.name, abandoned.attempts
                        );
                    }
                }
            }
        }
    }
}
