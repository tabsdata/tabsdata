//
// Copyright 2025 Tabsdata Inc.
//

use super::function_runner::DispatchSaturated;
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_tower::extractors::{MutRef, Ref};
use td_tower::post_commit::PostCommitActions;
use tracing::{debug, instrument};

#[instrument(name = "scheduler.notify_scheduler", skip_all, level = "debug")]
pub async fn notify_scheduler(notify: Ref<SchedulerNotify>) -> Result<(), TdError> {
    notify.notify_scheduler();
    Ok(())
}

/// A tick that hit `max_functions_per_tick` re-notifies the scheduler once
/// its transaction commits, so the remaining backlog is picked up by an
/// immediate next tick instead of the failover timer.
#[instrument(
    name = "scheduler.notify_scheduler_if_saturated",
    skip_all,
    level = "debug"
)]
pub async fn notify_scheduler_if_saturated(
    saturated: MutRef<DispatchSaturated>,
    notify: Ref<SchedulerNotify>,
    mut post_commit: MutRef<PostCommitActions>,
) -> Result<(), TdError> {
    if saturated.0 {
        debug!("scheduler.dispatch_tick: capped; re-notifying the scheduler");
        let notify = (*notify).clone();
        post_commit.push(move || {
            Box::pin(async move {
                notify.notify_scheduler();
            })
        });
    }
    Ok(())
}
