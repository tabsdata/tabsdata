//
// Copyright 2025 Tabsdata Inc.
//

use td_error::TdError;
use td_execution::SchedulerNotify;
use td_tower::extractors::Ref;
use tracing::instrument;

#[instrument(name = "scheduler.notify_scheduler", skip_all, level = "debug")]
pub async fn notify_scheduler(notify: Ref<SchedulerNotify>) -> Result<(), TdError> {
    notify.notify_scheduler();
    Ok(())
}
