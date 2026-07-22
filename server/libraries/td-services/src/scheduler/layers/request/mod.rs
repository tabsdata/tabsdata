//
// Copyright 2025 Tabsdata Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

mod at_time;
mod cron_trigger;
mod function_runner;
mod notify;
mod shared;
mod stage_trigger;

pub use at_time::dispatch_at_time;
pub use cron_trigger::{
    create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
};
pub use function_runner::{
    create_locked_function_runner_workers_start_request, enrich_minted_dispatch,
    fetch_lfr_to_dispatch, fetch_pfrs_to_dispatch, lfr_ids_from_minted_dispatch,
    materialize_replay_dispatch, pfr_ids_from_minted_dispatch,
};
pub use notify::notify_scheduler;
pub use stage_trigger::{
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request,
};
