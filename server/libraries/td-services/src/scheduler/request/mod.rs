//
// Copyright 2025 Tabsdata Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

mod cron_trigger;
mod function_runner;
mod stage_trigger;
mod stop;

pub(crate) use cron_trigger::put_cron_trigger_start;
pub use cron_trigger::{
    create_locked_cron_trigger_workers_start_request,
    create_locked_cron_trigger_workers_stop_request,
};
pub use function_runner::{
    WorkerMessages, create_locked_function_runner_workers_start_request, enrich_minted_dispatch,
    fetch_lfr_to_dispatch, fetch_pfrs_to_dispatch, lfr_ids_from_minted_dispatch,
    materialize_replay_dispatch, pfr_ids_from_dispatched_workers, put_worker_messages,
};
pub(crate) use stage_trigger::put_stage_trigger_start;
pub use stage_trigger::{
    create_locked_stage_trigger_workers_start_request,
    create_locked_stage_trigger_workers_stop_request,
};
pub use stop::stop_worker_update;
