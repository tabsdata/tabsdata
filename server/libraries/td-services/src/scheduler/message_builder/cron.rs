//
// Copyright 2026 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::cron::{CronInfo, CronInput};
use td_objects::types::basic::{AtTime, WorkerConfig};

/// Build the lean k8s cron `request.yaml` from a cron-trigger worker:
/// identity + the cron `config` (schedule), no `task` callback (the pod
/// gets its trigger via env). Needs only the worker row -- no jwt, server
/// addresses, or storage -- so the resolve endpoint can call it directly.
pub(crate) fn build_cron_input(worker: &WorkerDBWithNames) -> Result<CronInput, TdError> {
    let info = CronInfo {
        worker_id: worker.id,
        collection_id: worker.collection_id,
        collection: worker.collection.clone(),
        function_version_id: worker.function_version_id.clone(),
        function: worker.function.clone(),
        scheduled_on: AtTime::now(),
        config: WorkerConfig::serialize(&worker.worker_config)?,
    };
    Ok(CronInput { info })
}
