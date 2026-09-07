//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdError;
use td_objects::dxo::worker::WorkerDBWithNames;
use td_objects::dxo::yaml::cron::{CronInfo, CronInput};
use td_objects::types::basic::{AtTime, WorkerConfig};

/// Build the lean k8s cron `request.yaml` from a cron-trigger worker:
/// identity + the cron `config` (schedule), no `task` callback (the pod
/// gets its trigger via env). Needs only the worker row -- no jwt, server
/// addresses, or storage -- so the resolve endpoint can call it directly.
pub(crate) fn build_cron_input(worker: WorkerDBWithNames) -> Result<CronInput, TdError> {
    let info = CronInfo {
        worker_id: worker.base.base.id,
        collection_id: worker.base.base.collection_id,
        collection: worker.collection,
        function_version_id: worker.base.base.function_version_id,
        function: worker.function,
        scheduled_on: AtTime::now(),
        config: WorkerConfig::serialize(&worker.base.base.worker_config)?,
    };
    Ok(CronInput { info })
}
