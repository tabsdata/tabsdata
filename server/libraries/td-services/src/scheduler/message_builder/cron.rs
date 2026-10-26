//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdError;
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames, WorkerInsertDB};
use td_objects::dxo::worker_input::cron::{CronInfo, CronInput};
use td_objects::types::basic::{AtTime, WorkerConfig};

/// Build the lean k8s cron `request.yaml` from a cron-trigger worker:
/// identity + the cron `config` (schedule), no `task` callback (the pod
/// gets its trigger via env). Needs only the worker row -- no jwt, server
/// addresses, or storage -- so the resolve endpoint can call it directly.
pub(crate) fn build_cron_input(worker: WorkerDBWithNames) -> Result<CronInput, TdError> {
    let WorkerDBWithNames {
        base:
            WorkerDB {
                base:
                    WorkerInsertDB {
                        id,
                        collection_id,
                        function_version_id,
                        worker_config,
                        ..
                    },
                ..
            },
        collection,
        function,
        ..
    } = worker;
    let info = CronInfo {
        worker_id: id,
        collection_id,
        collection: collection.name()?,
        function_version_id,
        function,
        scheduled_on: AtTime::now(),
        config: WorkerConfig::serialize(&worker_config)?,
    };
    Ok(CronInput { info })
}
