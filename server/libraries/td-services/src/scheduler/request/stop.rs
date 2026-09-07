//
// Copyright 2026 Tabsdata Inc.
//

use td_bridge::Orchestrator;
use td_error::TdError;
use td_objects::dxo::worker::UpdateWorkerDB;
use td_objects::types::basic::AtTime;

/// The stop-flip update for trigger workers. The kubernetes stop is
/// synchronous — the queue put deletes the workload and the gateway never
/// invokes the stop responder — so a worker whose stop message was enqueued is
/// terminal `Stopped`. The supervisor stop is asynchronous: the row waits in
/// `StopRequested` until the exiting worker's callback flips it to `Stopped`.
pub async fn stop_worker_update(
    orchestrator: Orchestrator,
    at: &AtTime,
) -> Result<UpdateWorkerDB, TdError> {
    match orchestrator {
        Orchestrator::Kubernetes => Ok(UpdateWorkerDB::stopped(at)),
        Orchestrator::Supervisor => Ok(UpdateWorkerDB::stop_requested()),
    }
}
