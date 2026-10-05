//
// Copyright 2026 Tabsdata Inc.
//

use td_bridge::Orchestrator;
use td_error::TdError;
use td_objects::dxo::worker::WorkerUpdateDB;
use td_objects::types::basic::AtTime;

/// The stop-flip update for trigger workers. The kubernetes stop is
/// put-and-forget — the queue put records the deletion, the bridge carries it
/// out, and the gateway never invokes the stop responder — so a worker whose
/// stop message was enqueued is terminal `Stopped`; recovery covers a deletion
/// that never lands. The supervisor stop is asynchronous: the row waits in
/// `StopRequested` until the exiting worker's callback flips it to `Stopped`.
pub fn stop_worker_update(
    orchestrator: Orchestrator,
    at: &AtTime,
) -> Result<WorkerUpdateDB, TdError> {
    match orchestrator {
        Orchestrator::Kubernetes => Ok(WorkerUpdateDB::stopped(at)),
        Orchestrator::Supervisor => Ok(WorkerUpdateDB::stop_requested()),
    }
}
