//
// Copyright 2026 Tabsdata Inc.
//

use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_error::TdError;
use td_objects::dxo::worker::UpdateWorkerDB;
use td_objects::types::basic::AtTime;
use td_tower::extractors::Ref;

/// The stop-flip update for trigger workers. The kubernetes stop is
/// synchronous — the queue put deletes the workload and the gateway never
/// invokes the stop responder — so a worker whose stop message was enqueued is
/// terminal `Stopped`. The supervisor stop is asynchronous: the row waits in
/// `StopRequested` until the exiting worker's callback flips it to `Stopped`.
pub async fn stop_worker_update(
    bridge: Ref<BridgeProviderHandle>,
    at: Ref<AtTime>,
) -> Result<UpdateWorkerDB, TdError> {
    match bridge.orchestrator() {
        Orchestrator::Kubernetes => UpdateWorkerDB::stopped(&at).await,
        Orchestrator::Supervisor => UpdateWorkerDB::stop_requested().await,
    }
}
