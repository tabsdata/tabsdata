//
// Copyright 2026 Tabsdata Inc.
//

//! Without a workload observer, recovery sweeps attempts stuck Dispatched
//! past the timeout. An attempt it ends must take its worker and its pod
//! with it, or the pod runs on for an attempt nothing expects.

use crate::Services;
use crate::scheduler::test_wiring::{dispatch_services, sending_to};
use crate::tests::fixtures::dispatched_publisher;
use std::time::Duration;
use ta_services::service::Ctx;
use td_bridge::Orchestrator;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::messaging::{FunctionRequest, OrchestratorMessagePayload};
use td_bridge::test::TestBridgeProvider;
use td_database::pool::TdPool;
use td_error::TdError;
use td_execution::{RecoveryConfig, StuckRequestedTimeout};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{LogicalFunctionRunStatus, PhysicalFunctionRunStatus, WorkerStatus};
use td_queries::Queries;

fn sweeping(db: &TdPool) -> (Services, InMemoryOrchestratorMessageQueue) {
    let mut services = dispatch_services(db);
    services.recovery_config = Ctx::new(RecoveryConfig {
        stuck_requested_timeout: StuckRequestedTimeout(Duration::ZERO),
        ..RecoveryConfig::default()
    });
    sending_to(
        services,
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    )
}

fn function_stops(sent: &InMemoryOrchestratorMessageQueue) -> Vec<String> {
    sent.messages()
        .into_iter()
        .filter(|message| {
            matches!(
                message.payload,
                OrchestratorMessagePayload::OrchestratorFunctionRequestMessagePayload(
                    FunctionRequest::Stop(_)
                )
            )
        })
        .map(|message| message.id)
        .collect()
}

#[td_test::test(db)]
async fn test_a_swept_attempt_fails_its_worker_and_stops_its_pod(
    db: TdPool,
) -> Result<(), TdError> {
    let (_, attempt) = dispatched_publisher(&db, "c_swept", "f_swept").await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let [worker] = <[_; 1]>::try_from(workers).expect("one worker");

    let (services, sent) = sweeping(&db);
    services.recovery(()).await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    assert_eq!(after.base.status, PhysicalFunctionRunStatus::Failed);
    let run: LogicalFunctionRunDB = db.fetch_one(&attempt.base.logical_function_run_id).await?;
    assert_eq!(run.base.status, LogicalFunctionRunStatus::ReScheduled);
    let worker_after: WorkerDB = db.fetch_one(&worker.base.id).await?;
    assert_eq!(worker_after.base.status, WorkerStatus::Failed);
    assert!(worker_after.base.ended_on.is_some());
    assert_eq!(function_stops(&sent), vec![worker.base.id.to_string()]);
    Ok(())
}
