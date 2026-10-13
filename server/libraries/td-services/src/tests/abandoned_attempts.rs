//
// Copyright 2026 Tabsdata Inc.
//

//! Recovering a run whose attempt never reported ends that attempt's worker
//! and stops its workload.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use crate::scheduler::test_wiring::sending_to;
use td_bridge::Orchestrator;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::messaging::{FunctionRequest, OrchestratorMessagePayload};
use td_bridge::test::TestBridgeProvider;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionParam;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, TableNameDto, TransactionStatus, UserId,
    WorkerId, WorkerStatus,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn requested() -> Result<Vec<TestExecution>, TdError> {
    Ok(vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from("f_0")?,
                dependencies: vec![],
                tables: vec![TableNameDto::try_from("t_0")?],
                initial_status: LogicalFunctionRunStatus::Requested,
                expected_status: LogicalFunctionRunStatus::ReScheduled,
            }],
        }],
    }])
}

fn recover_services(db: &TdPool) -> (Services, InMemoryOrchestratorMessageQueue) {
    sending_to(
        Services::with_defaults(db.clone()),
        TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
    )
}

fn stopped(sent: &InMemoryOrchestratorMessageQueue) -> Vec<String> {
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

async fn assert_ended(
    db: &TdPool,
    sent: &InMemoryOrchestratorMessageQueue,
    attempt: &PhysicalFunctionRunId,
    worker: &WorkerId,
) -> Result<(), TdError> {
    let ended: PhysicalFunctionRunDB = db.timeline().fetch_one(attempt).await?;
    assert_eq!(ended.base.status, PhysicalFunctionRunStatus::Failed);
    let worker_row: WorkerDB = db.fetch_one(worker).await?;
    assert_eq!(
        worker_row.base.status,
        WorkerStatus::Failed,
        "the abandoned attempt's worker was left behind"
    );
    assert_eq!(stopped(sent), vec![worker.to_string()]);
    Ok(())
}

#[td_test::test(db)]
async fn test_recovering_a_transaction_ends_the_abandoned_attempts_worker(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = requested()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, _, t, f, w| {
        let db = db.clone();
        let test_transaction = &test_executions[0].transactions[0];
        let function = &test_transaction.functions[0];
        let transaction = t[test_transaction].base.id.clone();
        let attempt = f[function].base.id.clone();
        let worker = w[function].base.id.clone();
        async move {
            let (services, sent) = recover_services(&db);
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(TransactionParam { transaction }, ());
            services.transaction_recover(request).await?;
            assert_ended(&db, &sent, &attempt, &worker).await
        }
    })
    .await
    .map(|_| ())
}

#[td_test::test(db)]
async fn test_recovering_an_execution_ends_the_abandoned_attempts_worker(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = requested()?;
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, _, f, w| {
        let db = db.clone();
        let function = &test_executions[0].transactions[0].functions[0];
        let execution = e[&test_executions[0]].base.id.clone();
        let attempt = f[function].base.id.clone();
        let worker = w[function].base.id.clone();
        async move {
            let (services, sent) = recover_services(&db);
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(ExecutionParam { execution }, ());
            services.execution_recover(request).await?;
            assert_ended(&db, &sent, &attempt, &worker).await
        }
    })
    .await
    .map(|_| ())
}
