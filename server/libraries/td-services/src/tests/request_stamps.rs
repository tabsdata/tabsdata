//
// Copyright 2026 Tabsdata Inc.
//

//! A cancel or recover stamps the attempts it ends at its own unit's lock
//! instant, never at the request's arrival: a unit that waits on, or retries
//! after, another write must not end an attempt before that write.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::dispatched_publisher;
use crate::tests::tick_retries::{hold_a_write, release_once_waited_on};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionParam;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{AccessTokenId, AtTime, UserId};
use td_queries::Queries;

const COLLECTION: &str = "c_request_stamps";
const FUNCTION: &str = "f_request_stamps";

enum Request {
    CancelExecution,
    CancelTransaction,
    RecoverExecution,
}

/// Sends `request` for a dispatched attempt while another unit holds a write
/// on its worker, and checks the attempt and the worker end no earlier than
/// that write committed.
async fn assert_ends_after_the_write_it_waited_on(
    db: TdPool,
    request: Request,
) -> Result<(), TdError> {
    let (project, attempt) = dispatched_publisher(&db, COLLECTION, FUNCTION).await?;
    let workers: Vec<WorkerDB> = db.fetch_all(&attempt.base.id).await?;
    let [worker] = <[_; 1]>::try_from(workers).expect("one worker");
    let run: LogicalFunctionRunDB = db.fetch_one(&attempt.base.logical_function_run_id).await?;
    let context = RequestContext::with(AccessTokenId::default(), UserId::admin());
    let services: Services = dispatch_services(&db);

    let gate = hold_a_write(&db, &project, "workers", worker.base.id.as_i64()).await?;
    let sending = tokio::spawn(async move {
        match request {
            Request::CancelExecution => {
                let execution = run.base.execution_id;
                services
                    .execution_cancel(context.update(ExecutionParam { execution }, ()))
                    .await
            }
            Request::CancelTransaction => {
                let transaction = run.base.transaction_id;
                services
                    .transaction_cancel(context.update(TransactionParam { transaction }, ()))
                    .await
            }
            Request::RecoverExecution => {
                let execution = run.base.execution_id;
                services
                    .execution_recover(context.update(ExecutionParam { execution }, ()))
                    .await
            }
        }
        .map(|_| ())
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let committed = AtTime::now();
    release_once_waited_on(gate, sending).await?;

    let after: PhysicalFunctionRunDB = db.timeline().fetch_one(&attempt.base.id).await?;
    let ended_on = after.base.ended_on.expect("an ended attempt");
    assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    let after: WorkerDB = db.fetch_one(&worker.base.id).await?;
    let ended_on = after.base.ended_on.expect("an ended worker");
    assert!(*ended_on >= committed, "{ended_on:?} < {committed:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_an_execution_cancel_ends_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    assert_ends_after_the_write_it_waited_on(db, Request::CancelExecution).await
}

#[td_test::test(db)]
async fn test_a_transaction_cancel_ends_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    assert_ends_after_the_write_it_waited_on(db, Request::CancelTransaction).await
}

#[td_test::test(db)]
async fn test_an_execution_recover_ends_after_the_write_it_waited_on(
    db: TdPool,
) -> Result<(), TdError> {
    assert_ends_after_the_write_it_waited_on(db, Request::RecoverExecution).await
}
