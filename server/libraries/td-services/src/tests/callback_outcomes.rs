//
// Copyright 2026 Tabsdata Inc.
//

//! A worker's report is answered `Applied` when it moved something and
//! `Stale` when it came too late to, so the worker stops reporting instead
//! of turning the refusal into a Failed report.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, test_status_update,
};
use crate::tests::fixtures::register;
use td_common::datetime::Timestamp;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::status::ExitStatus;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::callback::CallbackOutcome;
use td_objects::dxo::callback::CallbackRequest;
use td_objects::dxo::callback::ExecutionCallbackRequest;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::physical_function_run::{FunctionRunParam, PhysicalFunctionRunDB};
use td_objects::dxo::worker::{WorkerDB, WorkerParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, TableNameDto, TransactionStatus, UserId, WorkerId, WorkerStatus,
    WorkerType,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn report(status: WorkerCallbackStatus) -> CallbackRequest {
    report_between(status, 123, 456)
}

fn report_between(status: WorkerCallbackStatus, start: i64, end: i64) -> CallbackRequest {
    CallbackRequest {
        start: Some(AtTime::from(Timestamp::try_from(start).unwrap())),
        end: Some(AtTime::from(Timestamp::try_from(end).unwrap())),
        status,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        output: None,
    }
}

pub(crate) async fn report_attempt(
    db: &TdPool,
    function_run: &PhysicalFunctionRunId,
    worker: &WorkerId,
    status: WorkerCallbackStatus,
) -> Result<CallbackOutcome, TdError> {
    send(db, function_run, worker, report(status)).await
}

async fn send(
    db: &TdPool,
    function_run: &PhysicalFunctionRunId,
    worker: &WorkerId,
    report: CallbackRequest,
) -> Result<CallbackOutcome, TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        (
            FunctionRunParam {
                function_run: function_run.clone(),
            },
            WorkerParam {
                worker: worker.clone(),
            },
        ),
        ExecutionCallbackRequest::report(report),
    );
    let response = Services::with_defaults(db.clone())
        .execution_callback(request)
        .await?;
    Ok(response.data.outcome)
}

fn function(
    name: &str,
    table: &str,
    expected_status: LogicalFunctionRunStatus,
) -> Result<TestFunction, TdError> {
    Ok(TestFunction {
        collection: CollectionName::try_from("c_0")?,
        name: FunctionName::try_from(name)?,
        dependencies: vec![],
        tables: vec![TableNameDto::try_from(table)?],
        initial_status: LogicalFunctionRunStatus::Running,
        expected_status,
    })
}

#[td_test::test(db)]
async fn test_a_report_after_its_attempt_ended_is_stale(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Finished,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Committed,
            functions: vec![function("f_0", "t_0", LogicalFunctionRunStatus::Committed)?],
        }],
    }];
    test_status_update(
        db.clone(),
        p.project_id,
        &test_executions,
        |_, _, _, f, w| {
            let db = db.clone();
            let test_function = &test_executions[0].transactions[0].functions[0];
            let function_run = f[test_function].id.clone();
            let worker = w[test_function].id.clone();
            async move {
                let done = report_attempt(&db, &function_run, &worker, WorkerCallbackStatus::Done);
                assert_eq!(done.await?, CallbackOutcome::Applied);
                let again = report_attempt(&db, &function_run, &worker, WorkerCallbackStatus::Done);
                assert_eq!(again.await?, CallbackOutcome::Applied);
                let failed =
                    report_attempt(&db, &function_run, &worker, WorkerCallbackStatus::Failed);
                assert_eq!(failed.await?, CallbackOutcome::Stale);
                Ok(())
            }
        },
    )
    .await
}

/// A pod reporting the end its attempt already has, whoever ended it,
/// changes nothing: the attempt keeps the instants it ended with.
#[td_test::test(db)]
async fn test_an_end_reported_again_changes_nothing(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Stalled,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Stalled,
            functions: vec![function("f_0", "t_0", LogicalFunctionRunStatus::Failed)?],
        }],
    }];
    test_status_update(
        db.clone(),
        p.project_id,
        &test_executions,
        |_, _, _, f, w| {
            let db = db.clone();
            let test_function = &test_executions[0].transactions[0].functions[0];
            let function_run = f[test_function].id.clone();
            let worker = w[test_function].id.clone();
            async move {
                let failed = report_between(WorkerCallbackStatus::Failed, 1_000, 2_000);
                let first = send(&db, &function_run, &worker, failed).await?;
                assert_eq!(first, CallbackOutcome::Applied);
                let attempt: PhysicalFunctionRunDB = db.timeline().fetch_one(&function_run).await?;
                let runner: WorkerDB = db.fetch_one(&worker).await?;

                let late = report_between(WorkerCallbackStatus::Failed, 5_000, 6_000);
                let again = send(&db, &function_run, &worker, late).await?;

                assert_eq!(again, CallbackOutcome::Applied);
                let attempt_after: PhysicalFunctionRunDB =
                    db.timeline().fetch_one(&function_run).await?;
                assert_eq!(attempt_after.started_on, attempt.started_on);
                assert_eq!(attempt_after.ended_on, attempt.ended_on);
                let runner_after: WorkerDB = db.fetch_one(&worker).await?;
                assert_eq!(runner_after.started_on, runner.started_on);
                assert_eq!(runner_after.ended_on, runner.ended_on);
                Ok(())
            }
        },
    )
    .await
}

#[td_test::test(db)]
async fn test_a_worker_reporting_another_attempt_is_not_found(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![
                function("f_0", "t_0", LogicalFunctionRunStatus::Running)?,
                function("f_1", "t_1", LogicalFunctionRunStatus::Running)?,
            ],
        }],
    }];
    test_status_update(
        db.clone(),
        p.project_id,
        &test_executions,
        |_, _, _, f, w| {
            let db = db.clone();
            let functions = &test_executions[0].transactions[0].functions;
            let function_run = f[&functions[0]].id.clone();
            let other_worker = w[&functions[1]].id.clone();
            async move {
                let error = report_attempt(
                    &db,
                    &function_run,
                    &other_worker,
                    WorkerCallbackStatus::Done,
                )
                .await
                .expect_err("the worker runs another attempt");
                assert_eq!(error.api_error(), ApiError::NotFound);
                Ok(())
            }
        },
    )
    .await
}

#[td_test::test(db)]
async fn test_a_trigger_report_after_its_stop_is_stale(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
        .await?;
    let function = db
        .scoped(p.project_id)
        .seed()
        .function(&collection, &register("f_0", None))
        .await?;

    for (status, expected) in [
        (WorkerStatus::Requested, CallbackOutcome::Applied),
        (WorkerStatus::StopScheduled, CallbackOutcome::Stale),
    ] {
        let worker = db
            .scoped(p.project_id)
            .seed()
            .worker(&collection, &function, &WorkerType::CronTrigger, &status)
            .await?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
            WorkerParam {
                worker: worker.id.clone(),
            },
            report(WorkerCallbackStatus::Running),
        );
        let response = Services::with_defaults(db.clone())
            .worker_callback(request)
            .await?;
        assert_eq!(
            response.data.outcome, expected,
            "a Running report on {status:?}"
        );
    }
    Ok(())
}
