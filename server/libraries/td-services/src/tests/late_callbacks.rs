//
// Copyright 2026 Tabsdata Inc.
//

//! Callbacks that arrive after their attempt stopped counting: a pod that
//! gave up retrying, or one that outlived the attempt recovery replaced. They
//! must not move any row of the run they belonged to.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, test_status_update,
};
use crate::scheduler::test_wiring::dispatch_services;
use crate::tests::fixtures::register;
use std::str::FromStr;
use std::time::Duration;
use ta_services::service::Ctx;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::status::ExitStatus;
use td_common::worker::{MessageAction, ResponseMessagePayload};
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt;
use td_error::TdError;
use td_execution::{RecoveryConfig, StuckRequestedTimeout};
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionRequest;
use td_objects::dxo::execution_callback::ExecutionCallbackRequest;
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::{FunctionRunParam, PhysicalFunctionRunDB};
use td_objects::dxo::worker::{CallbackRequest, WorkerDB, WorkerParam};
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, ExecutionStatus, FunctionIdName, FunctionName,
    LogicalFunctionRunStatus, PhysicalFunctionRunId, PhysicalFunctionRunStatus, TableNameDto,
    TransactionStatus, UserId, WorkerId, WorkerStatus,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn callback(status: WorkerCallbackStatus) -> CallbackRequest {
    ResponseMessagePayload {
        action: MessageAction::Notify,
        start: Some(123),
        end: Some(456),
        status,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        context: None,
    }
}

pub(crate) async fn post_callback(
    db: &TdPool,
    function_run: &PhysicalFunctionRunId,
    worker: &WorkerId,
    status: WorkerCallbackStatus,
) -> Result<(), TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        (
            FunctionRunParam {
                function_run: function_run.clone(),
            },
            WorkerParam {
                worker: worker.clone(),
            },
        ),
        ExecutionCallbackRequest::report(callback(status)),
    );
    Services::with_defaults(db.clone())
        .execution_callback(request)
        .await
        .map(|_| ())
}

/// The pod's `Done` landed and committed the run, its response was
/// lost, and after 120 s of retries the pod posts `Failed`. The committed
/// run's attempt and worker must stay `Done`.
#[td_test::test(db)]
async fn test_trailing_failed_after_commit_leaves_attempt_and_worker_done(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Finished,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Committed,
            functions: vec![TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from("f_0")?,
                dependencies: vec![],
                tables: vec![TableNameDto::try_from("t_0")?],
                initial_status: LogicalFunctionRunStatus::Running,
                expected_status: LogicalFunctionRunStatus::Committed,
            }],
        }],
    }];
    test_status_update(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
        let db = db.clone();
        let test_function = &test_executions[0].transactions[0].functions[0];
        let function_run = f[test_function].base.id.clone();
        let worker = w[test_function].base.id.clone();
        async move {
            post_callback(&db, &function_run, &worker, WorkerCallbackStatus::Done).await?;
            post_callback(&db, &function_run, &worker, WorkerCallbackStatus::Failed).await?;

            let attempt: PhysicalFunctionRunDB = db.timeline().fetch_one(&function_run).await?;
            assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Done);
            let worker: WorkerDB = db.fetch_one(&worker).await?;
            assert_eq!(worker.base.status, WorkerStatus::Done);
            Ok(())
        }
    })
    .await
}

/// Recovery abandoned attempt A1. A1's pod was only slow: it reports
/// `Running` before the next tick mints A2, then `Done` after. A1 must stay
/// `Failed`, and its `Done` must not settle the run A2 now owns.
#[td_test::test(db)]
async fn test_late_callbacks_of_an_abandoned_attempt_do_not_revive_it(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c_late")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&collection, &register("f_late", None))
        .await?;
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c_late")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f_late")?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: Default::default(),
                override_disabled_trigger: Default::default(),
                skip_if_running: Default::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    let execution = dispatch_services(&db).execute_function(request).await?.data;
    dispatch_services(&db).request(()).await?;

    let attempts: Vec<PhysicalFunctionRunDB> = db
        .timeline()
        .fetch_all(&(&execution.manual_trigger))
        .await?;
    assert_eq!(attempts.len(), 1);
    let abandoned = attempts.into_iter().next().unwrap();
    let workers: Vec<WorkerDB> = db.fetch_all(&abandoned.base.id).await?;
    assert_eq!(workers.len(), 1);
    let abandoned_worker = workers.into_iter().next().unwrap();

    let mut recovery = dispatch_services(&db);
    recovery.recovery_config = Ctx::new(RecoveryConfig {
        stuck_requested_timeout: StuckRequestedTimeout(Duration::ZERO),
        ..RecoveryConfig::default()
    });
    recovery.recovery(()).await?;

    let abandoned_after_recovery: PhysicalFunctionRunDB =
        db.timeline().fetch_one(&abandoned.base.id).await?;
    assert_eq!(
        abandoned_after_recovery.base.status,
        PhysicalFunctionRunStatus::Failed
    );

    post_callback(
        &db,
        &abandoned.base.id,
        &abandoned_worker.base.id,
        WorkerCallbackStatus::Running,
    )
    .await?;
    let abandoned_after_running: PhysicalFunctionRunDB =
        db.timeline().fetch_one(&abandoned.base.id).await?;
    assert_eq!(
        abandoned_after_running.base.status,
        PhysicalFunctionRunStatus::Failed,
        "a late Running must not revive an abandoned attempt"
    );
    dispatch_services(&db).request(()).await?;

    post_callback(
        &db,
        &abandoned.base.id,
        &abandoned_worker.base.id,
        WorkerCallbackStatus::Done,
    )
    .await?;
    let run: LogicalFunctionRunDB = db
        .fetch_one(and!(
            &abandoned.base.execution_id,
            &abandoned.base.function_id
        ))
        .await?;
    assert_eq!(
        run.base.status,
        LogicalFunctionRunStatus::Requested,
        "the abandoned attempt's Done must not settle the run its successor owns"
    );
    Ok(())
}
