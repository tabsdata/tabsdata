//
// Copyright 2026 Tabsdata Inc.
//

//! Consumers and readers see committed data only: never an in-flight
//! replay's placeholder, never the output of a discarded run.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, test_status_update,
};
use td_common::execution_status::WorkerCallbackStatus;
use td_common::status::ExitStatus;
use td_common::worker::{MessageAction, ResponseMessagePayload};
use td_database::access::Grant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::execution_callback::ExecutionCallbackRequest;
use td_objects::dxo::logical::LogicalFunctionRunParam;
use td_objects::dxo::physical_function_run::FunctionRunParam;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::dxo::worker::{CallbackRequest, WorkerParam};
use td_objects::dxo::worker_input::WorkerOutput;
use td_objects::dxo::worker_input::function::{FunctionOutput, TableInfo, WrittenTable};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionName, ColumnCount, ExecutionStatus, FunctionName,
    LogicalFunctionRunStatus, PhysicalFunctionRunId, RowCount, SchemaHash, TableName, TableNameDto,
    TransactionStatus, UserId, WorkerId,
};
use td_queries::Queries;
use td_queries::test_utils::{SeedEntry, begin_exclusive};

fn done_without_data(table: &str) -> Result<CallbackRequest, TdError> {
    Ok(ResponseMessagePayload {
        action: MessageAction::Notify,
        start: Some(123),
        end: Some(456),
        status: WorkerCallbackStatus::Done,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        context: Some(WorkerOutput::Function(FunctionOutput {
            output: vec![WrittenTable::NoData {
                table: TableName::try_from(table)?,
            }],
        })),
    })
}

fn done_with_data(table: &str) -> Result<CallbackRequest, TdError> {
    Ok(ResponseMessagePayload {
        action: MessageAction::Notify,
        start: Some(123),
        end: Some(456),
        status: WorkerCallbackStatus::Done,
        error_message: None,
        exit_status: ExitStatus::Success.code(),
        context: Some(WorkerOutput::Function(FunctionOutput {
            output: vec![WrittenTable::Data {
                table: TableName::try_from(table)?,
                info: TableInfo {
                    column_count: ColumnCount::try_from(1i64)?,
                    row_count: RowCount::try_from(2i64)?,
                    schema_hash: SchemaHash::try_from("hash")?,
                    data_bytes: None,
                },
            }],
        })),
    })
}

async fn post_callback(
    db: &TdPool,
    function_run: &PhysicalFunctionRunId,
    worker: &WorkerId,
    callback: CallbackRequest,
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
        ExecutionCallbackRequest::report(callback),
    );
    Services::with_defaults(db.clone())
        .execution_callback(request)
        .await
        .map(|_| ())
}

/// A committed run wrote t_0 with data. Replaying it must not hide
/// that data while the replay is in flight, nor once the replay fails.
#[td_test::test(db)]
async fn test_replay_in_flight_keeps_committed_data_visible(db: TdPool) -> Result<(), TdError> {
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
        let attempt = f[test_function].clone();
        let worker = w[test_function].base.id.clone();
        async move {
            post_callback(&db, &attempt.base.id, &worker, done_with_data("t_0")?).await?;

            let written: Vec<PhysicalTableDataVersionDBWithNames> =
                db.timeline().fetch_all(&attempt.base.id).await?;
            let table_id = written
                .iter()
                .find(|v| v.base.base.base.name.as_str() == "t_0")
                .expect("the attempt wrote t_0")
                .base
                .base
                .base
                .table_id;
            let committed = db
                .table_data_versions()
                .committed_data_at(&table_id, &AtTime::now())
                .await?
                .expect("the committed run's data is visible")
                .base
                .base
                .base
                .id;
            let visible = || async {
                Ok::<_, TdError>(
                    db.table_data_versions()
                        .committed_data_at(&table_id, &AtTime::now())
                        .await?
                        .map(|v| v.base.base.base.id),
                )
            };

            let replay = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
                LogicalFunctionRunParam {
                    logical_function_run: attempt.base.logical_function_run_id.clone(),
                },
                (),
            );
            let replayed = Services::with_defaults(db.clone())
                .replay_logical_function_run(replay)
                .await?
                .data
                .new_function_run_id;
            assert_eq!(
                visible().await?,
                Some(committed.clone()),
                "an in-flight replay must not hide the committed data"
            );

            let mut trx = begin_exclusive(&db, p.id).await?;
            let at = trx.locked_at();
            trx.get(Grant::asserted("a replay attempt fails"))
                .physical_runs()
                .end(&[replayed], at)
                .await?;
            trx.commit().await?;
            assert_eq!(
                visible().await?,
                Some(committed),
                "a failed replay must leave the committed data current"
            );
            Ok(())
        }
    })
    .await
}

/// f_0 wrote t_0 with data, then its execution was canceled before
/// the transaction committed. A later run of f_0 that writes no data must
/// not point t_0 back at the discarded data.
#[td_test::test(db)]
async fn test_no_data_version_does_not_chase_into_canceled_data(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let t_0 = TableNameDto::try_from("t_0")?;
    let canceled_execution = vec![TestExecution {
        expected_status: ExecutionStatus::Finished,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Canceled,
            functions: vec![
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![t_0.clone()],
                    initial_status: LogicalFunctionRunStatus::Running,
                    expected_status: LogicalFunctionRunStatus::Canceled,
                },
                TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_1")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_1")?],
                    initial_status: LogicalFunctionRunStatus::Running,
                    expected_status: LogicalFunctionRunStatus::Canceled,
                },
            ],
        }],
    }];
    test_status_update(db.clone(), p.id, &canceled_execution, |_, e, _, f, w| {
        let db = db.clone();
        let test_function = &canceled_execution[0].transactions[0].functions[0];
        let attempt = f[test_function].base.id.clone();
        let worker = w[test_function].base.id.clone();
        let execution = e[&canceled_execution[0]].base.id.clone();
        async move {
            post_callback(&db, &attempt, &worker, done_with_data("t_0")?).await?;
            let cancel = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(ExecutionParam { execution }, ());
            Services::with_defaults(db.clone())
                .execution_cancel(cancel)
                .await?;
            Ok(())
        }
    })
    .await?;

    let later_execution = vec![TestExecution {
        expected_status: ExecutionStatus::Finished,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Committed,
            functions: vec![TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from("f_0")?,
                dependencies: vec![],
                tables: vec![t_0],
                initial_status: LogicalFunctionRunStatus::Running,
                expected_status: LogicalFunctionRunStatus::Committed,
            }],
        }],
    }];
    test_status_update(db.clone(), p.id, &later_execution, |_, _, _, f, w| {
        let db = db.clone();
        let test_function = &later_execution[0].transactions[0].functions[0];
        let attempt = f[test_function].base.id.clone();
        let worker = w[test_function].base.id.clone();
        async move {
            post_callback(&db, &attempt, &worker, done_without_data("t_0")?).await?;
            let written: Vec<PhysicalTableDataVersionDBWithNames> =
                db.timeline().fetch_all(&attempt).await?;
            let t_0 = written
                .iter()
                .find(|v| v.base.base.base.name.as_str() == "t_0")
                .expect("the attempt wrote t_0");
            assert_eq!(
                t_0.base.base.base.with_data_table_data_version_id, None,
                "no committed run ever wrote t_0 with data"
            );
            Ok(())
        }
    })
    .await
}
