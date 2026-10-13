//
// Copyright 2026 Tabsdata Inc.
//

//! A cancel that reaches one run of a transaction cancels the whole
//! transaction: inputs are pinned at planning, so a transaction that read
//! cancelled data cannot publish any of its tables.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::ExecutionParam;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    PhysicalFunctionRunStatus, TableNameDto, TransactionStatus, UserId, WorkerStatus,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn function(
    name: &str,
    dependencies: Vec<TableDependencyDto>,
    table: &str,
    initial: LogicalFunctionRunStatus,
) -> Result<TestFunction, TdError> {
    Ok(TestFunction {
        collection: CollectionName::try_from("c_0")?,
        name: FunctionName::try_from(name)?,
        dependencies,
        tables: vec![TableNameDto::try_from(table)?],
        initial_status: initial,
        expected_status: initial,
    })
}

/// ep-0's `upstream` is still running when ep-1 is planned, so ep-1's
/// `reader` pins `upstream`'s in-flight output (the hidden fn_state
/// self-dependency gives every later run of a function this edge). ep-1's
/// transaction also holds `sibling`, already running and reading nothing of
/// ep-0. Canceling ep-0 cancels ep-1's whole transaction, and ends
/// `sibling`'s attempt and worker.
#[td_test::test(db)]
async fn test_canceling_an_execution_cancels_the_later_transactions_it_reaches(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![function(
                    "upstream",
                    vec![],
                    "t_up",
                    LogicalFunctionRunStatus::Running,
                )?],
            }],
        },
        TestExecution {
            expected_status: ExecutionStatus::Running,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Running,
                functions: vec![
                    function(
                        "reader",
                        vec![TableDependencyDto::try_from("t_up")?],
                        "t_reader",
                        LogicalFunctionRunStatus::Scheduled,
                    )?,
                    function(
                        "sibling",
                        vec![],
                        "t_sibling",
                        LogicalFunctionRunStatus::Running,
                    )?,
                ],
            }],
        },
    ];
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, t, f, w| {
        let db = db.clone();
        let execution = e[&test_executions[0]].base.id.clone();
        let transaction = t[&test_executions[1].transactions[0]].base.id.clone();
        let sibling = &test_executions[1].transactions[0].functions[1];
        let sibling_attempt = f[sibling].base.id.clone();
        let sibling_worker = w[sibling].base.id.clone();
        async move {
            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(ExecutionParam { execution }, ());
            Services::with_defaults(db.clone())
                .execution_cancel(request)
                .await?;

            let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&transaction).await?;
            let statuses: Vec<_> = runs.iter().map(|r| r.base.status).collect();
            assert!(
                statuses
                    .iter()
                    .all(|s| *s == LogicalFunctionRunStatus::Canceled),
                "ep-1's transaction is cancelled whole: {statuses:?}"
            );
            let rolled_up: TransactionDB = db.fetch_one(&transaction).await?;
            assert_eq!(rolled_up.status, TransactionStatus::Canceled);

            let attempt: PhysicalFunctionRunDB = db.timeline().fetch_one(&sibling_attempt).await?;
            assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Failed);
            let worker: WorkerDB = db.fetch_one(&sibling_worker).await?;
            assert_eq!(worker.base.status, WorkerStatus::Failed);
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
