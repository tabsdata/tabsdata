//
// Copyright 2026 Tabsdata Inc.
//

//! A plan only reads Running while something in it can progress.

use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use crate::scheduler::test_wiring::dispatch_services;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionParam};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    TableNameDto, TransactionStatus, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// ep-1's `consumer` is On Hold behind ep-0's Failed `producer`.
/// Recovering ep-1 alone cannot unblock it, since `producer` stays Failed, so
/// the recover leaves `consumer` On Hold and ep-1 Stalled.
#[td_test::test(db)]
async fn test_recovering_a_run_blocked_on_a_foreign_failure_does_not_read_running(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![
        TestExecution {
            expected_status: ExecutionStatus::Stalled,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Stalled,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("producer")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Failed,
                    expected_status: LogicalFunctionRunStatus::Failed,
                }],
            }],
        },
        TestExecution {
            expected_status: ExecutionStatus::Stalled,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Stalled,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("consumer")?,
                    dependencies: vec![TableDependencyDto::try_from("t_0")?],
                    tables: vec![TableNameDto::try_from("t_1")?],
                    initial_status: LogicalFunctionRunStatus::OnHold,
                    expected_status: LogicalFunctionRunStatus::OnHold,
                }],
            }],
        },
    ];
    run_status_scenario(db.clone(), p.id, &test_executions, |_, e, _, _, _| {
        let db = db.clone();
        let execution = e[&test_executions[1]].base.id.clone();
        async move {
            let recover = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
                ExecutionParam {
                    execution: execution.clone(),
                },
                (),
            );
            dispatch_services(&db).execution_recover(recover).await?;
            dispatch_services(&db).request(()).await?;

            let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution).await?;
            let statuses: Vec<_> = runs.iter().map(|r| r.base.status).collect();
            assert!(
                !statuses.contains(&LogicalFunctionRunStatus::Requested),
                "the blocked run was dispatched: {statuses:?}"
            );
            assert_eq!(statuses, [LogicalFunctionRunStatus::OnHold]);
            let rolled_up: ExecutionDB = db.fetch_one(&execution).await?;
            assert_eq!(
                rolled_up.status,
                ExecutionStatus::Stalled,
                "ep-1 has nothing able to progress: runs {statuses:?}"
            );
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
