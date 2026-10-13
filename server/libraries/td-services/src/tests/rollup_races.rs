//
// Copyright 2026 Tabsdata Inc.
//

//! Reports of sibling runs commit concurrently; the transaction and
//! execution each one rolls up must still match a recompute from all of them.

use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, test_status_update,
};
use crate::tests::callback_outcomes::report_attempt;
use futures_util::future::try_join_all;
use td_common::execution_status::{CallbackOutcome, WorkerCallbackStatus};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus, TableNameDto,
    TransactionStatus,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::derive_status_rollup;

const SIBLINGS: usize = 4;

fn sibling(i: usize) -> Result<TestFunction, TdError> {
    Ok(TestFunction {
        collection: CollectionName::try_from("c_0")?,
        name: FunctionName::try_from(format!("f_{i}"))?,
        dependencies: vec![],
        tables: vec![TableNameDto::try_from(format!("t_{i}"))?],
        initial_status: LogicalFunctionRunStatus::Running,
        expected_status: LogicalFunctionRunStatus::Committed,
    })
}

#[td_test::test(db)]
async fn test_sibling_reports_racing_roll_up_like_a_recompute(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Finished,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Committed,
            functions: (0..SIBLINGS).map(sibling).collect::<Result<_, _>>()?,
        }],
    }];
    test_status_update(db.clone(), p.id, &test_executions, |_, e, t, f, w| {
        let db = db.clone();
        let test_transaction = &test_executions[0].transactions[0];
        let execution = e[&test_executions[0]].base.id.clone();
        let transaction = t[test_transaction].base.id.clone();
        let reports: Vec<_> = test_transaction
            .functions
            .iter()
            .map(|function| (f[function].base.id.clone(), w[function].base.id.clone()))
            .collect();
        async move {
            let outcomes = try_join_all(reports.iter().map(|(function_run, worker)| {
                report_attempt(&db, function_run, worker, WorkerCallbackStatus::Done)
            }))
            .await?;
            assert!(outcomes.iter().all(|o| *o == CallbackOutcome::Applied));

            let stored: TransactionDB = db.fetch_one(&transaction).await?;
            let stored_execution: ExecutionDB = db.fetch_one(&execution).await?;
            derive_status_rollup(&db).await?;
            let recomputed: TransactionDB = db.fetch_one(&transaction).await?;
            let recomputed_execution: ExecutionDB = db.fetch_one(&execution).await?;
            assert_eq!(stored.status, recomputed.status);
            assert_eq!(stored.ended_on, recomputed.ended_on);
            assert_eq!(stored_execution.status, recomputed_execution.status);
            assert_eq!(stored_execution.ended_on, recomputed_execution.ended_on);
            Ok(())
        }
    })
    .await
}
