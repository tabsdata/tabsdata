//
// Copyright 2026 Tabsdata Inc.
//

//! A pod acts on the request it resolves once the resolve commits, so the
//! resolve holds its worker: a unit that ended the attempt first turns the
//! answer into not-found.

use crate::Services;
use crate::execution::update_status::tests::{
    TestExecution, TestFunction, TestTransaction, run_status_scenario,
};
use std::time::Duration;
use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::worker::WorkerParam;
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionName, ExecutionStatus, FunctionName, LogicalFunctionRunStatus,
    TableNameDto, TransactionStatus, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const BLOCKED_WITHIN: Duration = Duration::from_secs(10);

#[td_test::test(db)]
async fn test_a_resolve_waiting_on_a_cancel_of_its_attempt_answers_not_found(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let test_executions = vec![TestExecution {
        expected_status: ExecutionStatus::Running,
        transactions: vec![TestTransaction {
            expected_status: TransactionStatus::Running,
            functions: vec![TestFunction {
                collection: CollectionName::try_from("c_0")?,
                name: FunctionName::try_from("f_0")?,
                dependencies: vec![],
                tables: vec![TableNameDto::try_from("t_0")?],
                initial_status: LogicalFunctionRunStatus::Running,
                expected_status: LogicalFunctionRunStatus::Running,
            }],
        }],
    }];
    run_status_scenario(db.clone(), p.id, &test_executions, |_, _, _, f, w| {
        let db = db.clone();
        let function = &test_executions[0].transactions[0].functions[0];
        let attempt = f[function].base.id.clone();
        let worker = w[function].base.id.clone();
        async move {
            let pools = DbPool::<AcrossProjects>::new(
                DomainPools::of_apiserver(db.clone()),
                AcrossProjects,
            );
            let mut gate = pools.in_project(&p.id).begin(Attempt::unretried()).await?;
            let at = AtTime::now();
            let conn = gate.get(Grant::asserted("attempt ended, held open"));
            conn.physical_runs().end(&[attempt], &at).await?;

            let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(WorkerParam { worker });
            let services = Services::with_defaults(db.clone());
            let resolving =
                tokio::spawn(async move { services.resolve_worker_message(request).await });
            assert!(
                gate.get(Grant::asserted("blocking probe"))
                    .wait_until_blocking(BLOCKED_WITHIN)
                    .await
                    .expect("blocking probe"),
                "the resolve never waited on the worker"
            );
            gate.commit().await?;

            let resolved = resolving.await.expect("resolve task");
            assert!(
                matches!(&resolved, Err(e) if e.api_error() == ApiError::NotFound),
                "a resolve of an attempt ended first must answer not-found: {resolved:?}"
            );
            Ok(())
        }
    })
    .await
    .map(|_| ())
}
