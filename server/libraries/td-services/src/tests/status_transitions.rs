//
// Copyright 2026 Tabsdata Inc.
//

//! The status-transition triggers installed in the database refuse exactly
//! the changes the Rust relations do not declare. Each trigger function is
//! attached to a scratch table, so every `(kind, from, to)` triple is tried
//! without building the rows it would take to reach it.

use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::errors::SqlError;
use td_objects::types::basic::{
    LogicalFunctionRunStatus, PhysicalFunctionRunStatus, WorkerStatus, WorkerType,
};
use td_queries::test_utils::probes::{TransitionCase, trigger_disagreements};

/// `(worker type, from, to, allowed)`; the type is empty for runs and attempts.
type Case = TransitionCase;

async fn refused(db: &TdPool, trigger_fn: &str, cases: &[Case]) -> Result<Vec<String>, TdError> {
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut scratch = pools.begin(Attempt::unretried()).await?;
    let conn = scratch.get(Grant::asserted("scratch table for the trigger check"));
    let mismatches = trigger_disagreements(conn, trigger_fn, cases)
        .await
        .map_err(|e| SqlError::AnySqlxError("transitions_probe".to_string(), e))?;
    Ok(mismatches)
}

#[td_test::test(db)]
async fn test_run_trigger_matches_the_relation(db: TdPool) -> Result<(), TdError> {
    let cases: Vec<Case> = LogicalFunctionRunStatus::iter()
        .flat_map(|from| {
            LogicalFunctionRunStatus::iter().map(move |to| {
                (
                    String::new(),
                    from.to_string(),
                    to.to_string(),
                    from.can_move_to(&to),
                )
            })
        })
        .collect();
    let mismatches = refused(&db, "logical_function_runs__transitions_fn", &cases).await?;
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_attempt_trigger_matches_the_relation(db: TdPool) -> Result<(), TdError> {
    let cases: Vec<Case> = PhysicalFunctionRunStatus::iter()
        .flat_map(|from| {
            PhysicalFunctionRunStatus::iter().map(move |to| {
                (
                    String::new(),
                    from.to_string(),
                    to.to_string(),
                    from.can_move_to(&to),
                )
            })
        })
        .collect();
    let mismatches = refused(&db, "physical_function_runs__transitions_fn", &cases).await?;
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_worker_trigger_matches_the_relation(db: TdPool) -> Result<(), TdError> {
    let cases: Vec<Case> = WorkerType::iter()
        .flat_map(|kind| {
            WorkerStatus::iter().flat_map(move |from| {
                WorkerStatus::iter().map(move |to| {
                    (
                        kind.to_string(),
                        from.to_string(),
                        to.to_string(),
                        from.can_move_to(&kind, &to),
                    )
                })
            })
        })
        .collect();
    let mismatches = refused(&db, "workers__transitions_fn", &cases).await?;
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
