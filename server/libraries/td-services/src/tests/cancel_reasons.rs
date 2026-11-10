//
// Copyright 2026 Tabsdata Inc.
//

use crate::Services;
use crate::tests::fixtures::register;
use std::collections::BTreeMap;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::cancel::{CancelReason, CancelResponse};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionParam, ExecutionRequest};
use td_objects::dxo::function::{FunctionParam, FunctionRegister};
use td_objects::dxo::transaction::{TransactionDB, TransactionParam};
use td_objects::types::basic::{
    AccessTokenId, CollectionIdName, CollectionName, ExecutionId, FunctionIdName,
    OverrideDisabledTrigger, ProjectId, SkipIfRunning, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const COLLECTION: &str = "c_cancel";

fn context() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn collection() -> Result<CollectionParam, TdError> {
    Ok(CollectionParam {
        collection: CollectionIdName::from_str(COLLECTION)?,
    })
}

async fn setup(db: &TdPool, functions: Vec<FunctionRegister>) -> Result<ProjectId, TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    for function in functions {
        let request = context()
            .create(collection()?, function)
            .in_project(p.project_id);
        Services::with_defaults(db.clone())
            .register_function(request)
            .await?;
    }
    Ok(p.project_id)
}

async fn execute(
    db: &TdPool,
    project: ProjectId,
    function: &str,
    function_only: bool,
) -> Result<ExecutionId, TdError> {
    let request = context()
        .create(
            (
                collection()?,
                FunctionParam {
                    function: FunctionIdName::from_str(function)?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: function_only.into(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(project);
    Ok(Services::with_defaults(db.clone())
        .execute_function(request)
        .await?
        .data
        .id)
}

async fn cancel_execution(db: &TdPool, execution: &ExecutionId) -> Result<CancelResponse, TdError> {
    let request = context().update(
        ExecutionParam {
            execution: execution.clone(),
        },
        (),
    );
    Ok(Services::with_defaults(db.clone())
        .execution_cancel(request)
        .await?
        .data)
}

fn reached(response: &CancelResponse) -> BTreeMap<ExecutionId, Vec<(String, CancelReason)>> {
    let mut reached: BTreeMap<ExecutionId, Vec<(String, CancelReason)>> = BTreeMap::new();
    for run in &response.canceled {
        assert_eq!(run.collection.to_string(), COLLECTION);
        reached
            .entry(run.execution_id.clone())
            .or_default()
            .push((run.function.to_string(), run.reason));
    }
    reached.values_mut().for_each(|runs| runs.sort());
    reached
}

fn runs(runs: &[(&str, CancelReason)]) -> Vec<(String, CancelReason)> {
    runs.iter()
        .map(|(function, reason)| (function.to_string(), *reason))
        .collect()
}

#[td_test::test(db)]
async fn test_canceling_a_plan_cancels_the_later_runs_of_its_function(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db, vec![register("f", None)]).await?;
    let first = execute(&db, project, "f", true).await?;
    let second = execute(&db, project, "f", true).await?;
    let third = execute(&db, project, "f", true).await?;

    let response = cancel_execution(&db, &first).await?;

    assert_eq!(
        reached(&response),
        BTreeMap::from([
            (first, runs(&[("f", CancelReason::Requested)])),
            (second, runs(&[("f", CancelReason::SameFunction)])),
            (third, runs(&[("f", CancelReason::SameFunction)])),
        ]),
        "{response:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_a_later_run_of_a_canceled_function_is_the_same_function(
    db: TdPool,
) -> Result<(), TdError> {
    let reader = FunctionRegister {
        dependencies: Some(vec![TableDependencyDto::try_from("f_t0")?]),
        triggers: Some(vec![]),
        ..register("f", None)
    };
    let project = setup(&db, vec![reader]).await?;
    let first = execute(&db, project, "f", true).await?;
    let second = execute(&db, project, "f", true).await?;
    let third = execute(&db, project, "f", true).await?;

    let response = cancel_execution(&db, &first).await?;

    assert_eq!(
        reached(&response),
        BTreeMap::from([
            (first, runs(&[("f", CancelReason::Requested)])),
            (second, runs(&[("f", CancelReason::SameFunction)])),
            (third, runs(&[("f", CancelReason::SameFunction)])),
        ]),
        "{response:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_canceling_a_transaction_returns_every_run_it_canceled(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db, vec![register("f", None)]).await?;
    let first = execute(&db, project, "f", true).await?;
    let second = execute(&db, project, "f", true).await?;
    let transaction: TransactionDB = db
        .scoped(project)
        .fetch_all::<TransactionDB>(&first)
        .await?
        .remove(0);

    let request = context().update(
        TransactionParam {
            transaction: transaction.id.clone(),
        },
        (),
    );
    let response = Services::with_defaults(db.clone())
        .transaction_cancel(request)
        .await?
        .data;

    assert_eq!(
        reached(&response),
        BTreeMap::from([
            (first, runs(&[("f", CancelReason::Requested)])),
            (second, runs(&[("f", CancelReason::SameFunction)])),
        ]),
        "{response:?}"
    );
    Ok(())
}
