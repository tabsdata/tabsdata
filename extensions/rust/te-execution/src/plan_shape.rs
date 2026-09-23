//
// Copyright 2026 Tabsdata Inc.
//

//! The plan shape this edition's `EXECUTION_GRAPH_OPTIONS` yields when a
//! function whose table another collection's function triggers on is run.

use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDBWithNames, ExecutionRequest};
use td_objects::dxo::function::{FunctionParam, FunctionRegister};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionDBWithNames;
use td_objects::types::basic::{
    AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, ExecutionName,
    ExecutionStatus, FunctionIdName, FunctionName, FunctionRuntimeValues, LogicalFunctionRunStatus,
    OverrideDisabledTrigger, ReuseFrozen, SkipIfRunning, TableNameDto, TransactionStatus, UserId,
};
use td_objects::types::composed::TableTriggerDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_services::Services;

fn register(
    name: &str,
    tables: &[&str],
    trigger: Option<&str>,
) -> Result<FunctionRegister, TdError> {
    Ok(FunctionRegister {
        name: FunctionName::try_from(name)?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: trigger
            .map(TableTriggerDto::try_from)
            .transpose()?
            .map(|t| vec![t]),
        tables: Some(
            tables
                .iter()
                .map(|t| TableNameDto::try_from(*t))
                .collect::<Result<_, _>>()?,
        ),
        table_types: None,
        runtime_values: FunctionRuntimeValues::try_from("r")?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    })
}

/// `c0/f0` produces `t0`; `c1/f1` triggers on `c0/t0`. Runs `f0` and returns
/// its runs, transactions and execution with their rolled-up status.
async fn run_trigger(
    db: &TdPool,
) -> Result<
    (
        Vec<LogicalFunctionRunDB>,
        Vec<TransactionDBWithNames>,
        ExecutionDBWithNames,
    ),
    TdError,
> {
    let p = db.seed().test_project().await?;
    let c0 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c0")?, &UserId::admin())
        .await?;
    let c1 = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c1")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&c0, &register("f0", &["t0"], None)?)
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&c1, &register("f1", &["t1"], Some("c0/t0"))?)
        .await?;

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c0")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str("f0")?,
                },
            ),
            ExecutionRequest {
                name: Some(ExecutionName::try_from("run")?),
                function_only: false.into(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    Services::with_defaults(db.clone())
        .execute_function(request)
        .await?;

    let runs = db.fetch_all::<LogicalFunctionRunDB>(()).await?;
    let transactions = db.fetch_all::<TransactionDBWithNames>(()).await?;
    let mut executions = db.fetch_all::<ExecutionDBWithNames>(()).await?;
    assert_eq!(executions.len(), 1);
    let execution = executions.remove(0);
    Ok((runs, transactions, execution))
}

/// The open-source edition plans the triggered function alone.
#[td_test::test(db)]
async fn plan_holds_the_trigger_alone(db: TdPool) -> Result<(), TdError> {
    let (runs, transactions, execution) = run_trigger(&db).await?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].base.status, LogicalFunctionRunStatus::Scheduled);
    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0].base.status, TransactionStatus::Scheduled);
    assert_eq!(execution.base.status, ExecutionStatus::Scheduled);
    Ok(())
}
