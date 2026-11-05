//
// Copyright 2026 Tabsdata Inc.
//

//! Runs of a function whose update dropped one of its tables: the table is
//! frozen on the function, and dispatch mints the outputs the run's plan has.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use std::str::FromStr;
use std::sync::Arc;
use ta_services::service::Ctx;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::test::TestBridgeProvider;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionParam, FunctionRegister, FunctionUpdate};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
    FunctionOnly, FunctionRuntimeValues, OverrideDisabledTrigger, PhysicalFunctionRunStatus,
    ProjectId, ReuseFrozen, SkipIfRunning, TableNameDto, TableStatus, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const COLLECTION: &str = "c_dropped";
const FUNCTION: &str = "f_dropped";

fn kubernetes(db: &TdPool) -> Services {
    let mut services = dispatch_services(db);
    services.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
        TestBridgeProvider::default()
            .with_orchestrator(Orchestrator::Kubernetes)
            .with_message_queue(InMemoryOrchestratorMessageQueue::default()),
    )));
    services
}

fn params() -> Result<(CollectionParam, FunctionParam), TdError> {
    Ok((
        CollectionParam {
            collection: CollectionIdName::from_str(COLLECTION)?,
        },
        FunctionParam {
            function: FunctionIdName::from_str(FUNCTION)?,
        },
    ))
}

fn context() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

async fn setup(db: &TdPool) -> Result<ProjectId, TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    let register = FunctionRegister {
        child_functions: None,
        name: FUNCTION.try_into()?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: Some(vec![TableNameDto::try_from("t0")?]),
        table_types: None,
        parent_tables: None,
        runtime_values: FunctionRuntimeValues::try_from("rv")?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    let request = context()
        .create(params()?.0, register)
        .in_project(p.project_id);
    Services::with_defaults(db.clone())
        .register_function(request)
        .await?;
    Ok(p.project_id)
}

async fn drop_tables(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
    let update = FunctionUpdate {
        tables: Some(vec![]),
        ..Default::default()
    };
    let request = context().update(params()?, update).in_project(project);
    Services::with_defaults(db.clone())
        .update_function(request)
        .await?;
    let frozen: Vec<TableDB> = db.scd2().fetch_all(&TableStatus::Frozen).await?;
    assert_eq!(frozen.len(), 1, "t0 is frozen, not deleted");
    Ok(())
}

async fn execute(db: &TdPool, project: ProjectId) -> Result<ExecutionResponse, TdError> {
    let request = context()
        .create(
            params()?,
            ExecutionRequest {
                name: None,
                function_only: FunctionOnly::default(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(project);
    Ok(dispatch_services(db).execute_function(request).await?.data)
}

async fn dispatched_outputs(
    db: &TdPool,
    execution: &ExecutionResponse,
) -> Result<Vec<PhysicalTableDataVersionDB>, TdError> {
    let attempts: Vec<PhysicalFunctionRunDB> = db
        .timeline()
        .fetch_all(and!(&execution.id, PhysicalFunctionRunStatus::Dispatched))
        .await?;
    let [attempt] = <[_; 1]>::try_from(attempts).expect("one dispatched attempt");
    Ok(db.timeline().fetch_all(&attempt.id).await?)
}

/// Planned after the drop, the run has no logical version for t0: dispatch
/// mints only its state table.
#[td_test::test(db)]
async fn test_a_run_planned_after_a_drop_dispatches_without_the_table(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db).await?;
    drop_tables(&db, project).await?;
    let execution = execute(&db, project).await?;

    kubernetes(&db).tick().await?;

    let outputs = dispatched_outputs(&db, &execution).await?;
    assert_eq!(outputs.len(), 1, "the state table alone");
    Ok(())
}

/// Planned before the drop, the run keeps the table its plan has: dispatch
/// reads the tables as of the run's topology version.
#[td_test::test(db)]
async fn test_a_run_planned_before_a_drop_dispatches_with_the_table(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db).await?;
    let execution = execute(&db, project).await?;
    drop_tables(&db, project).await?;

    kubernetes(&db).tick().await?;

    let outputs = dispatched_outputs(&db, &execution).await?;
    assert_eq!(outputs.len(), 2, "t0 and the state table");
    Ok(())
}
