//
// Copyright 2026 Tabsdata Inc.
//

//! A run whose attempt cannot be minted from its plan is cancelled by the
//! dispatch step, and the project's other runs still dispatch.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use sqlx::PgConnection;
use std::str::FromStr;
use std::sync::Arc;
use ta_services::service::Ctx;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::test::TestBridgeProvider;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::access::Grant;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_database::test_utils::generated;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionParam, FunctionRegister};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
    FunctionOnly, FunctionRuntimeValues, LogicalFunctionRunStatus, OverrideDisabledTrigger,
    PhysicalFunctionRunStatus, ProjectId, ReuseFrozen, SkipIfRunning, TableNameDto, UserId,
};
use td_queries::Queries;
use td_queries::test_utils::{SeedEntry, begin_locked};

const COLLECTION: &str = "c_unmintable";

fn kubernetes(db: &TdPool) -> Services {
    let mut services = dispatch_services(db);
    services.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
        TestBridgeProvider::default()
            .with_orchestrator(Orchestrator::Kubernetes)
            .with_message_queue(InMemoryOrchestratorMessageQueue::default()),
    )));
    services
}

fn params(function: &str) -> Result<(CollectionParam, FunctionParam), TdError> {
    Ok((
        CollectionParam {
            collection: CollectionIdName::from_str(COLLECTION)?,
        },
        FunctionParam {
            function: FunctionIdName::from_str(function)?,
        },
    ))
}

fn context() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

async fn setup(db: &TdPool, functions: &[&str]) -> Result<ProjectId, TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    for function in functions {
        let register = FunctionRegister {
            child_functions: None,
            name: (*function).try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from(format!("{function}_t0"))?]),
            table_types: None,
            parent_tables: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let request = context()
            .create(params(function)?.0, register)
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
) -> Result<ExecutionResponse, TdError> {
    let request = context()
        .create(
            params(function)?,
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

/// Takes the planned logical versions away from `execution`'s runs, which
/// leaves dispatch nothing to mint their outputs from.
async fn unplan_outputs(
    db: &TdPool,
    project: ProjectId,
    execution: &ExecutionResponse,
) -> Result<(), TdError> {
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    let mut trx = begin_locked(db, project).await?;
    let conn: &mut PgConnection = trx.get(Grant::asserted("unmintable test"));
    for run in &runs {
        sqlx::query(generated(
            "DELETE FROM logical_table_data_versions WHERE producer_logical_function_run_id = $1"
                .to_string(),
        ))
        .bind(&run.id)
        .execute(&mut *conn)
        .await
        .map_err(td_database::access::ServiceDbError::CannotCommitTransaction)?;
    }
    trx.commit().await
}

async fn run_status(
    db: &TdPool,
    execution: &ExecutionResponse,
) -> Result<LogicalFunctionRunStatus, TdError> {
    let runs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    let [run] = <[_; 1]>::try_from(runs).expect("one run");
    Ok(run.status)
}

#[td_test::test(db)]
async fn test_an_unmintable_run_is_cancelled_and_the_rest_dispatch(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db, &["f_bad", "f_good"]).await?;
    let bad = execute(&db, project, "f_bad").await?;
    let good = execute(&db, project, "f_good").await?;
    unplan_outputs(&db, project, &bad).await?;

    kubernetes(&db).tick().await?;

    assert_eq!(
        run_status(&db, &bad).await?,
        LogicalFunctionRunStatus::Canceled
    );
    assert_eq!(
        run_status(&db, &good).await?,
        LogicalFunctionRunStatus::Requested
    );
    let attempts: Vec<PhysicalFunctionRunDB> = db
        .timeline()
        .fetch_all(and!(&good.id, PhysicalFunctionRunStatus::Dispatched))
        .await?;
    assert_eq!(attempts.len(), 1, "the good run's attempt is dispatched");
    let bad_attempts: Vec<PhysicalFunctionRunDB> = db.timeline().fetch_all(&bad.id).await?;
    assert!(
        bad_attempts.is_empty(),
        "the cancelled run never got an attempt"
    );
    Ok(())
}
