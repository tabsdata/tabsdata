//
// Copyright 2026 Tabsdata Inc.
//

//! Runs of a function deleted (and re-registered under the same name) while
//! they were still pending or on their way to a pod.

use crate::Services;
use crate::scheduler::test_wiring::dispatch_services;
use std::str::FromStr;
use std::sync::Arc;
use ta_services::service::Ctx;
use td_bridge::messaging::memory::InMemoryOrchestratorMessageQueue;
use td_bridge::test::TestBridgeProvider;
use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::access::LockInstant;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::{ApiError, TdError};
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::worker::{WorkerDB, WorkerParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, BundleId, CollectionIdName, CollectionName, Decorator, FunctionId,
    FunctionIdName, FunctionOnly, FunctionRuntimeValues, LogicalFunctionRunStatus,
    OverrideDisabledTrigger, ProjectId, ReuseFrozen, SkipIfRunning, TableNameDto, UserId,
    WorkerType,
};
use td_objects::types::composed::TableDependencyDto;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const COLLECTION: &str = "c_deleted";
const FUNCTION: &str = "f_deleted";

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

async fn register(db: &TdPool, project: ProjectId) -> Result<FunctionId, TdError> {
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
        dependencies: vec![TableDependencyDto::try_from("t0")?].into(),
        triggers: None,
        tables: vec![TableNameDto::try_from("t0")?].into(),
        table_types: None,
        parent_tables: None,
        runtime_values: FunctionRuntimeValues::try_from("rv")?,
        reuse_frozen_tables: ReuseFrozen::TRUE,
        enabled: None,
        workers: None,
    };
    let request = context().create(params()?.0, register).in_project(project);
    let function = Services::with_defaults(db.clone())
        .register_function(request)
        .await?
        .data;
    Ok(function.function_id)
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

async fn delete(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
    let request = context().delete(params()?).in_project(project);
    kubernetes(db).delete_function(request).await?;
    Ok(())
}

async fn resolve(db: &TdPool, worker: &WorkerDB) -> Result<(), TdError> {
    let request = context().read(WorkerParam {
        worker: worker.base.id.clone(),
    });
    Services::with_defaults(db.clone())
        .resolve_worker_message(request)
        .await?;
    Ok(())
}

async fn function_runners(db: &TdPool) -> Result<Vec<WorkerDB>, TdError> {
    let workers: Vec<WorkerDB> = db.fetch_all::<WorkerDB>(()).await?;
    Ok(workers
        .into_iter()
        .filter(|w| w.base.worker_type == WorkerType::FunctionRunner)
        .collect())
}

async fn setup(db: &TdPool) -> Result<ProjectId, TdError> {
    let p = db.seed().test_project().await?;
    db.scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from(COLLECTION)?, &UserId::admin())
        .await?;
    Ok(p.id)
}

/// A run of a deleted function ends Canceled on the next tick, and its pod
/// gets a final answer.
#[td_test::test(db)]
async fn test_a_pending_run_of_a_deleted_function_ends(db: TdPool) -> Result<(), TdError> {
    let project = setup(&db).await?;
    register(&db, project).await?;
    let execution = execute(&db, project).await?;

    delete(&db, project).await?;
    register(&db, project).await?;
    kubernetes(&db).request(()).await?;

    let lfrs: Vec<LogicalFunctionRunDB> = db.fetch_all(&execution.id).await?;
    assert!(!lfrs.is_empty());
    for lfr in &lfrs {
        assert!(
            lfr.base.status == LogicalFunctionRunStatus::Canceled,
            "a run of a deleted version must end, got {:?}",
            lfr.base.status
        );
    }
    for worker in function_runners(&db).await? {
        if let Err(error) = resolve(&db, &worker).await {
            assert_eq!(error.api_error(), ApiError::NotFound);
        }
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_a_dispatched_run_of_a_deleted_function_resolves_not_found(
    db: TdPool,
) -> Result<(), TdError> {
    let project = setup(&db).await?;
    register(&db, project).await?;
    execute(&db, project).await?;
    kubernetes(&db).request(()).await?;
    let workers = function_runners(&db).await?;
    assert_eq!(workers.len(), 1);

    delete(&db, project).await?;
    register(&db, project).await?;

    let error = resolve(&db, &workers[0]).await.unwrap_err();
    assert_eq!(error.api_error(), ApiError::NotFound);
    Ok(())
}

#[td_test::test(db)]
async fn test_resolve_refuses_a_run_whose_function_is_gone(db: TdPool) -> Result<(), TdError> {
    let project = setup(&db).await?;
    let function_id = register(&db, project).await?;
    execute(&db, project).await?;
    kubernetes(&db).request(()).await?;
    let workers = function_runners(&db).await?;
    assert_eq!(workers.len(), 1);

    db.scoped(project)
        .scd2()
        .close::<FunctionDB>(&function_id, LockInstant::asserted(*AtTime::now()))
        .await?;

    let error = resolve(&db, &workers[0]).await.unwrap_err();
    assert_eq!(error.api_error(), ApiError::NotFound);
    Ok(())
}
