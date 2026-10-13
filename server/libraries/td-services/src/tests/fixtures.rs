//
// Copyright 2026 Tabsdata Inc.
//

//! Cross-module test fixtures for the mint/dispatch object graph.

use crate::scheduler::test_wiring::dispatch_services;
use std::str::FromStr;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionParam, CollectionUpdateDB};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionRequest};
use td_objects::dxo::function::{FunctionDB, FunctionParam, FunctionRegister};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    AccessTokenId, BasePath, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
    FunctionOnly, FunctionRuntimeValues, LogicalFunctionRunStatus, OverrideDisabledTrigger,
    PhysicalFunctionRunStatus, ProjectId, ReuseFrozen, SkipIfRunning, TableNameDto, TransactionKey,
    UserId,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

/// A publisher registration with one regular output table, `<name>_t0`, so
/// several of them fit in one collection.
/// `base_path` becomes the function-version override.
pub(crate) fn register(name: &str, base_path: Option<BasePath>) -> FunctionRegister {
    FunctionRegister {
        child_functions: None,
        name: name.try_into().unwrap(),
        description: "d".try_into().unwrap(),
        bundle_id: BundleId::default(),
        snippet: "s".try_into().unwrap(),
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path,
        dependencies: None,
        triggers: None,
        tables: Some(vec![TableNameDto::try_from(format!("{name}_t0")).unwrap()]),
        table_types: None,
        parent_tables: None,
        runtime_values: FunctionRuntimeValues::try_from("rv").unwrap(),
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    }
}

/// Set (or clear) a collection's `base_path` via the live-update seam (no
/// service oneshot): seed the updater from the current `ActiveCollectionDB`,
/// stamp modified_on/by from a RequestContext, override base_path, then
/// `update`.
pub(crate) async fn set_collection_base_path(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    base_path: Option<&BasePath>,
) -> Result<ActiveCollectionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .read::<String>("")
        .context;
    let update = CollectionUpdateDB {
        name: None,
        description: None,
        modified_on: request_context.time.into(),
        modified_by_id: request_context.user_id.into(),
        base_path: Some(base_path.cloned()),
    };
    db.scoped(project_id)
        .update::<CollectionUpdateDB>(&update, &collection.id)
        .await?;
    let updated = db.fetch_one::<ActiveCollectionDB>(&collection.id).await?;
    Ok(updated)
}

/// Plan-build output for a publisher with one regular output table:
/// an LFR and its output LTDVs, with no attempt yet — what the dispatch tick
/// sees. `base_path` becomes the function-version override
/// (`FunctionRegister::base_path`).
pub(crate) async fn seed_ready_lfr(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    name: &str,
    base_path: Option<BasePath>,
) -> Result<LogicalFunctionRunDB, TdError> {
    let (func, exec, trx) = seed_run_prelude(db, project_id, collection, name, base_path).await?;
    db.scoped(project_id)
        .seed()
        .logical_function_run(
            collection,
            &func,
            &exec,
            &trx,
            &LogicalFunctionRunStatus::Scheduled,
        )
        .await
}

/// The same publisher, dispatched: the LFR plus a Scheduled attempt carrying
/// its stamped `base_path`.
pub(crate) async fn seed_dispatched_run(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    name: &str,
    base_path: Option<BasePath>,
) -> Result<PhysicalFunctionRunDB, TdError> {
    let (func, exec, trx) = seed_run_prelude(db, project_id, collection, name, base_path).await?;
    db.scoped(project_id)
        .seed()
        .physical_function_run(
            collection,
            &func,
            &exec,
            &trx,
            &LogicalFunctionRunStatus::Scheduled,
        )
        .await
}

/// A publisher executed and dispatched by a real tick, in a new project: its
/// one attempt is Dispatched with a worker, its pod not up yet.
pub(crate) async fn dispatched_publisher(
    db: &TdPool,
    collection: &str,
    function: &str,
) -> Result<(ProjectId, PhysicalFunctionRunDB), TdError> {
    let p = db.seed().test_project().await?;
    let seeded = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from(collection)?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&seeded, &register(function, None))
        .await?;
    let execute = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str(collection)?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str(function)?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: FunctionOnly::default(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(p.id);
    let execution = dispatch_services(db).execute_function(execute).await?.data;
    dispatch_services(db).request(()).await?;
    let attempts: Vec<PhysicalFunctionRunDB> = db
        .timeline()
        .fetch_all(&(&execution.manual_trigger))
        .await?;
    let [attempt] = <[_; 1]>::try_from(attempts).expect("one attempt");
    assert_eq!(attempt.base.status, PhysicalFunctionRunStatus::Dispatched);
    Ok((p.id, attempt))
}

async fn seed_run_prelude(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    name: &str,
    base_path: Option<BasePath>,
) -> Result<(FunctionDB, ExecutionDB, TransactionDB), TdError> {
    let func = db
        .scoped(project_id)
        .seed()
        .function(collection, &register(name, base_path))
        .await?;
    let exec = db.scoped(project_id).seed().execution(&func).await?;
    let trx = db
        .scoped(project_id)
        .seed()
        .transaction(&exec, &TransactionKey::try_from("ANY")?)
        .await?;
    Ok((func, exec, trx))
}
