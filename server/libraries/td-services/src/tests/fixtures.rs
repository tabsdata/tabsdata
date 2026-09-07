//
// Copyright 2026 Tabsdata Inc.
//

//! Cross-module test fixtures for the mint/dispatch object graph.

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionUpdateDB};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::FunctionRegister;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalPointerDao,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::types::basic::{
    AccessTokenId, AtTime, BasePath, BundleId, Decorator, FunctionRuntimeValues,
    LogicalFunctionRunStatus, LogicalPointerReason, LogicalPointerStatus, ProjectId, ReuseFrozen,
    TableNameDto, TransactionKey, UserId,
};
use td_queries::LiveEntry;
use td_queries::test_utils::SeedEntry;

/// A publisher registration with one regular output table ("t0").
/// `base_path` becomes the function-version override.
pub(crate) fn register(name: &str, base_path: Option<BasePath>) -> FunctionRegister {
    FunctionRegister {
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
        tables: Some(vec![TableNameDto::try_from("t0").unwrap()]),
        table_types: None,
        runtime_values: FunctionRuntimeValues::try_from("rv").unwrap(),
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    }
}

/// Set (or clear) a collection's `base_path` via the live-update seam (no
/// service oneshot): seed the updater from the current `ActiveCollectionDB`,
/// stamp modified_on/by from a RequestContext, override base_path, then
/// `live().update`.
pub(crate) async fn set_collection_base_path(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    base_path: Option<&BasePath>,
) -> Result<ActiveCollectionDB, TdError> {
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .read::<String>("")
        .context;
    let mut update = CollectionUpdateDB::from(collection);
    update.modified_on = request_context.time;
    update.modified_by_id = request_context.user_id;
    update.base_path = Some(base_path.cloned());
    db.scoped(project_id)
        .live()
        .update::<CollectionUpdateDB, ActiveCollectionDB>(&update, &collection.id)
        .await?;
    let updated = db
        .live()
        .fetch_one::<ActiveCollectionDB>(&collection.id)
        .await?;
    Ok(updated)
}

/// Seed a function with a single regular output table and mint a Scheduled
/// LFR/PFR pair (plus the LFR/LTDV pointer events the dispatch loop reads)
/// against it. `base_path` becomes the function-version override
/// (`FunctionRegister::base_path`).
pub(crate) async fn seed_ready_lfr(
    db: &TdPool,
    project_id: ProjectId,
    collection: &ActiveCollectionDB,
    name: &str,
    base_path: Option<BasePath>,
) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunDB), TdError> {
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
    let pfr = db
        .scoped(project_id)
        .seed()
        .physical_function_run(
            collection,
            &func,
            &exec,
            &trx,
            &LogicalFunctionRunStatus::Scheduled,
        )
        .await?;
    let lfr = db
        .live()
        .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
        .await?;

    // `seed().physical_function_run` leaves the LFR pointer Bound to its
    // seeded PFR. Real dispatch sees a Pending (InitialMint) LFR pointer and
    // binds it to the freshly-minted PFR. Append that Pending event so the
    // mint loop has an unbound pointer to bind, matching plan-build output.
    let pending = LogicalFunctionRunPointerDB::build_event(
        lfr.project_id,
        &lfr.id,
        LogicalPointerStatus::Pending,
        None,
        AtTime::now(),
        UserId::admin(),
        LogicalPointerReason::InitialMint,
    )?;
    db.scoped(project_id)
        .live()
        .insert::<LogicalFunctionRunPointerDB>(&pending)
        .await?;
    Ok((lfr, pfr))
}
