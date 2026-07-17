//
// Copyright 2026 Tabs Data Inc.
//

//! Live-family write paths and state-filtered reads over the chain dataset:
//! the cost of a single write / filtered read against a large, indexed table.

use std::time::Instant;

use sea_query::Condition;
use ta_objects::{Buildable, StateFilter};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::logical::LogicalFunctionRunInputDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB};
use td_objects::types::basic::{DependencyPos, InputIdx, PhysicalFunctionRunStatus, VersionPos};
use td_queries::LiveEntry;

use crate::dataset::ChainHandle;
use crate::harness::{record, seed};

/// Build (don't insert) a fresh, FK-valid LFRI from the handle's first unit.
fn build_lfri(h: &ChainHandle) -> Result<LogicalFunctionRunInputDB, TdError> {
    Ok(LogicalFunctionRunInputDB::builder()
        .collection_id(h.collection_id)
        .execution_id(&h.execs[0])
        .transaction_id(&h.transaction_id)
        .logical_function_run_id(h.producer_lfrs[0].clone())
        .requirement_logical_table_data_version_id(Some(h.ltdv_ids[0].clone()))
        .requirement_input_idx(Some(InputIdx::try_from(0)?))
        .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
        .requirement_version_pos(VersionPos::try_from(0)?)
        .build()?)
}

#[td_test::test(db)]
async fn load_live_insert(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_lfri(&h)?;
    // The field-less LFRI sources `project_id` from the scoped connection,
    // as production writes do.
    let scoped = db.scoped(h.project_id);
    let t = Instant::now();
    (&scoped)
        .live()
        .insert::<LogicalFunctionRunInputDB>(&row)
        .await?;
    record("live::insert", &db, profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_live_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let update = UpdatePhysicalFunctionRunDB::builder()
        .status(PhysicalFunctionRunStatus::Done)
        .build()?;
    let t = Instant::now();
    let r = (&db)
        .live()
        .update::<_, PhysicalFunctionRunDB>(&update, &h.fr_ids[0])
        .await?;
    record(
        "live::update",
        &db,
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_live_delete(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // Insert a throwaway row, then time its deletion by id.
    let row = build_lfri(&h)?;
    let scoped = db.scoped(h.project_id);
    (&scoped)
        .live()
        .insert::<LogicalFunctionRunInputDB>(&row)
        .await?;
    let t = Instant::now();
    let r = (&scoped)
        .live()
        .delete::<LogicalFunctionRunInputDB>(&row.id)
        .await?;
    record(
        "live::delete",
        &db,
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_all_in(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let state = StateFilter::<CollectionDB>::new(Condition::all());
    let t = Instant::now();
    let rows = (&db).live().fetch_all_in::<CollectionDB>(state, ()).await?;
    record("live::fetch_all_in", &db, profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_latest_before(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let state = StateFilter::<CollectionDB>::new(Condition::all());
    let t = Instant::now();
    let row = (&db)
        .live()
        .fetch_latest_before::<CollectionDB>(state, (), 0)
        .await?;
    record(
        "live::fetch_latest_before",
        &db,
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}
