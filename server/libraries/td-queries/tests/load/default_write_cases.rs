//
// Copyright 2026 Tabsdata Inc.
//

//! Default-family write paths and state-filtered reads over the chain dataset:
//! the cost of a single write / filtered read against a large, indexed table.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::logical::{LogicalFunctionRunInputInsertDB, LogicalFunctionRunStatusUpdateDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunUpdateDB;
use td_objects::types::basic::{
    DependencyPos, InputIdx, LogicalFunctionRunInputId, LogicalFunctionRunStatus,
    PhysicalFunctionRunStatus, VersionPos,
};
use td_queries::Queries;

use crate::dataset::ChainHandle;
use crate::harness::{record, seed};
use crate::raw::delete_by_pk;

/// Build (don't insert) a fresh, FK-valid LFRI from the handle's first unit.
pub(crate) fn build_lfri(h: &ChainHandle) -> Result<LogicalFunctionRunInputInsertDB, TdError> {
    Ok(LogicalFunctionRunInputInsertDB {
        id: LogicalFunctionRunInputId::default(),
        collection_id: h.collection_id,
        execution_id: h.execs[0].clone(),
        transaction_id: h.transaction_id.clone(),
        logical_function_run_id: h.producer_lfrs[0].clone(),
        requirement_logical_table_data_version_id: Some(h.ltdv_ids[0].clone()),
        requirement_input_idx: Some(InputIdx::try_from(0)?),
        requirement_dependency_pos: Some(DependencyPos::try_from(0)?),
        requirement_version_pos: VersionPos::try_from(0)?,
    })
}

#[td_test::test(db)]
async fn load_default_insert(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_lfri(&h)?;
    // The field-less LFRI sources `project_id` from the scoped connection,
    // as production writes do.
    let scoped = db.scoped(h.project_id);
    let t = Instant::now();
    (&scoped)
        .insert::<LogicalFunctionRunInputInsertDB>(&row)
        .await?;
    record("default::insert", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_default_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let update = LogicalFunctionRunStatusUpdateDB {
        status: LogicalFunctionRunStatus::Done,
    };
    let t = Instant::now();
    let r = db.update(&update, &h.producer_lfrs[0]).await?;
    record(
        "default::update",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_scd2_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let update = PhysicalFunctionRunUpdateDB {
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::Done,
    };
    let t = Instant::now();
    let r = db.scd2().update(&update, &h.fr_ids[0]).await?;
    record(
        "scd2::update",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_default_delete(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // Insert a throwaway row, then time its deletion by id.
    let row = build_lfri(&h)?;
    let scoped = db.scoped(h.project_id);
    (&scoped)
        .insert::<LogicalFunctionRunInputInsertDB>(&row)
        .await?;
    let t = Instant::now();
    let deleted = delete_by_pk::<LogicalFunctionRunInputInsertDB, _>(&scoped, &row.id).await?;
    record("default::delete", profile, t, deleted as usize, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_latest_before(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db.fetch_nth_last::<ActiveCollectionDB>((), 0).await?;
    record(
        "default::fetch_latest_before",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}
