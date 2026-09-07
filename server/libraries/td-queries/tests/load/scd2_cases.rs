//
// Copyright 2026 Tabsdata Inc.
//

//! The interval writes on the chain dataset's attempt table: a new version of
//! a partition, one row and chunked.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunInsertDB};
use td_objects::types::basic::PhysicalFunctionRunId;
use td_queries::Queries;

use crate::dataset::ChainHandle;
use crate::harness::{record, seed};

/// Build (don't insert) a new attempt of the handle's first run.
pub(crate) async fn build_attempt(
    db: &TdPool,
    h: &ChainHandle,
) -> Result<PhysicalFunctionRunInsertDB, TdError> {
    let current: PhysicalFunctionRunDB = db.timeline().fetch_one(&h.fr_ids[0]).await?;
    let mut row = current.base;
    row.id = PhysicalFunctionRunId::new();
    Ok(row)
}

#[td_test::test(db)]
async fn load_scd2_insert(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_attempt(&db, &h).await?;
    let scoped = db.scoped(h.project_id);
    let t = Instant::now();
    (&scoped).scd2().insert(&row).await?;
    record("scd2::insert", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_scd2_insert_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let mut rows = Vec::with_capacity(8);
    for _ in 0..8 {
        rows.push(build_attempt(&db, &h).await?);
    }
    let scoped = db.scoped(h.project_id);
    let t = Instant::now();
    let r = (&scoped).scd2().chunked().insert(&rows).await?;
    record(
        "scd2::insert_chunked",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}
