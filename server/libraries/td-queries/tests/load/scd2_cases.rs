//
// Copyright 2026 Tabsdata Inc.
//

//! The interval writes on the chain dataset: a new attempt version, one row
//! and chunked, and a pending materialization minted and settled.

use std::time::Instant;

use td_common::datetime::Timestamp;
use td_database::access::LockInstant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunInsertDB};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionInsertDB, PhysicalTableDataVersionUpdateDB,
};
use td_objects::types::basic::{HasData, PhysicalFunctionRunId, PhysicalTableDataVersionId};
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
async fn load_scd2_open(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_attempt(&db, &h).await?;
    let scoped = db.scoped(h.project_id);
    let at = LockInstant::asserted(Timestamp::default());
    let t = Instant::now();
    (&scoped).scd2().open(&row, at).await?;
    record("scd2::open", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_scd2_open_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let mut rows = Vec::with_capacity(8);
    for _ in 0..8 {
        rows.push(build_attempt(&db, &h).await?);
    }
    let scoped = db.scoped(h.project_id);
    let at = LockInstant::asserted(Timestamp::default());
    let t = Instant::now();
    let r = (&scoped).scd2().chunked().open(&rows, at).await?;
    record(
        "scd2::open_chunked",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

/// A new attempt of the handle's first run, and (not inserted) its
/// materialization of that run's first output.
async fn build_materialization(
    db: &TdPool,
    h: &ChainHandle,
) -> Result<PhysicalTableDataVersionInsertDB, TdError> {
    let written: Vec<PhysicalTableDataVersionDB> = db.timeline().fetch_all(&h.fr_ids[0]).await?;
    let mut row = written
        .into_iter()
        .next()
        .expect("the seeded run wrote a table")
        .base;
    let attempt = build_attempt(db, h).await?;
    (&db.scoped(h.project_id))
        .scd2()
        .open(&attempt, LockInstant::asserted(Timestamp::default()))
        .await?;
    row.id = PhysicalTableDataVersionId::new();
    row.physical_function_run_id = attempt.id;
    Ok(row)
}

#[td_test::test(db)]
async fn load_scd2_pending_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_materialization(&db, &h).await?;
    let scoped = db.scoped(h.project_id);
    let at = LockInstant::asserted(Timestamp::default());
    let t = Instant::now();
    let r = (&scoped).table_data_versions().pending(&[row], at).await?;
    record(
        "scd2::pending_chunked",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_scd2_settle(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let row = build_materialization(&db, &h).await?;
    let scoped = db.scoped(h.project_id);
    let at = LockInstant::asserted(Timestamp::default());
    (&scoped)
        .table_data_versions()
        .pending(std::slice::from_ref(&row), at)
        .await?;
    let reported = PhysicalTableDataVersionUpdateDB {
        has_data: Some(HasData::TRUE),
        schema_changed: None,
        column_count: None,
        row_count: None,
        data_bytes: None,
        schema_hash: None,
        with_data_table_data_version_id: None,
    };
    let t = Instant::now();
    let r = (&scoped)
        .table_data_versions()
        .settle(&reported, &row.id, at)
        .await?;
    record(
        "scd2::settle",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}
