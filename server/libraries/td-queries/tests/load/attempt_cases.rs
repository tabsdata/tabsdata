//
// Copyright 2026 Tabsdata Inc.
//

//! Attempts-family load cases over the chain dataset.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::AtTime;
use td_queries::Queries;

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_attempts_current_run(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row: Option<PhysicalFunctionRunDB> = db
        .scd2()
        .fetch_optional::<PhysicalFunctionRunDB>(&h.producer_lfrs[0])
        .await?;
    record(
        "attempts::current_run",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some(), "the producer run has a current attempt");
    Ok(())
}

#[td_test::test(db)]
async fn load_attempts_current_version(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row: Option<PhysicalTableDataVersionDB> = db
        .scd2()
        .fetch_optional::<PhysicalTableDataVersionDB>(&h.ltdv_ids[0])
        .await?;
    record(
        "attempts::current_version",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(
        row.is_some(),
        "the producer version has a current materialization"
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_attempts_version_at(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let row: Option<PhysicalTableDataVersionDB> = db
        .scd2()
        .as_of(&at)
        .fetch_optional::<PhysicalTableDataVersionDB>(&h.ltdv_ids[0])
        .await?;
    record(
        "attempts::version_at",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some(), "the version has a materialization at now");
    Ok(())
}

#[td_test::test(db)]
async fn load_ltdv_committed_data_at(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db
        .table_data_versions()
        .committed_data_at(&h.table_id, &AtTime::from(h.before_time))
        .await?;
    record(
        "ltdv::committed_data_at",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_ltdv_committed_data_before(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let newest = h.ltdv_ids.last().expect("seeded logical versions");
    let t = Instant::now();
    let row = db
        .table_data_versions()
        .committed_data_before(newest)
        .await?;
    record(
        "ltdv::committed_data_before",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    Ok(())
}
