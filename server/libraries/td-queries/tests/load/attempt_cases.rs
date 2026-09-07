//
// Copyright 2026 Tabsdata Inc.
//

//! Attempts-family load cases over the chain dataset.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::{AtTime, TriggeredOn};
use td_queries::Queries;

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_attempts_current_run(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row: Option<PhysicalFunctionRunDB> = (&db)
        .latest()
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
    let row: Option<PhysicalTableDataVersionDB> = (&db)
        .latest()
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
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let row: Option<PhysicalTableDataVersionDB> = (&db)
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
async fn load_ltdv_latest_bound_tdv_with_data_for_table(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .table_data_versions()
        .latest_bound_tdv_with_data_for_table(&h.table_id, &h.before_time, false)
        .await?;
    record(
        "ltdv::latest_bound_tdv_with_data_for_table",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    Ok(())
}
