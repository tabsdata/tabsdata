//
// Copyright 2026 Tabsdata Inc.
//

//! The logical and physical run status doors over the chain dataset: the row update
//! plus the metrics facts and the parents' roll-up each one carries.

use std::time::Instant;

use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::Grant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::WorkerExecutionUpdate;
use td_objects::types::basic::{AtTime, LogicalFunctionRunStatus};
use td_queries::Queries;
use td_queries::test_utils::begin_exclusive;

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_lfr_set_status(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let mut trx = begin_exclusive(&db, h.project_id).await?;
    let conn = trx.get(Grant::asserted("load case"));
    let t = Instant::now();
    conn.logical_runs()
        .set_status(&h.producer_lfrs[..1], LogicalFunctionRunStatus::Done)
        .await?;
    record("lfr::set_status", profile, t, 1, seeded);
    trx.commit().await?;
    Ok(())
}

#[td_test::test(db)]
async fn load_pfr_reported(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let physical_run: PhysicalFunctionRunDB = db.scd2().fetch_one(&h.producer_lfrs[0]).await?;
    let update = WorkerExecutionUpdate {
        started_on: Some(AtTime::now().into()),
        ended_on: Some(AtTime::now().into()),
        status: WorkerCallbackStatus::Done,
    };
    let mut trx = begin_exclusive(&db, h.project_id).await?;
    let conn = trx.get(Grant::asserted("load case"));
    let t = Instant::now();
    conn.physical_runs()
        .reported(&physical_run, &update)
        .await?;
    record("pfr::reported", profile, t, 1, seeded);
    trx.commit().await?;
    Ok(())
}
