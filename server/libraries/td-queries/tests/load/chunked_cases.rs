//
// Copyright 2026 Tabsdata Inc.
//

//! The wordless grain's chunked operations and the SCD2 close, over the chain
//! dataset. The closes run on a table the dataset seeded, so a per-case
//! database keeps them from disturbing the other reads.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::logical::LogicalFunctionRunStatusUpdateDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunInputInsertDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunUpdateDB;
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::basic::{AtTime, PhysicalFunctionRunStatus};
use td_queries::Queries;

use crate::default_write_cases::build_lfri;
use crate::harness::{record, seed};

pub(crate) fn done_update() -> PhysicalFunctionRunUpdateDB {
    PhysicalFunctionRunUpdateDB {
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        status: PhysicalFunctionRunStatus::Done,
        ended_on_hour_ms: None,
        ended_on_day_ms: None,
        ended_on_week_ms: None,
        ended_on_month_ms: None,
        metric_table_versions: None,
        metric_rows: None,
        metric_data_bytes: None,
    }
}

#[td_test::test(db)]
async fn load_chunked_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = db
        .chunked()
        .fetch_all::<LogicalFunctionRunDB>(&h.execs, ())
        .await?;
    record("chunked::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_chunked_insert(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let rows: Vec<LogicalFunctionRunInputInsertDB> =
        (0..8).map(|_| build_lfri(&h)).collect::<Result<_, _>>()?;
    let scoped = db.scoped(h.project_id);
    let t = Instant::now();
    let r = (&scoped).chunked().insert(&rows).await?;
    record(
        "chunked::insert",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_chunked_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let update = LogicalFunctionRunStatusUpdateDB {
        status: LogicalFunctionRunStatus::Done,
    };
    let t = Instant::now();
    let r = db.chunked().update(&update, &h.producer_lfrs).await?;
    record(
        "chunked::update",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_scd2_update_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let update = done_update();
    let t = Instant::now();
    let r = db.scd2().chunked().update(&update, &h.fr_ids).await?;
    record(
        "scd2::update_chunked",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_chunked_close(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let now = AtTime::now();
    let ids = [h.table_id];
    let t = Instant::now();
    let r = db.scd2().chunked().close::<TableDB>(&ids, (), &now).await?;
    record(
        "chunked::close",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    assert_eq!(r.rows_affected(), 1);
    Ok(())
}

#[td_test::test(db)]
async fn load_default_close(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let now = AtTime::now();
    let t = Instant::now();
    let r = db.scd2().close::<TableDB>(&h.table_id, &now).await?;
    record(
        "default::close",
        profile,
        t,
        r.rows_affected() as usize,
        seeded,
    );
    assert_eq!(r.rows_affected(), 1);
    Ok(())
}
