//
// Copyright 2026 Tabsdata Inc.
//

//! The timeline grain: every version of a partition regardless of validity,
//! one version by its id, and the listings over it, on the chain dataset's
//! producer tables.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::ListParams;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{Table, TableDB};
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::AtTime;
use td_queries::Queries;

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_timeline_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let _row: FunctionDB = db
        .timeline()
        .fetch_one(&h.producer_function_version_id)
        .await?;
    record("timeline::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_timeline_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row: Option<FunctionDB> = db
        .timeline()
        .fetch_optional(&h.producer_function_version_id)
        .await?;
    record(
        "timeline::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = db.timeline().fetch_all::<TableDB>(&h.table_id).await?;
    record("history::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_fetch_first(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db.timeline().fetch_first::<TableDB>(&h.table_id).await?;
    record(
        "history::fetch_first",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_fetch_last(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db.timeline().fetch_last::<TableDB>(&h.table_id).await?;
    record(
        "history::fetch_last",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let fvids = vec![h.producer_function_version_id.clone()];
    let t = Instant::now();
    let rows = db
        .timeline()
        .chunked()
        .fetch_all::<TableDB>(&fvids, ())
        .await?;
    record("history::fetch_all_chunked", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_list(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db.timeline().list::<Table>((), &params).await?;
    record("history::list", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_history_list_until(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let now = AtTime::now();
    let params = ListQueryParams::<Table>::try_from(&ListParams::default())?;
    let t = Instant::now();
    let rows = db
        .timeline()
        .list_until::<Table, _>(&now, (), &params)
        .await?;
    record("history::list_until", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}
