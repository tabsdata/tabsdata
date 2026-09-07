//
// Copyright 2026 Tabsdata Inc.
//

//! Current- and Default-family read cases, plus Sessions::purge, over the chain
//! dataset. Current reads target `TableDB`; Default reads target `ActiveCollectionDB`.

use crate::harness::{record, seed};
use std::time::Instant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::WorkerType;
use td_queries::Queries;

#[td_test::test(db)]
async fn load_current_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let _row: TableDB = (&db).latest().fetch_one::<TableDB>(&h.table_id).await?;
    record("latest::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .latest()
        .fetch_optional::<TableDB>(&h.table_id)
        .await?;
    record(
        "latest::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = (&db).latest().fetch_all::<TableDB>(()).await?;
    record("latest::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let fvids = vec![h.producer_function_version_id.clone()];
    let t = Instant::now();
    let rows = (&db)
        .latest()
        .chunked()
        .fetch_all::<TableDB>(&fvids, ())
        .await?;
    record("latest::fetch_all_chunked", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_first(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db).latest().fetch_first::<TableDB>(()).await?;
    record(
        "latest::fetch_first",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_first(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db).fetch_first::<ActiveCollectionDB>(()).await?;
    record(
        "default::fetch_first",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let _row: ActiveCollectionDB = (&db)
        .fetch_one::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record("default::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .fetch_optional::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record(
        "default::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = (&db).fetch_all::<ActiveCollectionDB>(()).await?;
    record("default::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_last(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db
        .fetch_last::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record(
        "default::fetch_last",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_optional_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // `FOR UPDATE` needs the read-write transaction; a read connection is read-only.
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let row = conn
        .for_update()
        .fetch_optional::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
        .await?;
    record(
        "default::fetch_optional_for_update",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_fetch_one_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let row = conn
        .for_update()
        .fetch_one::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
        .await?;
    record("default::fetch_one_for_update", profile, t, 1, seeded);
    let _ = row;
    Ok(())
}

/// The list-level claim: every row the caller is about to decide on and write
/// back, locked in one statement.
#[td_test::test(db)]
async fn load_default_fetch_all_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let rows = conn
        .for_update()
        .fetch_all::<LogicalFunctionRunDB>(&h.producer_lfrs[0])
        .await?;
    record(
        "default::fetch_all_for_update",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_default_claim_all_in(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let rows = conn
        .for_update()
        .claim_all::<WorkerDB>(WorkerDB::triggers_to_schedule(WorkerType::CronTrigger))
        .await?;
    record("default::claim_all", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_sessions_purge(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    (&db).sessions().purge().await?;
    record("sessions::purge", profile, t, 0, seeded);
    Ok(())
}
