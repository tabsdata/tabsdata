//
// Copyright 2026 Tabsdata Inc.
//

//! Current- and Live-family read cases, plus Sessions::purge, over the chain
//! dataset. Current reads target `TableDB`; Live reads target `ActiveCollectionDB`.

use crate::harness::{record, seed};
use std::time::Instant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::WorkerType;
use td_queries::{CurrentEntry, LiveEntry, SessionsEntry};

#[td_test::test(db)]
async fn load_current_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let _row: TableDB = (&db)
        .current()
        .fetch_one::<TableDB>(&h.producer_function_version_id)
        .await?;
    record("current::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .current()
        .fetch_optional::<TableDB>(&h.producer_function_version_id)
        .await?;
    record(
        "current::fetch_optional",
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
    let rows = (&db).current().fetch_all::<TableDB>(()).await?;
    record("current::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_current_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let fvids = vec![h.producer_function_version_id.clone()];
    let t = Instant::now();
    let rows = (&db)
        .current()
        .fetch_all_chunked::<TableDB, _>(&fvids)
        .await?;
    record("current::fetch_all_chunked", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let _row: ActiveCollectionDB = (&db)
        .live()
        .fetch_one::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record("live::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .live()
        .fetch_optional::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record(
        "live::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let rows = (&db).live().fetch_all::<ActiveCollectionDB>(()).await?;
    record("live::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_last(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = db
        .live()
        .fetch_last::<ActiveCollectionDB>(&h.collection_id)
        .await?;
    record(
        "live::fetch_last",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_optional_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // `FOR UPDATE` needs the read-write transaction; a read connection is read-only.
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let row = conn
        .live()
        .fetch_optional_for_update::<PhysicalFunctionRunDB>(&h.fr_ids[0])
        .await?;
    record(
        "live::fetch_optional_for_update",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_fetch_one_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // `FOR UPDATE` needs the read-write transaction; a read connection is read-only.
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let row = conn
        .live()
        .fetch_one_for_update::<PhysicalFunctionRunDB>(&h.fr_ids[0])
        .await?;
    record("live::fetch_one_for_update", profile, t, 1, seeded);
    let _ = row;
    Ok(())
}

/// The list-level claim: every row the caller is about to decide on and write
/// back, locked in one statement.
#[td_test::test(db)]
async fn load_live_fetch_all_for_update(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    // `FOR UPDATE` needs the read-write transaction; a read connection is read-only.
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let rows = conn
        .live()
        .fetch_all_for_update::<PhysicalFunctionRunDB>(&h.fr_ids[0])
        .await?;
    record("live::fetch_all_for_update", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_live_claim_all_in(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    // `FOR UPDATE` needs the read-write transaction; a read connection is read-only.
    let mut conn = db.begin().await.expect("write transaction");
    let t = Instant::now();
    let rows = conn
        .live()
        .claim_all_in::<WorkerDB>(WorkerDB::triggers_to_schedule(WorkerType::CronTrigger), &())
        .await?;
    record("live::claim_all_in", profile, t, rows.len(), seeded);
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
