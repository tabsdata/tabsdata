//
// Copyright 2026 Tabsdata Inc.
//

//! Versioned- and Historic-family load cases over the chain dataset's
//! pointer log (partition = LTDV logical id, version order = `set_on`).

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::logical::LogicalTableDataVersionPointerDB;
use td_objects::types::basic::AtTime;
use td_queries::{HistoricEntry, VersionedEntry};

use crate::harness::{record, seed};

type Ptr = LogicalTableDataVersionPointerDB;

#[td_test::test(db)]
async fn load_versioned_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let _row: Ptr = (&db)
        .versioned(&at)
        .fetch_one::<Ptr>(&h.ltdv_ids[0])
        .await?;
    record("versioned::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_versioned_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let row = (&db)
        .versioned(&at)
        .fetch_optional::<Ptr>(&h.ltdv_ids[0])
        .await?;
    record(
        "versioned::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_versioned_fetch_all_for_partition(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let rows = (&db)
        .versioned(&at)
        .fetch_all_for_partition::<Ptr>(&h.ltdv_ids[0])
        .await?;
    record(
        "versioned::fetch_all_for_partition",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_versioned_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let rows = (&db)
        .versioned(&at)
        .fetch_all_chunked::<Ptr>(&h.ltdv_ids)
        .await?;
    record(
        "versioned::fetch_all_chunked",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let _row: Ptr = (&db).historic(&at).fetch_one::<Ptr>(&h.ltdv_ids[0]).await?;
    record("historic::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let row = (&db)
        .historic(&at)
        .fetch_optional::<Ptr>(&h.ltdv_ids[0])
        .await?;
    record(
        "historic::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    // `()` matches all → latest row per partition across the whole log.
    let rows = (&db).historic(&at).fetch_all::<Ptr>(()).await?;
    record("historic::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_historic_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let rows = (&db)
        .historic(&at)
        .fetch_all_chunked::<Ptr, _>(&h.ltdv_ids)
        .await?;
    record(
        "historic::fetch_all_chunked",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}
