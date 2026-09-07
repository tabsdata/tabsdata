//
// Copyright 2026 Tabsdata Inc.
//

//! At- and FindAt-family load cases over the chain dataset's
//! materializations (partition = logical version id, version order =
//! `triggered_on`).

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::types::basic::{AtTime, TriggeredOn};
use td_queries::Queries;

use crate::harness::{record, seed};

type Ptdv = PhysicalTableDataVersionDB;

#[td_test::test(db)]
async fn load_find_at_fetch_one_by_partition(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let _row: Ptdv = (&db).as_of(&at).fetch_one::<Ptdv>(&h.ltdv_ids[0]).await?;
    record("as_of::fetch_one_by_partition", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_optional_by_partition(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let row = (&db)
        .as_of(&at)
        .fetch_optional::<Ptdv>(&h.ltdv_ids[0])
        .await?;
    record(
        "as_of::fetch_optional_by_partition",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_all_chunked_by_partition(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let rows = (&db)
        .as_of(&at)
        .chunked()
        .fetch_all::<Ptdv>(&h.ltdv_ids, ())
        .await?;
    record(
        "as_of::fetch_all_chunked_by_partition",
        profile,
        t,
        rows.len(),
        seeded,
    );
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_one(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let _row: Ptdv = (&db).as_of(&at).fetch_one::<Ptdv>(&h.ltdv_ids[0]).await?;
    record("as_of::fetch_one", profile, t, 1, seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let row = (&db)
        .as_of(&at)
        .fetch_optional::<Ptdv>(&h.ltdv_ids[0])
        .await?;
    record(
        "as_of::fetch_optional",
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_all(db: TdPool) -> Result<(), TdError> {
    let (_h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    // `()` matches all → latest row per partition across the whole log.
    let rows = (&db).as_of(&at).fetch_all::<Ptdv>(()).await?;
    record("as_of::fetch_all", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn load_find_at_fetch_all_chunked(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = TriggeredOn::try_from(AtTime::now())?;
    let t = Instant::now();
    let rows = (&db)
        .as_of(&at)
        .chunked()
        .fetch_all::<Ptdv>(&h.ltdv_ids, ())
        .await?;
    record("as_of::fetch_all_chunked", profile, t, rows.len(), seeded);
    assert!(!rows.is_empty());
    Ok(())
}
