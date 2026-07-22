//
// Copyright 2026 Tabsdata Inc.
//

//! Pointer-family load cases over the chain dataset's pointer event log.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::logical::{
    LogicalFunctionRunPointerDB, LogicalPointerDao, LogicalTableDataVersionPointerDB,
};
use td_objects::types::basic::{AtTime, TriggeredOn, UserId};
use td_queries::{Bind, LfrEntry, LtdvEntry, OutputRedirect, ReplayRedirect, VersionedEntry};
use td_security::ENCODED_ID_SYSTEM;

use crate::harness::{record, seed};

#[td_test::test(db)]
async fn load_pointers_concrete(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db).ltdv().pointers().concrete(&h.ltdv_ids[0]).await?;
    record(
        "pointers::concrete",
        &db,
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some(), "producer LTDV is Bound to a PTDV");
    Ok(())
}

#[td_test::test(db)]
async fn load_pointers_at_concrete(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let t = Instant::now();
    let row = (&db)
        .ltdv()
        .pointers()
        .at(&at)
        .concrete(&h.ltdv_ids[0])
        .await?;
    record(
        "pointers::at_concrete",
        &db,
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    assert!(row.is_some(), "producer LTDV is Bound at now");
    Ok(())
}

#[td_test::test(db)]
async fn load_pointers_bind_all(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let user = UserId::try_from(ENCODED_ID_SYSTEM)?;
    let at = AtTime::now();
    // Every seeded pointer is Bound, so all binds come back deferred and no
    // event is written — times the batched current fetch + state match.
    let binds: Vec<_> = h
        .ltdv_ids
        .iter()
        .map(|id| Bind {
            project_id: h.project_id,
            logical_id: id.clone(),
            new_concrete_id: h.producer_tdv_id.clone(),
        })
        .collect();
    let t = Instant::now();
    let deferred = (&db).ltdv().pointers().bind_all(&binds, &at, &user).await?;
    record(
        "pointers::bind_all",
        &db,
        profile,
        t,
        deferred.len(),
        seeded,
    );
    assert_eq!(
        deferred.len(),
        h.ltdv_ids.len(),
        "all seeded LTDVs are Bound"
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_pointers_redirect_replay_attempts(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    // Each LFR reports the PFR it is already bound to — not a replay attempt,
    // so nothing is appended; times the batched current fetch + guard.
    let latest: Vec<LogicalFunctionRunPointerDB> = (&db)
        .versioned(&at)
        .fetch_all_chunked::<LogicalFunctionRunPointerDB>(&h.producer_lfrs)
        .await?;
    let items: Vec<_> = latest
        .iter()
        .filter_map(|p| {
            Some(ReplayRedirect {
                project_id: p.project_id,
                logical_function_run_id: p.logical_id().clone(),
                new_physical_function_run_id: p.concrete_id().cloned()?,
                reporting_triggered_on: TriggeredOn::now(),
                set_by_id: p.set_by_id,
            })
        })
        .collect();
    let t = Instant::now();
    let redirected = (&db)
        .lfr()
        .pointers()
        .redirect_replay_attempts(&items, &at)
        .await?;
    record(
        "pointers::redirect_replay_attempts",
        &db,
        profile,
        t,
        items.len(),
        seeded,
    );
    assert!(redirected.is_empty(), "same-PFR reports are not replays");
    Ok(())
}

#[td_test::test(db)]
async fn load_pointers_redirect_outputs(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    // Redirect each LTDV to its already-bound PTDV — idempotent, no events.
    let latest: Vec<LogicalTableDataVersionPointerDB> = (&db)
        .versioned(&at)
        .fetch_all_chunked::<LogicalTableDataVersionPointerDB>(&h.ltdv_ids)
        .await?;
    let items: Vec<_> = latest
        .iter()
        .filter_map(|p| {
            Some(OutputRedirect {
                project_id: p.project_id,
                logical_table_data_version_id: p.logical_id().clone(),
                new_physical_table_data_version_id: p.concrete_id().cloned()?,
                set_by_id: p.set_by_id,
            })
        })
        .collect();
    let t = Instant::now();
    (&db)
        .ltdv()
        .pointers()
        .redirect_outputs(&items, &at)
        .await?;
    record(
        "pointers::redirect_outputs",
        &db,
        profile,
        t,
        items.len(),
        seeded,
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_ltdv_latest_bound_tdv_with_data_for_table(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let t = Instant::now();
    let row = (&db)
        .ltdv()
        .latest_bound_tdv_with_data_for_table(&h.table_id, &h.before_time, false)
        .await?;
    record(
        "ltdv::latest_bound_tdv_with_data_for_table",
        &db,
        profile,
        t,
        row.is_some() as usize,
        seeded,
    );
    Ok(())
}
