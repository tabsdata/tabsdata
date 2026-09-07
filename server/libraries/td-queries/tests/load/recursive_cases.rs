//
// Copyright 2026 Tabsdata Inc.
//

//! Recursive-family load cases — the live/current/historic recursion
//! surfaces over the chain dataset's dependency edges, anchored at a seeded
//! function id.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::dependency::{DependencyDB, DependencyDBWithNames};
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{AtTime, Depth};
use td_objects::types::recursion::RecursionOptions;
use td_queries::{CurrentEntry, HistoricEntry, RecursiveEntry};

use crate::harness::{record, seed};

/// Both directions, generous depth.
fn options() -> Result<RecursionOptions, TdError> {
    let d = Depth::try_from(16)?;
    Ok(RecursionOptions::new(Some(d), Some(d)))
}

#[td_test::test(db)]
async fn load_recursive_live(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let opts = options()?;
    let t = Instant::now();
    let rows = (&db)
        .recursive::<DependencyDB, FunctionDB>(opts)
        .direct_reference(&h.consumer_function_id)
        .fetch_all()
        .await?;
    record("recursive::live", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_recursive_current(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let opts = options()?;
    let t = Instant::now();
    let rows = (&db)
        .current()
        .recursive::<DependencyDBWithNames, FunctionDB>(opts)
        .direct_reference(&h.consumer_function_id)
        .fetch_all()
        .await?;
    record("recursive::current", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_recursive_historic(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let opts = options()?;
    let t = Instant::now();
    let rows = (&db)
        .historic(&at)
        .recursive::<DependencyDBWithNames, FunctionDB>(opts)
        .direct_reference(&h.consumer_function_id)
        .fetch_all()
        .await?;
    record("recursive::historic", profile, t, rows.len(), seeded);
    Ok(())
}
