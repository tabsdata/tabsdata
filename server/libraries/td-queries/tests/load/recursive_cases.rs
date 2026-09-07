//
// Copyright 2026 Tabsdata Inc.
//

//! Recursive-family load cases — the `latest()` and `as_of()` recursion
//! surfaces over the chain dataset's dependency edges, anchored at a seeded
//! function id.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{AtTime, Depth};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;

use crate::harness::{record, seed};

/// Both directions, generous depth.
fn options() -> Result<RecursionOptions, TdError> {
    let d = Depth::try_from(16)?;
    Ok(RecursionOptions::new(Some(d), Some(d)))
}

#[td_test::test(db)]
async fn load_recursive_current(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let opts = options()?;
    let t = Instant::now();
    let rows = db
        .scd2()
        .recursive::<DependencyDBWithNames, FunctionDB>(opts)
        .start_at(&h.consumer_function_id)
        .fetch_all()
        .await?;
    record("recursive::current", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_recursive_find_at(db: TdPool) -> Result<(), TdError> {
    let (h, profile, seeded) = seed(&db).await?;
    let at = AtTime::now();
    let opts = options()?;
    let t = Instant::now();
    let rows = db
        .scd2()
        .as_of(&at)
        .recursive::<DependencyDBWithNames, FunctionDB>(opts)
        .start_at(&h.consumer_function_id)
        .fetch_all()
        .await?;
    record("recursive::find_at", profile, t, rows.len(), seeded);
    Ok(())
}
