//
// Copyright 2026 Tabsdata Inc.
//

//! Base-path stamping, end to end. A run's storage prefix is resolved ONCE, at
//! mint — `COALESCE(functions.base_path, collection effective)` — and frozen
//! onto both the `physical_function_runs` row and every output
//! `physical_table_data_versions` row. Reads rebuild paths from the stamped
//! value, never from a live lookup, so moving a collection's `base_path`
//! redirects future runs without orphaning what past runs wrote.
//!
//! The only prefix that can move in place is `collections.base_path`
//! (set/clear). A project's `base_path` is create-only, and a function
//! override is immutable per function version — a new override is a new
//! version, and a run pins its version.
//!
//! - [`stamping`] — mint-time resolution: the override/collection/project
//!   ladder, FR ≡ TDV symmetry, dispatch batch and replay paths.
//! - [`drift`] — a `base_path` move after mint: frozen rows, fresh
//!   re-resolution for new mints and new attempts.
//! - [`reads`] — the download endpoints keep serving pre-move data (the
//!   failure the frozen column exists to prevent).

mod drift;
mod reads;
mod stamping;

use crate::logical::dispatch_mint::{MintedDispatch, mint_function_runs_on_dispatch};
use td_database::access::{InProject, LockInstant};
use td_database::connection::PgConnectionExt;
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
use td_objects::types::basic::{AtTime, PhysicalFunctionRunId};
use td_queries::Queries;

/// Dispatch-mint `ready` and commit, so reads observe what mint wrote.
async fn mint_committed(
    db: &TdPool,
    ready: &[LogicalFunctionRunDB],
) -> Result<Vec<MintedDispatch>, TdError> {
    let mut conn = DomainConnection::<Apiserver, InProject>::new(db.begin().await.unwrap());
    if let Some(first) = ready.first() {
        conn.scope_in_project(&first.project_id.to_string())
            .await
            .unwrap();
    }
    let minted =
        mint_function_runs_on_dispatch(&mut conn, ready, LockInstant::asserted(*AtTime::now()))
            .await?;
    conn.commit().await.unwrap();
    Ok(minted)
}

async fn fetch_run(
    db: &TdPool,
    id: &PhysicalFunctionRunId,
) -> Result<PhysicalFunctionRunDBWithNames, TdError> {
    let run = db
        .timeline()
        .fetch_one::<PhysicalFunctionRunDBWithNames>(id)
        .await?;
    Ok(run)
}
