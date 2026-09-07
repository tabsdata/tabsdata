//
// Copyright 2026 Tabsdata Inc.
//

//! Shared scaffolding for the per-family load cases.

use crate::dataset::{self, ChainHandle};
use crate::metric::{MetricRow, micros};
use crate::profile::SizeProfile;
use std::time::Instant;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_error::TdError;

/// Seed the chain dataset at the env-selected profile size. `seeded` is the
/// producer-LTDV count; queries scaling on the function-run count recompute
/// it from [`ChainHandle::fr_ids`].
pub(crate) async fn seed(db: &TdPool) -> Result<(ChainHandle, SizeProfile, usize), TdError> {
    let profile = SizeProfile::from_env();
    let handle = dataset::chain_dataset(db, profile.chain()).await?;
    let seeded = handle.ltdv_ids.len();
    Ok((handle, profile, seeded))
}

/// ANALYZE this test's own schema, so the plan-shape guards read real
/// statistics. Never a database-wide sweep: tests share one database and
/// isolate on a schema, so the sweep races every concurrent test's teardown.
pub(crate) async fn analyze(db: &TdPool, what: &str) {
    let mut conn = db
        .begin()
        .await
        .unwrap_or_else(|e| panic!("analyze {what}: {e:?}"));
    conn.analyze_current_schema()
        .await
        .unwrap_or_else(|e| panic!("analyze {what}: {e:?}"));
}

/// `t` is captured immediately before the query, so seeding is excluded.
pub(crate) fn record(
    case: &'static str,
    profile: SizeProfile,
    t: Instant,
    rows: usize,
    seeded: usize,
) {
    MetricRow {
        case,
        profile: profile.name(),
        wall_us: micros(t.elapsed()),
        rows,
        seeded,
    }
    .emit();
}

/// Min-of-3 timing for re-runnable reads: one jitter-inflated sample would
/// skew the scaling exponent. Writes use [`timed_once!`].
macro_rules! timed {
    ($out:ident, $name:literal, $body:expr) => {{
        let mut __best = f64::INFINITY;
        let mut __last = None;
        for _ in 0..3 {
            let __start = std::time::Instant::now();
            let __r = $body;
            __best = __best.min(__start.elapsed().as_micros() as f64);
            __last = Some(__r);
        }
        $out.push(($name, __best));
        __last.unwrap()
    }};
}
pub(crate) use timed;

/// Single-execution [`timed!`] for queries that mutate state.
macro_rules! timed_once {
    ($out:ident, $name:literal, $body:expr) => {{
        let __start = std::time::Instant::now();
        let __r = $body;
        $out.push(($name, __start.elapsed().as_micros() as f64));
        __r
    }};
}
pub(crate) use timed_once;
