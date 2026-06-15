//
// Copyright 2026 Tabs Data Inc.
//

//! Postgres test pool factory. Hands back a `TdPool` that owns its own
//! in-process embedded postgres via [`BackendConfig::EmbeddedPostgres`].
//!
//! # Concurrency cap
//!
//! Each call boots a fresh embedded server — its own port, data dir,
//! postmaster + auxiliary process tree (bgwriter, walwriter, autovacuum,
//! checkpointer, stats), and **its own System V IPC resources** (one shared
//! memory segment for the buffer pool / lock tables, plus a semaphore
//! array). These are scarce, kernel-wide limits, not per-process: on macOS
//! `kern.sysv.shmmni` defaults to ~32 segments and `kern.sysv.semmni` is
//! similarly tight; on Linux they're higher but still finite. Run
//! `ipcs -a` to inspect what's allocated and `ipcs -l` (Linux) / `sysctl
//! kern.sysv` (macOS) to see the limits. Once they're exhausted, new
//! instances fail to start with cryptic "could not create shared memory
//! segment" errors that look like disk-full but aren't. Unbounded
//! parallelism also multiplies fd usage, RAM (each instance allocates its
//! own `shared_buffers`), and disk churn from the temp data dirs.
//!
//! To keep usage bounded, `#[td_test::test(db)]` file-locks postgres tests
//! across processes using `serial_test::file_serial` with round-robin lock
//! names (`postgres_0..postgres_{N-1}`). The default cap is `N = 8`;
//! override via the `TD_TEST_POSTGRES_PARALLELISM` env var at compile time
//! of the test crate (changes require a rebuild — it's `std::env::var`,
//! not `tracked_env`). The lock is OS-enforced so it releases on panic,
//! abort, or `kill -9`; if a run does crash with IPC resources still
//! lingering, `ipcrm` removes the orphaned segments.

use crate::config::common::TEST_SCHEMA;
use crate::config::{BackendConfig, DatabaseConfig, EmbeddedConfig, Schema};
use crate::pool::TdPool;

/// Boot a fresh embedded postgres and connect a `TdPool` to it. The server
/// is wiped and stopped automatically when the returned `TdPool` (and any
/// of its clones) drops — `EmbeddedConfig::persistent = false` makes the
/// underlying `postgresql_embedded` handle treat the data dir as temporary,
/// so `pg_ctl stop` runs and the data dir is removed on `Drop`.
///
/// # Project scope in tests
///
/// The production connection path is fail-closed (no session GUC). In test
/// builds the `td-test` feature is on, so `connect_postgres`'s `after_connect`
/// runs `SET app.bypass_rls = on` on every serving connection (the statement is
/// `#[cfg(feature = "td-test")]`, compiled out in production). Thus RLS is
/// **disabled by default** in tests: a plain `db.live()` reads across all
/// projects, the same as a `scope = Global` request. To scope a query — for a
/// field-less insert whose `project_id` comes from the GUC, or to exercise true
/// cross-project isolation — use `db.scoped(p)` (or go through a service, whose
/// `ProjectScopeProvider` sets `SET LOCAL app.project_id` from the request's
/// project); either clears the bypass and re-enforces RLS for that transaction.
pub async fn pool() -> TdPool {
    let config = DatabaseConfig {
        conn: Some(BackendConfig::embedded_postgres(EmbeddedConfig {
            data_dir: None,
            port: None,
            persistent: false,
            username: None,
            password: None,
        })),
        schema: Some(Schema::new(TEST_SCHEMA)),
        pool: Default::default(),
    };
    TdPool::connect(&config).await.expect("postgres test pool")
}
