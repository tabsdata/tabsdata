//
// Copyright 2026 Tabs Data Inc.
//

//! The **shared** embedded postgres used by the whole test suite (the
//! `#[td_test::test(db)]` macro path). One server per worktree
//! (`<root>/target/td-test-pg/data`, on a worktree-stable fixed port), reused
//! across every test and process.
//!
//! It is started **lazily by the first test that needs it** via [`ensure_started`]
//! — the test runner can't pre-start it (`__os__setup_test_rs` runs before the
//! build).
//!
//! **Readiness is a real connection, never a pid file.** The server lives on a
//! fixed, worktree-derived port ([`admin_port`]); a process [`probe`]s that port
//! and, if it answers, just connects. This sidesteps the `postmaster.pid` race
//! entirely: the pid file exists (with a placeholder port) while `initdb`
//! bootstraps and before the postmaster accepts connections, so reading the port
//! from it could dial a dead endpoint. With the port fixed there is nothing to
//! read.
//!
//! The cold boot is still serialized by a cross-process file lock: concurrent
//! `initdb` on the one shared `data_dir` would corrupt it, and the fixed port
//! cannot be the lock because `initdb` runs before the port bind. The lock is
//! contended only on the first boot (or after a wipe) — once a server answers,
//! the probe fast-path returns without touching it. `cargo make test` stops +
//! wipes the server in `__os__cleanup_test_rs` (cross-platform), so it leaves
//! nothing behind; plain `cargo test` / `cargo nextest run` do not run that task,
//! so they leave it warm and reuse it. Each test isolates itself on its own
//! uniquely-named schema (see [`super::postgres`]).

use crate::config::EmbeddedConfig;
use crate::pool::connect::redacted_url;
use sqlx::postgres::PgPoolOptions;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::OnceCell;
use url::Url;

/// How long a single readiness probe waits for a connection before deeming the
/// server not (yet) up, and how the boot loop paces re-probes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const BOOT_ATTEMPTS: u32 = 120;
const BOOT_RETRY: Duration = Duration::from_millis(250);

/// `<root>/target/td-test-pg/data` — the shared server's data directory. Stable
/// per-worktree (separate `target/`), so every test process agrees on which
/// server to use.
pub fn data_dir() -> PathBuf {
    td_common::testdir::root()
        .join("target")
        .join("td-test-pg")
        .join("data")
}

/// A fixed loopback port for **this worktree's** shared server, derived from the
/// workspace root. Stable across runs and processes of the same build (so they
/// all agree without reading any runtime state) and distinct per worktree (so
/// concurrent worktrees don't share — or fight over — one server). Range
/// `50000..=59999`.
fn admin_port() -> u16 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    td_common::testdir::root().hash(&mut hasher);
    50000 + (hasher.finish() % 10000) as u16
}

/// Boot config for the shared server: persistent (survives across tests/runs),
/// fixed `data_dir`, fixed worktree-derived port (so the live URL is known
/// without reading `postmaster.pid`).
pub fn embedded_config() -> EmbeddedConfig {
    EmbeddedConfig {
        data_dir: Some(data_dir()),
        port: Some(admin_port()),
        persistent: true,
        username: None,
        password: None,
    }
}

static STARTED: OnceCell<()> = OnceCell::const_new();

/// Whether the shared server is up and **accepting connections** right now: a
/// real connect to the fixed port with the persisted superuser credentials.
/// `false` if the server is down, still starting, or the password file does not
/// exist yet (a cold data dir) — every "not ready" reason collapses to a failed
/// connect, which is exactly the signal the boot loop wants.
async fn probe() -> bool {
    let cfg = embedded_config();
    let Ok((mut url, user, pass)) = crate::embedded::live_admin(&cfg) else {
        return false;
    };
    url.set_username(&user).ok();
    url.set_password(Some(&pass)).ok();
    match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(PROBE_TIMEOUT)
        .connect(url.as_str())
        .await
    {
        Ok(pool) => {
            pool.close().await;
            true
        }
        Err(_) => false,
    }
}

/// Ensure the shared server is up and accepting connections — idempotent. Within
/// a process an `OnceCell` runs this once (concurrent `cargo test` threads await
/// it). The fast path is a [`probe`]: if the fixed-port server already answers,
/// return. Only a cold/unreachable server takes the cross-process file lock to
/// serialize the boot — concurrent `initdb` on the one shared `data_dir` would
/// corrupt it. The lock is held only across the boot and is contended only on the
/// first boot; once a server answers, no later caller touches it.
pub async fn ensure_started() {
    STARTED
        .get_or_init(|| async {
            // Fast path: the server already answers — just use it.
            if probe().await {
                return;
            }
            // `<root>/target/td-test-pg` — parent of the data dir, holds the lock.
            let base = data_dir()
                .parent()
                .expect("test pg data dir has a parent")
                .to_path_buf();
            let _ = std::fs::create_dir_all(&base);

            // `lock()` blocks, so take it on a blocking thread; `LockFile` is
            // `Send`, so it can be held across the await and dropped after.
            let lock_path = base.join(".start.lock");
            let mut lock = tokio::task::spawn_blocking(move || {
                let mut lock =
                    fslock::LockFile::open(lock_path.as_path()).expect("open shared test pg lock");
                lock.lock().expect("acquire shared test pg lock");
                lock
            })
            .await
            .expect("lock task");

            // A peer may have booted while we waited for the lock.
            if !probe().await {
                // `boot_detached` (not `start_detached`) so the boot is decided by
                // the connect-probe above, not by a possibly-stale `postmaster.pid`;
                // its boot is Drop-safe, so even a lost race leaves a peer's server
                // untouched.
                crate::embedded::boot_detached(&embedded_config())
                    .await
                    .expect("boot shared test postgres");
                // `pg_ctl start -w` already waits for readiness, but confirm via a
                // real connection (and tolerate any final lag).
                let mut accepting = false;
                for _ in 0..BOOT_ATTEMPTS {
                    if probe().await {
                        accepting = true;
                        break;
                    }
                    tokio::time::sleep(BOOT_RETRY).await;
                }
                assert!(
                    accepting,
                    "shared test postgres booted but never accepted connections"
                );
            }
            let _ = lock.unlock();
        })
        .await;
}

/// Redacted base URL + superuser credentials for the running shared server, for
/// building per-schema admin and serving connectables. Call after [`ensure_started`].
pub fn admin_base() -> (Url, String, String) {
    let (url, user, pass) = crate::embedded::live_admin(&embedded_config())
        .expect("shared test postgres must be running (call ensure_started first)");
    (redacted_url(&url), user, pass)
}
