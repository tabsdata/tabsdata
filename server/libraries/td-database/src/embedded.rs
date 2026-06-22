//
// Copyright 2026 Tabs Data Inc.
//

//! Embedded Postgres lifecycle. The server is brought up **detached**
//! ([`boot_detached`]/[`start_detached`]) and torn down ([`stop_detached`]) out
//! of band — by `td_dbmanager start`/`stop` in production and by the shared test
//! server ([`crate::test_utils::server`]) under tests. Nothing boots it at connect
//! time: [`DatabaseConfig::connect`](crate::config::DatabaseConfig) only ever
//! connects to a URL, and an embedded backend is resolved to its live URL first.
//!
//! The postmaster is daemonized by `pg_ctl start`, so [`boot`] simply
//! `mem::forget`s the `PostgreSQL` handle: on success the daemon outlives this
//! process; on failure we never started one, so forgetting also avoids the
//! handle's `Drop` running `pg_ctl stop` against a server a *sibling* booted on
//! the same data_dir. The running server's only owner is therefore the on-disk
//! state, read back by [`read_postmaster`] / [`live_url`].

use crate::config::EmbeddedConfig;
use crate::error::DbError;
use postgresql_embedded::{PostgreSQL, Settings, VersionReq};
use std::path::{Path, PathBuf};
use tracing::info;
use url::Url;

/// Exact embedded PostgreSQL version. Read at compile time from the same
/// `POSTGRESQL_VERSION` that `postgresql_embedded` uses to select the bundled
/// archive, so the runtime requirement cannot drift from what was bundled.
const EMBEDDED_POSTGRES_VERSION: &str = env!("POSTGRESQL_VERSION");

const PASSWORD_FILE: &str = ".td_embedded_password";

const OWNER_PASSWORD_FILE_PREFIX: &str = ".td_embedded_owner_password.";

/// PID file written by postgres on start and removed on clean shutdown.
const POSTMASTER_PID_FILE: &str = "postmaster.pid";

/// Status line (8th line of `postmaster.pid`) a postmaster writes only once it is
/// accepting connections. Earlier states are `starting`/`stopping`; `initdb`'s
/// standalone bootstrap backend writes no status line at all.
const POSTMASTER_STATUS_READY: &str = "ready";

/// Settings locating the server on `cfg`'s `data_dir`/`port` — enough to resolve
/// the install and [`stop`](PostgreSQL::stop) it, which needs no credentials.
/// Touches no password file. [`boot_settings`] layers the superuser password on
/// for the boot path.
fn base_settings(cfg: &EmbeddedConfig) -> Settings {
    let mut settings = Settings {
        version: VersionReq::parse(EMBEDDED_POSTGRES_VERSION)
            .expect("EMBEDDED_POSTGRES_VERSION must be a valid version requirement"),
        temporary: !cfg.persistent,
        ..Settings::default()
    };
    if let Some(dir) = cfg.data_dir.clone() {
        settings.data_dir = dir;
    }
    if let Some(port) = cfg.port {
        settings.port = port;
    }
    if let Some(username) = cfg.username.clone() {
        settings.username = username;
    }
    // Raise `max_connections` well above PostgreSQL's default (100): one embedded
    // server now backs the whole test suite, and a high-concurrency runner opens
    // many pools at once (each test's admin + serving + teardown pools), so give
    // generous headroom. One server, so this is a single larger shared-memory segment.
    settings
        .configuration
        .insert("max_connections".to_string(), "500".to_string());
    settings
}

/// [`base_settings`] plus the superuser password, for booting: a persistent cluster
/// must be reached with the same credentials `initdb` baked in, so when none is
/// configured the password persisted on first boot is loaded (created if absent).
fn boot_settings(cfg: &EmbeddedConfig) -> Result<Settings, DbError> {
    let mut settings = base_settings(cfg);
    if let Some(password) = cfg.password.clone() {
        settings.password = password;
    } else if cfg.persistent {
        settings.password = load_or_create_password(&settings.data_dir)?;
    }
    Ok(settings)
}

/// The data directory the server boots from: the configured one, else the
/// `postgresql_embedded` default.
fn data_dir_of(cfg: &EmbeddedConfig) -> PathBuf {
    cfg.data_dir
        .clone()
        .unwrap_or_else(|| Settings::default().data_dir)
}

/// Install/initialize (idempotent) then start `server`, returning the URL it
/// listens on.
///
/// **Always forgets the handle.** The `pg_ctl start` postmaster is daemonized, so
/// dropping the `PostgreSQL` would run its `Drop` — `pg_ctl stop` whenever
/// `status()` is `Started`, which is merely "`postmaster.pid` exists". On success
/// we want the daemon to outlive this process; on failure (e.g. a concurrent boot
/// lost the data_dir race) we never started one, and a `Drop` would stop the
/// *sibling* that owns the data_dir. Forgetting unconditionally is correct either
/// way — the running server's only owner is the on-disk state.
async fn boot(mut server: PostgreSQL) -> Result<Url, DbError> {
    let booted = async {
        server
            .setup()
            .await
            .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
        server
            .start()
            .await
            .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
        Url::parse(&server.settings().url("postgres"))
            .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))
    }
    .await;
    std::mem::forget(server);
    booted
}

/// Boot the embedded server **detached** unconditionally — no reuse check — and
/// leave the postmaster running past this process's exit. The caller is
/// responsible for not booting over a server it wants to keep (`start_detached`
/// guards on [`read_postmaster`]; the test harness connect-probes first). A boot
/// that loses a race to an already-running postmaster fails cleanly (see [`boot`])
/// rather than disturbing the winner.
pub async fn boot_detached(cfg: &EmbeddedConfig) -> Result<(), DbError> {
    let url = boot(PostgreSQL::new(boot_settings(cfg)?)).await?;
    let mut redacted = url;
    redacted.set_username("").ok();
    redacted.set_password(None).ok();
    info!("embedded postgres started, listening at {redacted}");
    Ok(())
}

/// Start the embedded server **detached** and idempotent: if one is already
/// running and accepting connections on `cfg`'s `data_dir`, reuse it; otherwise
/// boot one and leave the postmaster running past this process's exit. Its
/// lifecycle then belongs to a later [`stop_detached`] (e.g. `td_dbmanager stop`).
/// Persistent embedded backends only.
pub async fn start_detached(cfg: &EmbeddedConfig) -> Result<(), DbError> {
    if read_postmaster(&data_dir_of(cfg)).is_some_and(|pm| pm.accepting()) {
        info!("embedded postgres already running; reusing it");
        return Ok(());
    }
    boot_detached(cfg).await
}

/// Stop an embedded server that may have been started by another process. Resolves
/// the server for `cfg`'s `data_dir` and runs `pg_ctl stop --wait`, which blocks
/// until the postmaster exits. Idempotent: a no-op when nothing is running.
pub async fn stop_detached(cfg: &EmbeddedConfig) -> Result<(), DbError> {
    if read_postmaster(&data_dir_of(cfg)).is_none() {
        info!("embedded postgres not running; nothing to stop");
        return Ok(());
    }
    let mut server = PostgreSQL::new(base_settings(cfg));
    // Resolve the installation so `pg_ctl` is locatable; idempotent and does not
    // touch the running server (install + initialize are already done).
    server
        .setup()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
    server
        .stop()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
    // After a successful stop `status()` is `Stopped`, so the `PostgreSQL` `Drop`
    // (which only stops a `Started` server) is a no-op — no double stop.
    info!("embedded postgres stopped");
    Ok(())
}

/// Admin (superuser) connectable for an **already-running** embedded server: its
/// live URL plus the superuser identity (configured `username`/`password`, else
/// the `postgresql_embedded` default username and the password persisted at first
/// boot). Does not boot the server; errors when none is running. Lets a separate
/// process (`td_dbmanager create`/`upgrade`) provision the server that
/// `td_dbmanager start` brought up.
pub fn live_admin(cfg: &EmbeddedConfig) -> Result<(Url, String, String), DbError> {
    let url = live_url(cfg)?;
    let username = cfg
        .username
        .clone()
        .unwrap_or_else(|| Settings::default().username);
    let password = match cfg.password.clone() {
        Some(password) => password,
        None => load_password(&data_dir_of(cfg))?,
    };
    Ok((url, username, password))
}

/// URL of an already-running embedded server, derived without booting it: the
/// configured fixed port, else the live port from `postmaster.pid` in `data_dir`.
/// Lets a separate process (e.g. `dbmanager ping`) reach the server the apiserver
/// booted. Errors when no server is running. The returned URL carries placeholder
/// credentials; callers set the real ones.
pub fn live_url(cfg: &EmbeddedConfig) -> Result<Url, DbError> {
    let port = match cfg.port {
        Some(port) => port,
        None => read_postmaster(&data_dir_of(cfg))
            .filter(|pm| pm.pid > 0)
            .ok_or_else(|| DbError::EmbeddedPostgres("no running embedded server".to_string()))?
            .port,
    };
    let settings = Settings {
        port,
        ..Settings::default()
    };
    Url::parse(&settings.url("postgres")).map_err(|e| DbError::EmbeddedPostgres(e.to_string()))
}

/// The relevant fields of a `postmaster.pid` file (PG writes one line each):
/// line 1 PID, line 4 port, line 8 status. `initdb`'s standalone bootstrap
/// backend writes a transient file with a *negative* PID and the placeholder
/// port 5432 (no network socket) and no status line — hence [`Self::accepting`].
struct Postmaster {
    pid: i64,
    port: u16,
    ready: bool,
}

impl Postmaster {
    /// A real postmaster that is up and accepting connections: a positive PID
    /// (not `initdb`'s standalone backend) and the `ready` status line.
    fn accepting(&self) -> bool {
        self.pid > 0 && self.ready
    }
}

/// Parse `postmaster.pid` in `data_dir`. `None` when absent or lacking the PID +
/// port lines (no server, or a file too partial to use). The single reader behind
/// every liveness/readiness/port question, so they cannot diverge.
fn read_postmaster(data_dir: &Path) -> Option<Postmaster> {
    let content = std::fs::read_to_string(data_dir.join(POSTMASTER_PID_FILE)).ok()?;
    let lines: Vec<&str> = content.lines().collect();
    let pid = lines.first()?.trim().parse::<i64>().ok()?;
    let port = lines.get(3)?.trim().parse::<u16>().ok()?;
    let ready = lines.get(7).is_some_and(|s| s.trim() == POSTMASTER_STATUS_READY);
    Some(Postmaster { pid, port, ready })
}

/// Read the persisted superuser password from `<data_dir>/../PASSWORD_FILE`,
/// or generate a fresh one and write it on first boot. Lives outside the
/// data_dir itself because `initdb` refuses a non-empty data_dir.
fn load_or_create_password(data_dir: &Path) -> Result<String, DbError> {
    load_or_create_secret(data_dir, PASSWORD_FILE)
}

/// Read the persisted superuser password without creating one — for connecting to
/// a server that must already be running (and so already provisioned). A missing
/// file is a real error here, not a first-boot signal.
fn load_password(data_dir: &Path) -> Result<String, DbError> {
    let path = secret_parent(data_dir)?.join(PASSWORD_FILE);
    std::fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .map_err(|e| DbError::EmbeddedPostgres(format!("read password file: {e}")))
}

fn owner_password_file(schema: &str) -> String {
    format!("{OWNER_PASSWORD_FILE_PREFIX}{schema}")
}

pub fn load_or_create_owner_password(data_dir: &Path, schema: &str) -> Result<String, DbError> {
    load_or_create_secret(data_dir, &owner_password_file(schema))
}

pub fn load_owner_password(data_dir: &Path, schema: &str) -> Result<String, DbError> {
    let parent = secret_parent(data_dir)?;
    let path = parent.join(owner_password_file(schema));
    std::fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .map_err(|e| DbError::EmbeddedPostgres(format!("read owner password file: {e}")))
}

fn secret_parent(data_dir: &Path) -> Result<&Path, DbError> {
    data_dir
        .parent()
        .ok_or_else(|| DbError::EmbeddedPostgres(format!("data_dir has no parent: {data_dir:?}")))
}

fn load_or_create_secret(data_dir: &Path, file: &str) -> Result<String, DbError> {
    let parent = secret_parent(data_dir)?;
    let path = parent.join(file);
    if path.exists() {
        return std::fs::read_to_string(&path)
            .map(|s| s.trim().to_string())
            .map_err(|e| DbError::EmbeddedPostgres(format!("read password file: {e}")));
    }
    std::fs::create_dir_all(parent)
        .map_err(|e| DbError::EmbeddedPostgres(format!("create password dir: {e}")))?;
    let password = td_common::random::password();
    std::fs::write(&path, &password)
        .map_err(|e| DbError::EmbeddedPostgres(format!("write password file: {e}")))?;
    Ok(password)
}
