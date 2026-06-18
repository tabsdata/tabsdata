//
// Copyright 2026 Tabs Data Inc.
//

//! Embedded Postgres for in-process database hosting. Used by the
//! apiserver's no-URL fallback and by the per-test postgres factory.
//!
//! Drop on [`EmbeddedPostgres`] stops the server cleanly and releases its
//! shared-memory segment, so callers must keep the handle alive for as
//! long as they need the server up.

use crate::config::EmbeddedConfig;
use crate::error::DbError;
use postgresql_embedded::{PostgreSQL, Settings, VersionReq};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{info, warn};
use url::Url;

/// Superuser password persisted alongside a persistent data directory.
/// `Settings::default()` regenerates a random password on every call, which
/// would not match what `initdb` baked into the existing catalog — so we
/// own the password lifecycle for the persistent case.
/// Exact embedded PostgreSQL version. Must match the bundled archive selected
/// at build time via `POSTGRESQL_VERSION` in `.cargo/config.toml`.
const EMBEDDED_POSTGRES_VERSION: &str = "=18.4.0";

const PASSWORD_FILE: &str = ".td_embedded_password";

const OWNER_PASSWORD_FILE_PREFIX: &str = ".td_embedded_owner_password.";

/// PID file written by postgres on start and removed on clean shutdown.
const POSTMASTER_PID_FILE: &str = "postmaster.pid";

/// How long to wait for a prior postmaster (started by an earlier td-apiserver
/// process on the same data_dir) to fully release `postmaster.pid` before we
/// boot our own. Covers the ~100 ms shutdown overlap window between a stopped
/// instance and an immediate restart.
const STALE_POSTMASTER_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait, on shutdown, for `postmaster.pid` to disappear after we
/// ask postgres to stop. `pg_ctl stop --wait` should already block until the
/// postmaster exits, but the .pid file removal can lag a bit; this is the
/// belt-and-braces tail wait.
const SHUTDOWN_PID_FILE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

const POSTMASTER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Drop guard for a live embedded Postgres server. Drop stops the server
/// cleanly; that's the only reason this wrapper exists. The connection URL
/// is returned separately by [`start`] and tracked by the pool, not here.
#[derive(Debug)]
pub struct EmbeddedPostgres {
    server: PostgreSQL,
}

impl EmbeddedPostgres {
    /// Underlying `postgresql_embedded` handle, in case callers need access
    /// to settings (port, data dir) or the raw `PostgreSQL` API.
    pub fn server(&self) -> &PostgreSQL {
        &self.server
    }

    /// Asynchronously shut down the embedded server. Calls `pg_ctl stop --wait`
    /// and polls for `postmaster.pid` removal. Use this from an explicit
    /// shutdown path so the postgres lifecycle is not at the mercy of Drop
    /// ordering after the tokio runtime has been torn down.
    pub async fn stop(&self) -> Result<(), DbError> {
        self.server
            .stop()
            .await
            .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
        let pid_file = self.server.settings().data_dir.join(POSTMASTER_PID_FILE);
        let deadline = Instant::now() + SHUTDOWN_PID_FILE_WAIT_TIMEOUT;
        while pid_file.exists() && Instant::now() < deadline {
            tokio::time::sleep(POSTMASTER_POLL_INTERVAL).await;
        }
        if pid_file.exists() {
            warn!(
                pid_file = %pid_file.display(),
                "postmaster.pid still present after async shutdown wait; an immediate restart on this data_dir may race",
            );
        }
        Ok(())
    }
}

impl Drop for EmbeddedPostgres {
    /// Wait for `postmaster.pid` to disappear after the upstream
    /// `PostgreSQL` drop runs `pg_ctl stop --wait`. The upstream Drop
    /// returns once pg_ctl reports the postmaster gone, but the .pid file
    /// removal can lag by tens of milliseconds — long enough for an
    /// immediate restart in the same data_dir to race a stale lock file.
    /// Polling here closes that window without bringing the test driver
    /// into the picture.
    fn drop(&mut self) {
        let pid_file = self.server.settings().data_dir.join(POSTMASTER_PID_FILE);
        let deadline = Instant::now() + SHUTDOWN_PID_FILE_WAIT_TIMEOUT;
        while pid_file.exists() && Instant::now() < deadline {
            std::thread::sleep(POSTMASTER_POLL_INTERVAL);
        }
        if pid_file.exists() {
            warn!(
                pid_file = %pid_file.display(),
                "postmaster.pid still present after shutdown wait; an immediate restart on this data_dir may race",
            );
        }
    }
}

/// Boot a fresh embedded Postgres and return its [`EmbeddedPostgres`] handle.
pub async fn start(cfg: &EmbeddedConfig) -> Result<(EmbeddedPostgres, Url), DbError> {
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
    if let Some(password) = cfg.password.clone() {
        settings.password = password;
    } else if cfg.persistent {
        settings.password = load_or_create_password(&settings.data_dir)?;
    }

    wait_for_postmaster_release(&settings.data_dir).await?;

    let mut server = PostgreSQL::new(settings);
    server
        .setup()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
    server
        .start()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;

    let url = server.settings().url("postgres");
    let url = Url::parse(&url).map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;

    Ok((EmbeddedPostgres { server }, url))
}

/// Wait for `postmaster.pid` in `data_dir` to disappear before we attempt to
/// boot a fresh postgres on top of it. Closes the restart race where an
/// outgoing instance's postmaster has been asked to stop but has not yet
/// finished tearing down, leaving its lock file in place.
///
/// Postgres removes `postmaster.pid` cleanly on shutdown, so the file's
/// presence is a sufficient "another server might still be running" signal.
/// We do NOT remove a stale file ourselves — if the prior postmaster crashed
/// hard and left the file behind, the user should see the surfaced error and
/// recover manually, not have the lock silently bypassed by us.
async fn wait_for_postmaster_release(data_dir: &Path) -> Result<(), DbError> {
    let pid_file: PathBuf = data_dir.join(POSTMASTER_PID_FILE);
    if !pid_file.exists() {
        return Ok(());
    }
    info!(
        pid_file = %pid_file.display(),
        "postmaster.pid present at start; waiting for the prior server to release it",
    );
    let deadline = Instant::now() + STALE_POSTMASTER_WAIT_TIMEOUT;
    while pid_file.exists() {
        if Instant::now() >= deadline {
            return Err(DbError::EmbeddedPostgres(format!(
                "postmaster.pid at {} did not disappear within {}s; \
                 either the previous postmaster is still running or it crashed \
                 and left a stale lock — inspect and remove manually if needed",
                pid_file.display(),
                STALE_POSTMASTER_WAIT_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(POSTMASTER_POLL_INTERVAL).await;
    }
    Ok(())
}

/// Read the persisted superuser password from `<data_dir>/../PASSWORD_FILE`,
/// or generate a fresh one and write it on first boot. Lives outside the
/// data_dir itself because `initdb` refuses a non-empty data_dir.
fn load_or_create_password(data_dir: &Path) -> Result<String, DbError> {
    load_or_create_secret(data_dir, PASSWORD_FILE)
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
