//
// Copyright 2026 Tabs Data Inc.
//

//! Detached lifecycle of the managed server. `pg_ctl start` daemonizes the
//! postmaster, so the running server's only owner is the on-disk state
//! ([`read_postmaster`]) — never a `PostgreSQL` handle.

use super::knobs::BootConfig;
use super::secrets::load_or_create_password;
use crate::error::DbError;
use crate::pool::connect::redacted_url;
use postgresql_embedded::{PostgreSQL, Settings, VersionReq};
use std::path::Path;
use tracing::{info, warn};
use url::Url;

/// Read at compile time from the same `POSTGRESQL_VERSION` that
/// `postgresql_embedded` uses to select the bundled archive, so the runtime
/// requirement cannot drift from what was bundled.
const EMBEDDED_POSTGRES_VERSION: &str = env!("POSTGRESQL_VERSION");

/// PID file written by postgres on start and removed on clean shutdown.
const POSTMASTER_PID_FILE: &str = "postmaster.pid";

/// Status line (8th line of `postmaster.pid`) written only once the postmaster
/// accepts connections; `initdb`'s bootstrap backend writes none at all.
const POSTMASTER_STATUS_READY: &str = "ready";

/// Settings locating the server on `cfg`'s `data_dir`/`port` — enough to stop
/// it, which needs no credentials. [`boot_settings`] layers the superuser
/// password on for the boot path.
fn base_settings(cfg: &BootConfig) -> Settings {
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
    // One server backs the whole test suite; a high-concurrency runner opens
    // many pools at once, so raise PostgreSQL's default (100) well above it.
    settings
        .configuration
        .insert("max_connections".to_string(), "500".to_string());
    settings
}

/// A persistent cluster must be reached with the credentials `initdb` baked
/// in, so without a configured password the one persisted on first boot is
/// loaded (created if absent).
fn boot_settings(cfg: &BootConfig) -> Result<Settings, DbError> {
    let mut settings = base_settings(cfg);
    if let Some(password) = cfg.password.clone() {
        settings.password = password;
    } else if cfg.persistent {
        settings.password = load_or_create_password(&settings.data_dir)?;
    }
    Ok(settings)
}

/// Install/initialize (idempotent) then start `server`, returning the URL it
/// listens on.
///
/// **Always forgets the handle**: its `Drop` runs `pg_ctl stop` whenever
/// `postmaster.pid` exists. On success the daemon must outlive this process;
/// on failure (e.g. a lost data_dir race) a `Drop` would stop the *sibling*
/// that owns the data_dir.
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

/// Boot the server detached, unconditionally — no reuse check. The caller is
/// responsible for not booting over a server it wants to keep; a boot that
/// loses a race to a running postmaster fails cleanly (see [`boot`]).
pub(crate) async fn boot_detached(cfg: &BootConfig) -> Result<(), DbError> {
    let url = boot(PostgreSQL::new(boot_settings(cfg)?)).await?;
    info!(
        "embedded postgres started, listening at {}",
        redacted_url(&url)
    );
    Ok(())
}

/// Start the server detached and idempotent: reuse one already accepting
/// connections on the configured port; refuse one on a *different* port (the
/// config drifted — adopting it would leave every component dialing a dead
/// port); otherwise boot.
pub(super) async fn start_detached(cfg: &BootConfig) -> Result<(), DbError> {
    if let Some(pm) = read_postmaster(&cfg.data_dir_or_default())
        && pm.accepting()
    {
        if let Some(configured) = cfg.port
            && pm.port != configured
        {
            return Err(DbError::ServerPortMismatch {
                running: pm.port,
                configured,
            });
        }
        info!("embedded postgres already running; reusing it");
        return Ok(());
    }
    boot_detached(cfg).await
}

/// Stop the server via `pg_ctl stop --wait`, whichever process started it.
/// Idempotent. A port drift only warns — stop must always be able to tear
/// down, or the drift [`start_detached`] refuses could never be recovered.
pub(super) async fn stop_detached(cfg: &BootConfig) -> Result<(), DbError> {
    let Some(pm) = read_postmaster(&cfg.data_dir_or_default()) else {
        info!("embedded postgres not running; nothing to stop");
        return Ok(());
    };
    if let Some(configured) = cfg.port
        && pm.accepting()
        && pm.port != configured
    {
        warn!(
            running = pm.port,
            configured, "running server's port differs from the configured one; stopping it anyway"
        );
    }
    let mut server = PostgreSQL::new(base_settings(cfg));
    // Resolve the installation so `pg_ctl` is locatable; does not touch the
    // running server.
    server
        .setup()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
    server
        .stop()
        .await
        .map_err(|e| DbError::EmbeddedPostgres(e.to_string()))?;
    info!("embedded postgres stopped");
    Ok(())
}

/// `postmaster.pid` fields: line 1 PID, line 4 port, line 8 status. `initdb`'s
/// bootstrap backend writes a transient file with a *negative* PID, a
/// placeholder port, and no status line — hence [`Self::accepting`].
struct Postmaster {
    pid: i64,
    port: u16,
    ready: bool,
}

impl Postmaster {
    fn accepting(&self) -> bool {
        self.pid > 0 && self.ready
    }
}

/// Parse `postmaster.pid` in `data_dir`; `None` when absent or too partial to
/// use. The single reader behind every liveness question.
fn read_postmaster(data_dir: &Path) -> Option<Postmaster> {
    let content = std::fs::read_to_string(data_dir.join(POSTMASTER_PID_FILE)).ok()?;
    let lines: Vec<&str> = content.lines().collect();
    let pid = lines.first()?.trim().parse::<i64>().ok()?;
    let port = lines.get(3)?.trim().parse::<u16>().ok()?;
    let ready = lines
        .get(7)
        .is_some_and(|s| s.trim() == POSTMASTER_STATUS_READY);
    Some(Postmaster { pid, port, ready })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A fresh data dir holding a `postmaster.pid` claiming `port`/`status`.
    fn fake_postmaster(port: u16, status: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("td-boot-test-{}", td_common::id::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(POSTMASTER_PID_FILE),
            format!("12345\n/data\n1700000000\n{port}\n/tmp\nlocalhost\n  1234 5678\n{status}\n"),
        )
        .unwrap();
        dir
    }

    #[test]
    fn read_postmaster_parses_pid_port_and_status() {
        let dir = fake_postmaster(7433, "ready");
        let pm = read_postmaster(&dir).unwrap();
        assert_eq!(pm.pid, 12345);
        assert_eq!(pm.port, 7433);
        assert!(pm.accepting());

        let dir = fake_postmaster(7433, "starting");
        assert!(!read_postmaster(&dir).unwrap().accepting());
        assert!(read_postmaster(&PathBuf::from("/nonexistent")).is_none());
    }

    /// The port gate runs before any `pg_ctl` is touched.
    #[tokio::test]
    async fn start_detached_checks_running_port() {
        let dir = fake_postmaster(7433, "ready");
        let cfg = BootConfig {
            data_dir: Some(dir),
            port: Some(7434),
            persistent: true,
            password: None,
        };
        assert!(matches!(
            start_detached(&cfg).await,
            Err(DbError::ServerPortMismatch {
                running: 7433,
                configured: 7434,
            })
        ));

        let matching = BootConfig {
            port: Some(7433),
            ..cfg
        };
        start_detached(&matching).await.unwrap();
    }
}
