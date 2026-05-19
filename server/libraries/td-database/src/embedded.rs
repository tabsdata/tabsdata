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
use postgresql_embedded::{PostgreSQL, Settings};
use std::path::Path;
use url::Url;

/// Superuser password persisted alongside a persistent data directory.
/// `Settings::default()` regenerates a random password on every call, which
/// would not match what `initdb` baked into the existing catalog — so we
/// own the password lifecycle for the persistent case.
const PASSWORD_FILE: &str = ".td_embedded_password";

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
}

/// Boot a fresh embedded Postgres and return its [`EmbeddedPostgres`] handle.
pub async fn start(cfg: &EmbeddedConfig) -> Result<(EmbeddedPostgres, Url), DbError> {
    let mut settings = Settings {
        temporary: !cfg.persistent,
        ..Settings::default()
    };
    if let Some(dir) = cfg.data_dir.clone() {
        settings.data_dir = dir;
    }
    if let Some(port) = cfg.port {
        settings.port = port;
    }

    if cfg.persistent {
        settings.password = load_or_create_password(&settings.data_dir)?;
    }

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

/// Read the persisted superuser password from `<data_dir>/../PASSWORD_FILE`,
/// or generate a fresh one and write it on first boot. Lives outside the
/// data_dir itself because `initdb` refuses a non-empty data_dir.
fn load_or_create_password(data_dir: &Path) -> Result<String, DbError> {
    let parent = data_dir.parent().ok_or_else(|| {
        DbError::EmbeddedPostgres(format!("data_dir has no parent: {data_dir:?}"))
    })?;
    let path = parent.join(PASSWORD_FILE);
    if path.exists() {
        return std::fs::read_to_string(&path)
            .map(|s| s.trim().to_string())
            .map_err(|e| DbError::EmbeddedPostgres(format!("read password file: {e}")));
    }
    std::fs::create_dir_all(parent)
        .map_err(|e| DbError::EmbeddedPostgres(format!("create password dir: {e}")))?;
    use rand::RngExt;
    let password: String = (&mut rand::rng())
        .sample_iter(rand::distr::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    std::fs::write(&path, &password)
        .map_err(|e| DbError::EmbeddedPostgres(format!("write password file: {e}")))?;
    Ok(password)
}
