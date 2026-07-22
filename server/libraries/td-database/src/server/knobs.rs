//
// Copyright 2026 Tabsdata Inc.
//

//! [`BootConfig`] — the managed server's boot knobs — and the env-resolved
//! [`database_dir`] it stores its data in.

use super::entry::bootable_server;
use crate::config::{ComponentConfig, EMBEDDED_CREDENTIAL};
use crate::error::DbError;
use postgresql_embedded::Settings;
use std::path::PathBuf;
use td_common::env::{DATABASE_DIR_ENV, REPOSITORY_URI_ENV};

/// Boot knobs for the managed server. Not config surface: deployments derive
/// these via [`boot_config`]; the shared test server builds them directly. No
/// superuser name knob — `postgresql_embedded` always runs `initdb` with its
/// fixed default (`postgres`).
#[derive(Debug, Clone, Default)]
pub struct BootConfig {
    /// `None` lets `postgresql_embedded` pick a temp directory.
    pub data_dir: Option<PathBuf>,
    /// `None` requests a random ephemeral port (test use only).
    pub port: Option<u16>,
    /// When `false`, the data directory is wiped on drop (tests).
    pub persistent: bool,
    /// `None` falls back to the password file alongside `data_dir`.
    pub password: Option<String>,
}

impl BootConfig {
    pub(crate) fn data_dir_or_default(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| Settings::default().data_dir)
    }
}

/// The managed server's data directory: `TD_DATABASE_DIR`, else
/// `<TD_URI_REPOSITORY>/database`. The password files live next to it.
pub fn database_dir() -> Result<PathBuf, DbError> {
    if let Ok(dir) = std::env::var(DATABASE_DIR_ENV) {
        return Ok(PathBuf::from(dir));
    }
    if let Ok(repository) = std::env::var(REPOSITORY_URI_ENV) {
        return Ok(PathBuf::from(repository).join("database"));
    }
    Err(DbError::MissingDatabaseDir)
}

/// Boot knobs from the ([`bootable_server`]-gated) `database_server:` entry:
/// its URL's port (default 5432) on the env-resolved [`database_dir`];
/// `${embedded}` (and absence) map to the first-boot generated password.
pub fn boot_config(server: &ComponentConfig) -> Result<BootConfig, DbError> {
    let server = bootable_server(Some(server))?;
    let password = server
        .password
        .as_deref()
        .filter(|value| *value != EMBEDDED_CREDENTIAL)
        .map(str::to_owned);
    Ok(BootConfig {
        data_dir: Some(database_dir()?),
        port: Some(server.conn.url().port().unwrap_or(5432)),
        persistent: true,
        password,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // These tests mutate process-global environment variables, so they run
    // serially to avoid racing each other under a shared test process.

    #[cfg(postgres)]
    #[test]
    #[serial]
    fn boot_config_from_server_entry() {
        unsafe { std::env::set_var(DATABASE_DIR_ENV, "/tmp/td-boot-config-test") };
        let server: ComponentConfig = serde_yaml::from_str(
            "
url: postgres://localhost:7432/postgres
username: postgres
password: secret
",
        )
        .unwrap();
        let boot = boot_config(&server).unwrap();
        assert_eq!(boot.port, Some(7432));
        assert!(boot.persistent);
        assert_eq!(boot.password.as_deref(), Some("secret"));

        let server: ComponentConfig = serde_yaml::from_str(
            "
url: postgres://localhost:7432/postgres
username: admin
password: secret
",
        )
        .unwrap();
        assert!(matches!(
            boot_config(&server),
            Err(DbError::ServerUsernameNotBootable(name)) if name == "admin"
        ));

        let server: ComponentConfig = serde_yaml::from_str(
            "
url: postgres://localhost/postgres
username: ${embedded}
password: ${embedded}
",
        )
        .unwrap();
        let boot = boot_config(&server).unwrap();
        assert_eq!(boot.port, Some(5432));
        assert!(boot.password.is_none());

        let server: ComponentConfig =
            serde_yaml::from_str("url: postgres://db.example.com/postgres").unwrap();
        assert!(matches!(
            boot_config(&server),
            Err(DbError::NonLocalServerUrl(host)) if host == "db.example.com"
        ));
        unsafe { std::env::remove_var(DATABASE_DIR_ENV) };
    }

    #[test]
    #[serial]
    fn database_dir_env_precedence() {
        unsafe {
            std::env::remove_var(DATABASE_DIR_ENV);
            std::env::remove_var(REPOSITORY_URI_ENV);
        }
        assert!(matches!(database_dir(), Err(DbError::MissingDatabaseDir)));

        unsafe { std::env::set_var(REPOSITORY_URI_ENV, "/tmp/td-repo") };
        assert_eq!(
            database_dir().unwrap(),
            PathBuf::from("/tmp/td-repo/database")
        );

        unsafe { std::env::set_var(DATABASE_DIR_ENV, "/tmp/td-data") };
        assert_eq!(database_dir().unwrap(), PathBuf::from("/tmp/td-data"));

        unsafe {
            std::env::remove_var(DATABASE_DIR_ENV);
            std::env::remove_var(REPOSITORY_URI_ENV);
        }
    }
}
