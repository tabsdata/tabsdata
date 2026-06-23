//
// Copyright 2026 Tabs Data Inc.
//

//! [`SystemConfig`] — the privileged "system" connection of a **managed** database.

use super::backend::BackendConfig;
use super::connection::DatabaseConfig;
use super::pool::PoolConfig;
use crate::error::DbError;
use serde::{Deserialize, Serialize};

/// The privileged system connection, present only for a **managed** database
/// (`embedded`/`sidecar`): the connection target plus admin/superuser credentials.
/// For `embedded` it is the in-process server's spawn knobs (boot credentials are
/// generated); for `sidecar` it is the alongside server's admin URL and login. It
/// has no home schema — it provisions across schemas with explicit DDL. A `remote`
/// database has none (we never manage it).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    /// Connection target — embedded spawn knobs or an external admin URL.
    #[serde(flatten)]
    pub conn: BackendConfig,
    /// Admin/superuser login. Generated for `embedded`; required for `sidecar`.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// This connection's own pool, independent of every component's serving pool
    /// (no inter-pool inheritance). The system pool only provisions and boots, so
    /// it can run lean — e.g. a single connection — regardless of how large the
    /// component pools are. Unset fields fall back to the per-field defaults.
    #[serde(default, flatten)]
    pub pool: PoolConfig,
}

impl SystemConfig {
    /// The privileged connectable: this connection (its backend lowered to a
    /// [`ConnectableBackend`](super::ConnectableBackend)) with no home schema and its
    /// own pool settings, ready for [`crate::system::SystemPool`] to open. Url-based
    /// system backends only; an embedded system entry is resolved by
    /// [`DatabaseResolver::system_connectable_running`](super::DatabaseResolver::system_connectable_running).
    pub fn connectable(&self) -> Result<DatabaseConfig, DbError> {
        Ok(DatabaseConfig {
            conn: self.conn.clone().into_connectable()?,
            schema: None,
            username: self.username.clone(),
            password: self.password.clone(),
            app_role: None,
            pool: self.pool.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The system pool is configured independently of any component pool, and an
    /// unset field falls back to the per-field Rust default — never to another
    /// pool's value.
    #[test]
    fn system_pool_is_independent_with_field_defaults() {
        let yaml = "\
url: postgres://localhost/tabsdata
username: admin
password: secret
max_connections: 1
";
        let sys: SystemConfig = serde_yaml::from_str(yaml).unwrap();
        let connectable = sys.connectable().unwrap();
        assert_eq!(
            connectable.pool.max_connections, 1,
            "system pool reads its own value"
        );
        assert_eq!(
            connectable.pool.min_connections,
            PoolConfig::default().min_connections,
            "an unset field uses the Rust default, not another pool config"
        );
    }
}
