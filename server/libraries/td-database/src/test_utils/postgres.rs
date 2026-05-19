//
// Copyright 2026 Tabs Data Inc.
//

//! Postgres test pool factory. Hands back a `TdPool` that owns its own
//! in-process embedded postgres via [`BackendConfig::EmbeddedPostgres`].

use crate::config::{BackendConfig, DatabaseConfig, EmbeddedConfig};
use crate::pool::TdPool;

/// Boot a fresh embedded postgres and connect a `TdPool` to it. The server
/// is wiped and stopped automatically when the returned `TdPool` (and any
/// of its clones) drops — `EmbeddedConfig::persistent = false` makes the
/// underlying `postgresql_embedded` handle treat the data dir as temporary,
/// so `pg_ctl stop` runs and the data dir is removed on `Drop`.
pub async fn pool() -> TdPool {
    let config = DatabaseConfig {
        conn: Some(BackendConfig::embedded_postgres(EmbeddedConfig {
            data_dir: None,
            port: None,
            persistent: false,
        })),
        pool: Default::default(),
    };
    TdPool::connect(&config).await.expect("postgres test pool")
}
