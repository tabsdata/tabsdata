//
// Copyright 2026 Tabs Data Inc.
//

//! SQLite test pool factory. Builds a fresh per-test database file under
//! `tdtestdir!()` with relaxed pool settings.

use crate::config::DatabaseConfig;
use crate::pool::TdPool;
use url::Url;

/// Fresh sqlite pool backed by a per-test temp file. Caller is responsible
/// for `.upgrade()` if they need the schema applied.
pub async fn pool() -> TdPool {
    let db_file = td_common::tdtestdir!().join(format!("{}.db", td_common::id::id()));
    let db_url = Url::from_file_path(db_file).unwrap();
    let mut config = DatabaseConfig::new(db_url).unwrap();
    config.pool.min_connections = 0;
    config.pool.idle_timeout = std::time::Duration::from_millis(500);
    TdPool::connect(&config).await.expect("sqlite test pool")
}
