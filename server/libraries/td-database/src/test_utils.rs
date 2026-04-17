//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::pool::TdPool;
use url::Url;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db() -> Result<TdPool, DbError> {
    let db = TdPool::connect(&test_config()).await?;
    db.upgrade().await?;
    db.check().await?;
    Ok(db)
}

pub fn test_config() -> DatabaseConfig {
    let db_file = td_common::tdtestdir!().join(format!("{}.db", td_common::id::id()));
    let db_url = Url::from_file_path(db_file).unwrap();
    let mut config = DatabaseConfig::new(db_url).unwrap();
    config.pool.min_connections = 0;
    config.pool.idle_timeout = std::time::Duration::from_millis(500);
    config
}
