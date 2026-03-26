//
// Copyright 2026 Tabs Data Inc.
//

use crate::error::DbError;
use crate::pool::TdPool;
use crate::{DatabaseConfig, db_with_schema};
use url::Url;

/// Creates a connection pool for the `tabsdata` database.
pub async fn db() -> Result<TdPool, DbError> {
    let db = db_with_schema(&test_config(), td_schema::schema()).await?;
    db.upgrade().await?;
    db.check().await?;
    Ok(db)
}

pub fn test_config() -> DatabaseConfig {
    let db_file = td_common::tdtestdir!().join(format!("{}.db", td_common::id::id()));
    let db_url = Url::from_file_path(db_file).unwrap();
    DatabaseConfig::new(db_url)
}
