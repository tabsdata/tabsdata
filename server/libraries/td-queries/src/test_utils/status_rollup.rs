//
// Copyright 2026 Tabsdata Inc.
//

//! The v10 rung's own roll-up of each execution and transaction, the oracle
//! the live door must match.

use ta_objects::errors::SqlError;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_schema::schema::deltas::status_rollup_backfill;

pub async fn derive_status_rollup(db: &TdPool) -> Result<(), SqlError> {
    let failed = |e| SqlError::AnySqlxError("status_rollup".to_string(), e);
    let mut trx = db.begin().await.map_err(failed)?;
    let conn = &mut *trx;
    conn.scope_across_projects().await.map_err(failed)?;
    conn.execute_batch(&status_rollup_backfill())
        .await
        .map_err(failed)?;
    trx.commit().await.map_err(failed)?;
    Ok(())
}
