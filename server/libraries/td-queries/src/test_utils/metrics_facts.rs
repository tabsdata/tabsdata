//
// Copyright 2026 Tabsdata Inc.
//

//! The v10 rung's own derivation of `metrics_facts`, the oracle the live
//! writers must match.

use ta_objects::errors::SqlError;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_schema::schema::deltas::metrics_facts_backfill;

pub async fn derive_metrics_facts(db: &TdPool) -> Result<(), SqlError> {
    let failed = |e| SqlError::AnySqlxError("metrics_facts".to_string(), e);
    let mut trx = db.begin().await.map_err(failed)?;
    let conn = &mut *trx;
    conn.scope_across_projects().await.map_err(failed)?;
    conn.execute_batch(&[
        "DELETE FROM metrics_facts".to_string(),
        metrics_facts_backfill(),
    ])
    .await
    .map_err(failed)?;
    trx.commit().await.map_err(failed)?;
    Ok(())
}
