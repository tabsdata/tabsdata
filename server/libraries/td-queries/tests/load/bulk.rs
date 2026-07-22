//
// Copyright 2026 Tabsdata Inc.
//

//! Chunked multi-row `INSERT` for the seed datasets. Unlike `COPY`, it still
//! fires table triggers, so `_current` and sticky-status maintenance stay
//! correct.

use sea_query::Query;
use ta_objects::Dao;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_queries::QueryExecutor;

/// Under sqlite's ~999 bind-parameter limit, the tightest backend.
const MAX_BIND_PARAMS: usize = 800;

pub async fn bulk_insert<D, E>(executor: E, rows: &[D]) -> Result<u64, SqlError>
where
    D: Dao,
    E: QueryExecutor + Copy,
{
    if rows.is_empty() {
        return Ok(0);
    }
    let ncols = D::columns().into_iter().count().max(1);
    let chunk = (MAX_BIND_PARAMS / ncols).max(1);

    let mut total = 0u64;
    for batch in rows.chunks(chunk) {
        let mut query = Query::insert();
        query.into_table(D::table()).columns(D::columns());
        for dao in batch {
            query.values(dao.insert_values())?;
        }
        let result = query
            .build_query()
            .execute(executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &batch[0].column_display_values()))?;
        total += result.rows_affected();
    }
    Ok(total)
}
