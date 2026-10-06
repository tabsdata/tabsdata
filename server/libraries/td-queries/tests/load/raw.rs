//
// Copyright 2026 Tabsdata Inc.
//

//! Statements the typed surface deliberately doesn't offer. `delete` is
//! declared only by the DAOs a service actually deletes, and none of the
//! chain-dataset DAOs is one, so the delete cases issue the statement
//! themselves.

use sea_query::{Expr, ExprTrait, Query};
use ta_objects::errors::SqlError;
use ta_objects::{PkDao, SeaValue};
use td_database::sea_query::builder::SeaQueryExt;
use td_queries::QueryExecutor;

pub async fn delete_by_pk<D, E>(executor: E, pk: &D::Pk) -> Result<u64, SqlError>
where
    D: PkDao,
    E: QueryExecutor,
{
    let result = Query::delete()
        .from_table(D::TABLE)
        .and_where(Expr::col(D::PK).eq(pk.as_sea_expr()))
        .build_query()
        .execute(executor)
        .await
        .map_err(SqlError::on_read::<D>)?;
    Ok(result.rows_affected())
}
