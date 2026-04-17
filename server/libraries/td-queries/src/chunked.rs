//
// Copyright 2026 Tabs Data Inc.
//

//! Chunked query execution for slices that may exceed database bind-parameter
//! limits.
//!
//! Each element in the slice is resolved via [`SqlCondition`] and combined
//! with OR. The slice is split into chunks of at most [`MAX_QUERY_PARAMS`]
//! elements, and one query is executed per chunk.

use crate::versioned::{Cte, latest_ranked_cte};
use sea_query::{Alias, Expr, ExprTrait, Query};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{CurrentDao, Dao, DaoCore, DaoUpdate, States, VersionedDao};
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;

/// Maximum bind parameters per query.
///
/// Conservatively set well below database limits (SQLite: 32,766;
/// PostgreSQL: 65,535) to leave room for other parameters in the query
/// (pagination, ordering, additional WHERE clauses).
/// Also, useful to avoid stack overflow errors on deeply nested sea_query::Conditions.
pub const MAX_QUERY_PARAMS: usize = 1_000;

fn resolve_or<D: DaoCore>(items: &[impl SqlCondition]) -> Result<sea_query::Condition, SqlError> {
    let mut cond = sea_query::Condition::any();
    for item in items {
        cond = cond.add(item.resolve::<D>()?);
    }
    Ok(cond)
}

pub trait ChunkedQueries {
    fn fetch_all_chunked<D: Dao, V: SqlCondition>(
        self,
        values: &[V],
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>;

    fn update_chunked<U: DaoUpdate, D: DaoCore, V: SqlCondition>(
        self,
        dao: &U,
        values: &[V],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>;

    fn delete_chunked<D: DaoCore, V: SqlCondition>(
        self,
        values: &[V],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>;

    fn fetch_all_at_chunked<const S: u8, D: VersionedDao + States<S>, V: SqlCondition>(
        self,
        values: &[V],
        on: &D::VersionOrder,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>;

    fn fetch_all_current_chunked<const S: u8, D: CurrentDao + States<S>, V: SqlCondition>(
        self,
        values: &[V],
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>;
}

impl<E> ChunkedQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_all_chunked<D: Dao, V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError> {
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_PARAMS) {
            let r: Vec<D> = Query::select()
                .from(D::table())
                .columns(D::columns())
                .cond_where(resolve_or::<D>(chunk)?)
                .order_by_columns(D::order_by())
                .build_query_as()
                .fetch_all(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }

    async fn update_chunked<U: DaoUpdate, D: DaoCore, V: SqlCondition>(
        mut self,
        dao: &U,
        values: &[V],
    ) -> Result<DynamicQueryResult, SqlError> {
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in values.chunks(MAX_QUERY_PARAMS) {
            let r = Query::update()
                .table(U::table())
                .values(dao.update_values())
                .cond_where(resolve_or::<D>(chunk)?)
                .build_query()
                .execute(self.reuse())
                .await
                .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    async fn delete_chunked<D: DaoCore, V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<DynamicQueryResult, SqlError> {
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in values.chunks(MAX_QUERY_PARAMS) {
            let r = Query::delete()
                .from_table(D::table())
                .cond_where(resolve_or::<D>(chunk)?)
                .build_query()
                .execute(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    async fn fetch_all_at_chunked<const S: u8, D: VersionedDao + States<S>, V: SqlCondition>(
        mut self,
        values: &[V],
        on: &D::VersionOrder,
    ) -> Result<Vec<D>, SqlError> {
        let state_cond = resolve_condition::<D>(D::state())?;
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_PARAMS) {
            let r: Vec<D> = Query::select()
                .columns(D::columns())
                .from(Cte::Ranked)
                .and_where(Expr::col(Cte::Rn).eq(1))
                .cond_where(resolve_or::<D>(chunk)?)
                .cond_where(state_cond.clone())
                .order_by_columns(D::order_by())
                .take()
                .with(latest_ranked_cte::<D>(on)?)
                .build_query_as()
                .fetch_all(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }

    async fn fetch_all_current_chunked<const S: u8, D: CurrentDao + States<S>, V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError> {
        let state_cond = resolve_condition::<D>(D::state())?;
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_PARAMS) {
            let r: Vec<D> = Query::select()
                .columns(D::columns())
                .from(Alias::new(D::current_table_name()))
                .cond_where(resolve_or::<D>(chunk)?)
                .cond_where(state_cond.clone())
                .build_query_as()
                .fetch_all(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }
}
