//
// Copyright 2026 Tabs Data Inc.
//

//! Chunked query execution for slices that may exceed database
//! bind-parameter limits.
//!
//! Each element in the slice is resolved via [`SqlCondition`] and combined
//! with OR. The slice is split into chunks of at most `MAX_QUERY_CONDITIONS`
//! elements, and one query is executed per chunk.
//!
//! These are the bare-table ops. For the chunked equivalents on other
//! read modes, reach for the corresponding wrapper:
//!
//! - `Current::fetch_all_chunked` — read from the `_current` view.
//! - `Versioned::fetch_all_chunked` — partition-scoped point-in-time.
//! - `Historic::fetch_all_chunked` — cross-partition slow scan.
//!
//! The per-chunk SQL builders and the chunk-size constants live in
//! [`crate::primitives::chunked`].

use crate::QueryExecutor;
use crate::primitives::chunked::{
    MAX_INSERT_ROWS, MAX_QUERY_CONDITIONS, delete_chunk_query, fetch_all_chunk_query,
    insert_vec_query, update_chunk_query, warn_if_chunked,
};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Dao, DaoCore, DaoUpdate};
use td_database::domain::Domained;
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;

pub trait ChunkedQueries {
    /// Batch-insert multiple DAOs using multi-row INSERT statements.
    ///
    /// Rows are chunked to stay within database bind-parameter limits.
    /// Each chunk produces a single `INSERT INTO ... VALUES (...), (...), ...`.
    fn insert_vec<D: Dao>(
        self,
        daos: &[D],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        Self: Domained<Domain = D::Db>;

    /// Fetch every row matching any of `values`, OR'd together. Splits
    /// `values` into chunks to stay within bind-parameter limits and
    /// issues one query per chunk.
    fn fetch_all_chunked<D: Dao, V: SqlCondition>(
        self,
        values: &[V],
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        Self: Domained<Domain = D::Db>;

    /// Apply `dao` as the update to every row matching any of `values`.
    /// Chunked like [`fetch_all_chunked`].
    fn update_chunked<U: DaoUpdate, D: DaoCore, V: SqlCondition>(
        self,
        dao: &U,
        values: &[V],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        Self: Domained<Domain = U::Db>;

    /// Delete every row matching any of `values`. Chunked like
    /// [`fetch_all_chunked`].
    fn delete_chunked<D: DaoCore, V: SqlCondition>(
        self,
        values: &[V],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>
    where
        Self: Domained<Domain = D::Db>;
}

impl<E> ChunkedQueries for E
where
    E: QueryExecutor,
{
    async fn insert_vec<D: Dao>(mut self, daos: &[D]) -> Result<DynamicQueryResult, SqlError>
    where
        Self: Domained<Domain = D::Db>,
    {
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in daos.chunks(MAX_INSERT_ROWS) {
            let r = insert_vec_query::<D>(chunk)?
                .build_query()
                .execute(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    async fn fetch_all_chunked<D: Dao, V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError>
    where
        Self: Domained<Domain = D::Db>,
    {
        warn_if_chunked::<D>(values.len());
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = fetch_all_chunk_query::<D, V>(chunk)?
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
    ) -> Result<DynamicQueryResult, SqlError>
    where
        Self: Domained<Domain = U::Db>,
    {
        warn_if_chunked::<D>(values.len());
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r = update_chunk_query::<U, D, V>(dao, chunk)?
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
    ) -> Result<DynamicQueryResult, SqlError>
    where
        Self: Domained<Domain = D::Db>,
    {
        warn_if_chunked::<D>(values.len());
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r = delete_chunk_query::<D, V>(chunk)?
                .build_query()
                .execute(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }
}
