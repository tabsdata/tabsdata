//
// Copyright 2026 Tabsdata Inc.
//

//! The same operations over a slice of conditions that may exceed the
//! bind-parameter limit: each chunk is one statement, the slice's conditions
//! OR-joined and `extra` AND-ed onto every chunk. Reads keep the grain of the
//! handle they came from; writes exist on the grains that have them.

use crate::Serves;
use crate::WritesTo;
use crate::generic::grain::{Current, Grain, Unversioned, select_chunk_in};
use crate::primitives::chunked::{
    MAX_INSERT_ROWS, MAX_QUERY_CONDITIONS, close_chunk_query, fetch_chunked, insert_vec_query,
    open_vec_query, trace_if_chunked, update_chunk_query, update_open_chunk_query,
};
use sqlx::postgres::PgQueryResult;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    InsertDao, InsertableFrom, Scd2CloseDao, Scd2Dao, Scd2OpenDao, SelectDao, UnversionedDao,
    UpdateDao,
};
use td_database::sea_query::builder::SeaQueryExt;

pub struct Chunked<E, G = Unversioned> {
    pub(crate) executor: E,
    pub(crate) grain: G,
}

impl<E, G> Chunked<E, G> {
    pub async fn fetch_all<D: SelectDao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        G: Grain<D>,
        E: Serves<D>,
    {
        let extra = extra.resolve();
        let grain = self.grain;
        fetch_chunked::<D, D, _, _>(self.executor, values, |chunk| {
            select_chunk_in::<D, G>(&grain, chunk, &extra)
        })
        .await
    }
}

impl<E> Chunked<E, Unversioned> {
    pub async fn insert<D: InsertDao + InsertableFrom<E::Scope>>(
        self,
        daos: &[D],
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + WritesTo<D>,
    {
        let mut executor = self.executor;
        let mut result = PgQueryResult::default();
        for chunk in daos.chunks(MAX_INSERT_ROWS) {
            let r = insert_vec_query::<D>(chunk)?
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| {
                    let values: Vec<_> = chunk
                        .iter()
                        .flat_map(|dao| dao.column_display_values())
                        .collect();
                    SqlError::on_write::<D>(e, &values)
                })?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    pub async fn update<D: UpdateDao>(
        self,
        dao: &D,
        values: &[impl SqlCondition<D::FilterBy>],
    ) -> Result<PgQueryResult, SqlError>
    where
        D::FilterBy: UnversionedDao,
        E: Serves<D> + WritesTo<D>,
    {
        let mut executor = self.executor;
        trace_if_chunked::<D>(values.len());
        let mut result = PgQueryResult::default();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r = update_chunk_query::<D>(dao, chunk)
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }
}

impl<E> Chunked<E, Current> {
    /// Ends the matching open intervals at `at`, chunk by chunk.
    pub async fn close<D: Scd2CloseDao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
        at: D::At,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + WritesTo<D>,
    {
        let mut executor = self.executor;
        trace_if_chunked::<D>(values.len());
        let extra = extra.resolve();
        let at = at.into();
        let mut result = PgQueryResult::default();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r = close_chunk_query::<D>(chunk, &extra, at)?
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &[]))?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    /// Rewrites the payload of the matching open rows, chunk by chunk.
    pub async fn update<D: UpdateDao>(
        self,
        dao: &D,
        values: &[impl SqlCondition<D::FilterBy>],
    ) -> Result<PgQueryResult, SqlError>
    where
        D::FilterBy: Scd2Dao,
        E: Serves<D> + WritesTo<D>,
    {
        let mut executor = self.executor;
        trace_if_chunked::<D>(values.len());
        let mut result = PgQueryResult::default();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r = update_open_chunk_query::<D>(dao, chunk)
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    /// New versions open from `at`, chunk by chunk.
    pub async fn open<D: Scd2OpenDao + InsertableFrom<E::Scope>>(
        self,
        daos: &[D],
        at: D::At,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + WritesTo<D>,
    {
        let mut executor = self.executor;
        let at = at.into();
        let mut result = PgQueryResult::default();
        for chunk in daos.chunks(MAX_INSERT_ROWS) {
            let r = open_vec_query::<D>(chunk, at)?
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| {
                    let values: Vec<_> = chunk
                        .iter()
                        .flat_map(|dao| dao.column_display_values())
                        .collect();
                    SqlError::on_write::<D>(e, &values)
                })?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }
}
