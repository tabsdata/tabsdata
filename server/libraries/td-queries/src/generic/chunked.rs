//
// Copyright 2026 Tabsdata Inc.
//

//! The same operations over a slice of conditions that may exceed the
//! bind-parameter limit: each chunk is one statement, the slice's conditions
//! OR-joined and `extra` AND-ed onto every chunk.

use crate::Serves;
use crate::primitives::chunked::{
    MAX_INSERT_ROWS, MAX_QUERY_CONDITIONS, close_chunk_query, fetch_all_at_chunk_query,
    fetch_all_chunk_query, fetch_all_current_chunk_query, fetch_chunked, insert_vec_query,
    open_vec_query, update_chunk_query, update_open_chunk_query, warn_if_chunked,
};
use sqlx::postgres::PgQueryResult;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    InsertDao, InsertableFrom, SameInner, Scd2Dao, Scd2OpenDao, SelectDao, TableDao, TimelineDao,
    UnversionedDao, UpdateDao,
};
use td_database::access::LockInstant;
use td_database::sea_query::builder::SeaQueryExt;

pub struct Chunked<E> {
    pub(crate) executor: E,
}

impl<E> Chunked<E> {
    pub async fn fetch_all<D: SelectDao + UnversionedDao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        let extra = extra.resolve();
        fetch_chunked::<D, D, _, _>(self.executor, values, |chunk| {
            fetch_all_chunk_query::<D>(chunk, &extra)
        })
        .await
    }

    pub async fn insert<D: InsertDao + InsertableFrom<E::Scope>>(
        self,
        daos: &[D],
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D>,
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
        E: Serves<D>,
    {
        let mut executor = self.executor;
        warn_if_chunked::<D>(values.len());
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

pub struct Scd2Chunked<E> {
    pub(crate) executor: E,
}

impl<E> Scd2Chunked<E> {
    /// Ends the matching open intervals at `at`, chunk by chunk.
    pub async fn close<D: TableDao + Scd2Dao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D>,
    {
        let mut executor = self.executor;
        warn_if_chunked::<D>(values.len());
        let extra = extra.resolve();
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
        E: Serves<D>,
    {
        let mut executor = self.executor;
        warn_if_chunked::<D>(values.len());
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
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D>,
    {
        let mut executor = self.executor;
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

    pub async fn fetch_all<D: SelectDao + Scd2Dao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        let extra = extra.resolve();
        fetch_chunked::<D, D, _, _>(self.executor, values, |chunk| {
            fetch_all_current_chunk_query::<D>(chunk, &extra)
        })
        .await
    }
}

pub struct AsOfChunked<'a, E, V> {
    pub(crate) on: &'a V,
    pub(crate) executor: E,
}

impl<E, V> AsOfChunked<'_, E, V> {
    pub async fn fetch_all<D: SelectDao + Scd2Dao + TimelineDao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        V: SameInner<D::VersionOrder>,
        E: Serves<D>,
    {
        let extra = extra.resolve();
        fetch_chunked::<D, D, _, _>(self.executor, values, |chunk| {
            fetch_all_at_chunk_query::<D, V>(chunk, self.on, &extra)
        })
        .await
    }
}

pub struct TimelineChunked<E> {
    pub(crate) executor: E,
}

impl<E> TimelineChunked<E> {
    pub async fn fetch_all<D: SelectDao + TimelineDao>(
        self,
        values: &[impl SqlCondition<D>],
        extra: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        let extra = extra.resolve();
        fetch_chunked::<D, D, _, _>(self.executor, values, |chunk| {
            fetch_all_chunk_query::<D>(chunk, &extra)
        })
        .await
    }
}
