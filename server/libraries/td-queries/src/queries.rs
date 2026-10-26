//
// Copyright 2026 Tabsdata Inc.
//

//! [`Queries`] — the whole query surface on every executor.
//!
//! The default grain carries no entry word. A word appears only where the
//! caller has something to choose: a point in time, row locks, batching, or a
//! traversal.
//!
//! A grain word names no DAO; the terminal does, as the wordless grain does:
//! `db.scd2().fetch_one::<D>(c)`, never `db.current::<D>()`.
//!
//! Every wordless read binds on [`UnversionedDao`]: on a versioned relation
//! the caller has a version to pick, so the read must say which — a grain
//! word for an instant, or `timeline()` for the version log, where a pinned
//! version id is a plain [`AddressesOneRow`] read.

use crate::QueryExecutor;
use crate::Serves;
use crate::Writes;
use crate::daos::dependency::Dependencies;
use crate::daos::execution::Executions;
use crate::daos::function_runs::{LogicalRuns, PhysicalRuns};
use crate::daos::metrics::Metrics;
use crate::daos::projects::Projects;
use crate::daos::session::Sessions;
use crate::daos::table_data_versions::TableDataVersions;
use crate::daos::topology::Topology;
use crate::generic::chunked::Chunked;
use crate::generic::grain::{AllVersions, Current, Locked, Unversioned};
use crate::generic::reader::Reader;
use crate::primitives::table::{
    delete_query, insert_or_ignore_query, insert_query, update_query, upsert_query,
};
use sqlx::postgres::PgQueryResult;
use std::future::Future;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    AddressesOneRow, DeleteDao, InsertDao, InsertableFrom, ListDto, SelectDao, UnversionedDao,
    UpdateDao, UpsertDao,
};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

pub trait Queries: QueryExecutor {
    fn insert<D: InsertDao + InsertableFrom<Self::Scope>>(
        self,
        dao: &D,
    ) -> impl Future<Output = Result<PgQueryResult, SqlError>> + Send
    where
        Self: Serves<D> + Writes,
    {
        async move {
            insert_query::<D>(dao)?
                .build_query()
                .execute(self)
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))
        }
    }

    /// On conflict over [`key_columns`](UpsertDao::key_columns), replace the
    /// [`value_columns`](UpsertDao::value_columns).
    fn upsert<D: UpsertDao + InsertableFrom<Self::Scope>>(
        self,
        dao: &D,
    ) -> impl Future<Output = Result<PgQueryResult, SqlError>> + Send
    where
        Self: Serves<D> + Writes,
    {
        async move {
            upsert_query::<D>(dao)?
                .build_query()
                .execute(self)
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))
        }
    }

    /// Write-once: on conflict the first row per key wins.
    fn insert_or_ignore<D: UpsertDao + InsertableFrom<Self::Scope>>(
        self,
        dao: &D,
    ) -> impl Future<Output = Result<PgQueryResult, SqlError>> + Send
    where
        Self: Serves<D> + Writes,
    {
        async move {
            insert_or_ignore_query::<D>(dao)?
                .build_query()
                .execute(self)
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))
        }
    }

    fn update<U: UpdateDao>(
        self,
        dao: &U,
        condition: impl SqlCondition<U::FilterBy>,
    ) -> impl Future<Output = Result<PgQueryResult, SqlError>> + Send
    where
        U::FilterBy: UnversionedDao,
        Self: Serves<U> + Writes,
    {
        async move {
            update_query(dao, condition)?
                .build_query()
                .execute(self)
                .await
                .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))
        }
    }

    fn delete<D: DeleteDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<PgQueryResult, SqlError>> + Send
    where
        Self: Serves<D> + Writes,
    {
        async move {
            let values = condition.display_values();
            delete_query::<D>(condition)?
                .build_query()
                .execute(self)
                .await
                .map_err(|e| SqlError::on_write::<D>(e, &values))
        }
    }

    fn fetch_one<D: SelectDao + UnversionedDao>(
        self,
        condition: impl AddressesOneRow<D, Self::Scope>,
    ) -> impl Future<Output = Result<D, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_one(condition)
    }

    fn fetch_optional<D: SelectDao + UnversionedDao>(
        self,
        condition: impl AddressesOneRow<D, Self::Scope>,
    ) -> impl Future<Output = Result<Option<D>, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_optional(condition)
    }

    fn fetch_all<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_all(condition)
    }

    /// The first row in the DAO's default order, of however many match.
    fn fetch_first<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<Option<D>, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_first(condition)
    }

    /// The DAO's default ordering reversed, `LIMIT 1`.
    fn fetch_last<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<Option<D>, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_last(condition)
    }

    /// `offset = 0` is the most recent match.
    fn fetch_nth_last<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
        offset: u64,
    ) -> impl Future<Output = Result<Option<D>, SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).fetch_nth_last(condition, offset)
    }

    fn assert_exists<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<(), SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).assert_exists(condition)
    }

    fn assert_not_exists<D: SelectDao + UnversionedDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> impl Future<Output = Result<(), SqlError>> + Send
    where
        Self: Serves<D>,
    {
        unversioned(self).assert_not_exists(condition)
    }

    fn list<T>(
        self,
        condition: impl SqlCondition<T::Dao>,
        params: &ListQueryParams<T>,
    ) -> impl Future<Output = Result<Vec<T>, SqlError>> + Send
    where
        T: ListDto,
        T::Dao: UnversionedDao,
        Self: Serves<T::Dao>,
    {
        unversioned(self).list(condition, params)
    }

    /// The interval-table door: the open SCD2 intervals
    /// (`valid_to = 'infinity'`) until [`as_of`](Reader::as_of) moves the
    /// instant.
    fn scd2(self) -> Reader<Self, Current> {
        Reader {
            executor: self,
            grain: Current,
        }
    }

    /// The version log of a `TimelineDao`: every version row, no time
    /// predicate. The only door to a `timeline` relation.
    fn timeline(self) -> Reader<Self, AllVersions> {
        Reader {
            executor: self,
            grain: AllVersions,
        }
    }

    /// The wordless reads, holding row locks until the transaction ends.
    fn for_update(self) -> Reader<Self, Locked>
    where
        Self: Writes,
    {
        Reader {
            executor: self,
            grain: Locked,
        }
    }

    /// The same operations over a slice that may exceed the bind-parameter
    /// limit.
    fn chunked(self) -> Chunked<Self> {
        Chunked {
            executor: self,
            grain: Unversioned,
        }
    }

    fn logical_runs(self) -> LogicalRuns<Self> {
        LogicalRuns { executor: self }
    }

    fn physical_runs(self) -> PhysicalRuns<Self> {
        PhysicalRuns { executor: self }
    }

    fn table_data_versions(self) -> TableDataVersions<Self> {
        TableDataVersions { executor: self }
    }

    fn executions(self) -> Executions<Self> {
        Executions { executor: self }
    }

    fn dependencies(self) -> Dependencies<Self> {
        Dependencies { executor: self }
    }

    fn sessions(self) -> Sessions<Self> {
        Sessions { executor: self }
    }

    fn metrics(self) -> Metrics<Self> {
        Metrics { executor: self }
    }

    fn projects(self) -> Projects<Self> {
        Projects { executor: self }
    }

    fn topology(self) -> Topology<Self> {
        Topology { executor: self }
    }
}

impl<E: QueryExecutor> Queries for E {}

fn unversioned<E>(executor: E) -> Reader<E, Unversioned> {
    Reader {
        executor,
        grain: Unversioned,
    }
}
