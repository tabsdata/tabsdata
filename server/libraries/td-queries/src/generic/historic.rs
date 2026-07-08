//
// Copyright 2026 Tabs Data Inc.
//

//! Cross-partition ranked-CTE reads — the slow-path versioned surface.
//!
//! Every method here builds a ranked CTE over the entire versioned
//! table to find the latest row per partition at the wrapper's `on`,
//! then filters at the outer scope. Cost is O(table size) per call —
//! reach this surface only when the partition isn't known at the call
//! site (e.g. by-name lookups). For partition-known reads, prefer
//! [`crate::Versioned`].

use crate::QueryExecutor;
use crate::generic::{Bound, Unbound};
use crate::primitives::chunked::{MAX_QUERY_CONDITIONS, fetch_all_at_chunk_query, warn_if_chunked};
use crate::primitives::versioned::{latest_ranked_cte, select_at, select_latest_from_cte};
use sea_query::Condition;
use std::marker::PhantomData;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{DaoCore, ListDto, ListFilter, StateFilter, VersionedDao};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

query_handle! {
    /// Entry trait providing [`historic`](HistoricEntry::historic) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Historic`]
    /// cross-partition scan handle.
    pub trait HistoricEntry {
        fn historic<T>(self, on: &T) -> Historic<'_, T, Self> {
            Historic {
                on,
                condition: None,
                executor: self,
                _phantom: PhantomData,
            }
        }
    }
}

/// Cross-partition slow-scan surface. Reached via
/// [`HistoricEntry::historic`].
pub struct Historic<'a, T, E, S = Unbound> {
    pub(crate) on: &'a T,
    pub(crate) condition: Option<Condition>,
    pub(crate) executor: E,
    pub(crate) _phantom: PhantomData<S>,
}

impl<'a, T, E> Historic<'a, T, E, Unbound> {
    /// Bind a [`StateFilter`] (and pin the DAO type via inference).
    pub fn state<D: VersionedDao<VersionOrder = T>>(
        self,
        filter: StateFilter<D>,
    ) -> Historic<'a, T, E, Bound<D>> {
        Historic {
            on: self.on,
            condition: Some(filter.condition().clone()),
            executor: self.executor,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn with_dao<D>(self) -> Historic<'a, T, E, Bound<D>> {
        Historic {
            on: self.on,
            condition: self.condition,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Historic<'a, T, E> — All state. Pins the DAO via `with_dao::<D>()` and
// delegates to the bound impl with `condition: None`.
// ---------------------------------------------------------------------------

impl<'a, T, E> Historic<'a, T, E, Unbound>
where
    E: QueryExecutor,
{
    /// Latest row matching `condition` across all partitions at `on`.
    pub async fn fetch_one<D>(self, condition: impl SqlCondition) -> Result<D, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_one(condition).await
    }

    /// Optional variant of [`fetch_one`](Self::fetch_one).
    pub async fn fetch_optional<D>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Option<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_optional(condition).await
    }

    /// All latest rows matching `condition`.
    pub async fn fetch_all<D>(self, condition: impl SqlCondition) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_all(condition).await
    }

    /// Chunked variant of [`fetch_all`](Self::fetch_all).
    ///
    /// **Cost warning**: each chunk produces *one* ranked CTE over the
    /// entire versioned table at `on`. A slice of N values issues
    /// ⌈N / [`MAX_QUERY_CONDITIONS`]⌉ DB queries, **each** scanning the
    /// full table. Total work scales with `chunks × table_size`, not
    /// with the number of values. Prefer
    /// [`crate::Versioned::fetch_all_chunked`] when the partition
    /// column is known.
    pub async fn fetch_all_chunked<D, V: SqlCondition>(
        self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_all_chunked(values).await
    }

    /// Latest-per-partition listing at `on`, filtered & paginated.
    pub async fn list_versions<D, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = <<D as ListDto>::Dao as DaoCore>::Db>,
    {
        self.with_dao::<<D as ListDto>::Dao>()
            .list_versions(condition, filter, params)
            .await
    }

    /// Listing of every row at `on` — all versions, no rn=1 dedup.
    pub async fn list_full<D, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = <<D as ListDto>::Dao as DaoCore>::Db>,
    {
        self.with_dao::<<D as ListDto>::Dao>()
            .list_full(condition, filter, params)
            .await
    }
}

// ---------------------------------------------------------------------------
// Historic<'a, T, E, Bound<D>> — bound to DAO and an optional state filter.
// ---------------------------------------------------------------------------

impl<'a, T, D, E> Historic<'a, T, E, Bound<D>>
where
    D: VersionedDao<VersionOrder = T>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    /// Latest row matching `condition` and the bound state.
    pub async fn fetch_one(self, condition: impl SqlCondition) -> Result<D, SqlError> {
        let r = select_latest_from_cte::<D>(condition, self.condition.as_ref())?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(latest_ranked_cte::<D>(self.on)?)
            .build_query_as()
            .fetch_one(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Optional variant.
    pub async fn fetch_optional(self, condition: impl SqlCondition) -> Result<Option<D>, SqlError> {
        let r = select_latest_from_cte::<D>(condition, self.condition.as_ref())?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(latest_ranked_cte::<D>(self.on)?)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// All latest rows matching `condition` and the bound state.
    pub async fn fetch_all(self, condition: impl SqlCondition) -> Result<Vec<D>, SqlError> {
        let r = select_latest_from_cte::<D>(condition, self.condition.as_ref())?
            .order_by_columns(D::order_by())
            .take()
            .with(latest_ranked_cte::<D>(self.on)?)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Chunked variant of [`fetch_all`](Self::fetch_all). Same cost
    /// warning as [`Historic::fetch_all_chunked`].
    pub async fn fetch_all_chunked<V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError> {
        warn_if_chunked::<D>(values.len());
        let on: &T = self.on;
        let state_cond: Option<Condition> = self.condition.clone();
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = fetch_all_at_chunk_query::<D, V>(chunk, on, state_cond.as_ref())?
                .build_query_as()
                .fetch_all(self.executor.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }

    /// Latest-per-partition listing matching the bound state.
    pub async fn list_versions<L, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<L>,
    ) -> Result<Vec<L>, SqlError>
    where
        L: ListDto<Dao = D>,
    {
        let with = latest_ranked_cte::<D>(self.on)?;
        let base = select_latest_from_cte::<D>(condition, self.condition.as_ref())?
            .cond_where(filter.condition::<D>()?)
            .take();
        let mut query = params.apply_to(base).with(with);
        let daos: Vec<D> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        daos.iter()
            .map(|dao| L::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    /// Full audit-trail listing matching the bound state.
    pub async fn list_full<L, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<L>,
    ) -> Result<Vec<L>, SqlError>
    where
        L: ListDto<Dao = D>,
    {
        let base = select_at::<D>(condition, self.on, self.condition.as_ref())?
            .cond_where(filter.condition::<D>()?)
            .take();
        let mut query = params.apply_to(base);
        let daos: Vec<D> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        daos.iter()
            .map(|dao| L::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}
