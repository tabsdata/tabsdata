//
// Copyright 2026 Tabs Data Inc.
//

//! Partition-scoped point-in-time reads — the safe versioned surface.
//!
//! Every method here pushes the partition equality (or `IN (..)`) into
//! the ranked CTE, so the window only ranks rows for the requested
//! partition(s) — O(rows-in-partition), not O(rows-in-table). For
//! cross-partition reads (when the partition isn't known at the call
//! site) walk through [`crate::Historic`] instead — that surface is
//! the explicit slow-path opt-in.

use crate::QueryExecutor;
use crate::generic::{Bound, Unbound};
use crate::primitives::chunked::{MAX_QUERY_CONDITIONS, warn_if_chunked};
use crate::primitives::versioned::{
    partitioned_latest_ranked_cte, select_latest_from_partitioned_cte,
};
use sea_query::Condition;
use std::marker::PhantomData;
use ta_objects::errors::SqlError;
use ta_objects::{SeaValue, StateFilter, VersionedDao};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;

query_handle! {
    /// Entry trait providing [`versioned`](VersionedEntry::versioned) on
    /// every [`QueryExecutor`](crate::QueryExecutor). Returns a [`Versioned`]
    /// partition-scoped point-in-time handle.
    pub trait VersionedEntry {
        fn versioned<T>(self, on: &T) -> Versioned<'_, T, Self> {
            Versioned {
                on,
                condition: None,
                executor: self,
                _phantom: PhantomData,
            }
        }
    }
}

/// Partition-scoped point-in-time surface. Reached via
/// [`VersionedEntry::versioned`].
pub struct Versioned<'a, T, E, S = Unbound> {
    pub(crate) on: &'a T,
    pub(crate) condition: Option<Condition>,
    pub(crate) executor: E,
    pub(crate) _phantom: PhantomData<S>,
}

impl<'a, T, E> Versioned<'a, T, E, Unbound> {
    /// Bind a [`StateFilter`] (and pin the DAO type via inference).
    pub fn state<D: VersionedDao<VersionOrder = T>>(
        self,
        filter: StateFilter<D>,
    ) -> Versioned<'a, T, E, Bound<D>> {
        Versioned {
            on: self.on,
            condition: Some(filter.condition().clone()),
            executor: self.executor,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn with_dao<D>(self) -> Versioned<'a, T, E, Bound<D>> {
        Versioned {
            on: self.on,
            condition: self.condition,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Versioned<'a, T, E> — All state. Pins the DAO via `with_dao::<D>()` and
// delegates to the bound impl with `condition: None`. `fetch_all_for_partition`
// is the exception: it queries the raw timeline (no rn=1 dedup) and never
// applies a state filter.
// ---------------------------------------------------------------------------

impl<'a, T, E> Versioned<'a, T, E, Unbound>
where
    E: QueryExecutor,
{
    /// Latest version in `partition` at the wrapper's `on` (All state).
    pub async fn fetch_one<D>(self, partition: &D::Partition) -> Result<D, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_one(partition).await
    }

    /// Optional variant of [`fetch_one`](Self::fetch_one).
    pub async fn fetch_optional<D>(self, partition: &D::Partition) -> Result<Option<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_optional(partition).await
    }

    /// Every row in `partition` up to the wrapper's `on` — full
    /// event-log history, no rn=1 dedup. Use this on event-log tables
    /// (`#[dao(... event_log, ...)]`) when callers need the audit
    /// trail of state transitions, not just the latest event.
    pub async fn fetch_all_for_partition<D>(
        self,
        partition: &D::Partition,
    ) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        T: SeaValue,
        E: Domained<Domain = D::Db>,
    {
        use sea_query::{Expr, ExprTrait, Query};
        let r: Vec<D> = Query::select()
            .columns(D::columns())
            .from(D::table())
            .and_where(Expr::col(D::partition_col()).eq(partition.as_sea_value()))
            .and_where(Expr::col(D::version_order_col()).lte(self.on.as_sea_value()))
            .order_by_columns(D::order_by())
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Chunked latest-per-partition (All state).
    pub async fn fetch_all_chunked<D>(self, partitions: &[D::Partition]) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao<VersionOrder = T>,
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_all_chunked(partitions).await
    }
}

// ---------------------------------------------------------------------------
// Versioned<'a, T, E, Bound<D>> — bound to DAO and an optional state filter.
// ---------------------------------------------------------------------------

impl<'a, T, D, E> Versioned<'a, T, E, Bound<D>>
where
    D: VersionedDao<VersionOrder = T>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    /// Latest version in `partition` at `on` matching the bound state.
    pub async fn fetch_one(self, partition: &D::Partition) -> Result<D, SqlError> {
        let r = select_latest_from_partitioned_cte::<D>(self.condition.as_ref())?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(partitioned_latest_ranked_cte::<D>(
                std::slice::from_ref(partition),
                self.on,
            )?)
            .build_query_as()
            .fetch_one(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Optional variant of [`fetch_one`](Self::fetch_one).
    pub async fn fetch_optional(self, partition: &D::Partition) -> Result<Option<D>, SqlError> {
        let r = select_latest_from_partitioned_cte::<D>(self.condition.as_ref())?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(partitioned_latest_ranked_cte::<D>(
                std::slice::from_ref(partition),
                self.on,
            )?)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Chunked latest-per-partition matching the bound state.
    pub async fn fetch_all_chunked(
        mut self,
        partitions: &[D::Partition],
    ) -> Result<Vec<D>, SqlError> {
        warn_if_chunked::<D>(partitions.len());
        let on: &T = self.on;
        let state_cond: Option<Condition> = self.condition.clone();
        let mut results = Vec::new();
        for chunk in partitions.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = select_latest_from_partitioned_cte::<D>(state_cond.as_ref())?
                .order_by_columns(D::order_by())
                .take()
                .with(partitioned_latest_ranked_cte::<D>(chunk, on)?)
                .build_query_as()
                .fetch_all(self.executor.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }
}
