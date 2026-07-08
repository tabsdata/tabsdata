//
// Copyright 2026 Tabs Data Inc.
//

use crate::QueryExecutor;
use crate::generic::{Bound, Unbound};
use crate::primitives::chunked::{
    MAX_QUERY_CONDITIONS, fetch_all_current_chunk_query, warn_if_chunked,
};
use sea_query::{Condition, Query, SelectStatement};
use std::marker::PhantomData;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{CurrentDao, DaoCore, ListDto, ListFilter, StateFilter};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

query_handle! {
    /// Entry trait providing [`current`](CurrentEntry::current) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Current`]
    /// `_current`-view handle.
    pub trait CurrentEntry {
        fn current(self) -> Current<Self> {
            Current {
                condition: None,
                executor: self,
                _phantom: PhantomData,
            }
        }
    }
}

/// `_current` view surface. `S = Unbound` (the default) is reached via
/// [`CurrentEntry::current`]; `S = Bound<D>` is reached via
/// [`Current::state`] or [`Current::with_dao`].
pub struct Current<E, S = Unbound> {
    pub(crate) condition: Option<Condition>,
    pub(crate) executor: E,
    pub(crate) _phantom: PhantomData<S>,
}

impl<E> Current<E, Unbound> {
    /// Bind a [`StateFilter`] (and pin the DAO type via inference).
    /// Subsequent leaf methods don't need a DAO turbofish.
    pub fn state<D: CurrentDao>(self, filter: StateFilter<D>) -> Current<E, Bound<D>> {
        Current {
            condition: Some(filter.condition().clone()),
            executor: self.executor,
            _phantom: PhantomData,
        }
    }

    /// Pin the DAO without a state filter. Used by the `Unbound`
    /// leaves to delegate to the `Bound<D>` impl with `condition: None`.
    pub(crate) fn with_dao<D>(self) -> Current<E, Bound<D>> {
        Current {
            condition: self.condition,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Current<E> — All state (no state filter). Each method pins the DAO via
// `with_dao::<D>()` and delegates to the bound impl, which sees
// `condition: None` and skips the redundant AND-TRUE in the WHERE clause.
// ---------------------------------------------------------------------------

impl<E> Current<E, Unbound>
where
    E: QueryExecutor,
{
    /// Fetch the single row matching `condition` (All state).
    pub async fn fetch_one<D: CurrentDao>(self, condition: impl SqlCondition) -> Result<D, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_one(condition).await
    }

    /// Fetch the row matching `condition`, or `None` (All state).
    pub async fn fetch_optional<D: CurrentDao>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Option<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_optional(condition).await
    }

    /// Fetch every row matching `condition` (All state).
    pub async fn fetch_all<D: CurrentDao>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_all(condition).await
    }

    /// Chunked fetch (All state) — every row matching any of `values`.
    pub async fn fetch_all_chunked<D: CurrentDao, V: SqlCondition>(
        self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        self.with_dao::<D>().fetch_all_chunked(values).await
    }

    /// Filtered, paginated list over the `_current` view (All state).
    pub async fn list<D, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: CurrentDao,
        E: Domained<Domain = <<D as ListDto>::Dao as DaoCore>::Db>,
    {
        self.with_dao::<<D as ListDto>::Dao>()
            .list(condition, filter, params)
            .await
    }
}

// ---------------------------------------------------------------------------
// Current<E, Bound<D>> — bound to DAO and an optional state filter.
// ---------------------------------------------------------------------------

impl<D, E> Current<E, Bound<D>>
where
    D: CurrentDao,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    /// Fetch the single row matching `condition` and the bound state.
    pub async fn fetch_one(self, condition: impl SqlCondition) -> Result<D, SqlError> {
        select_current::<D>(condition, self.condition.as_ref())?
            .build_query_as()
            .fetch_one(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    /// Fetch the row matching `condition` and state, or `None`.
    pub async fn fetch_optional(self, condition: impl SqlCondition) -> Result<Option<D>, SqlError> {
        select_current::<D>(condition, self.condition.as_ref())?
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    /// Fetch every row matching `condition` and the bound state.
    pub async fn fetch_all(self, condition: impl SqlCondition) -> Result<Vec<D>, SqlError> {
        select_current::<D>(condition, self.condition.as_ref())?
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    /// Chunked variant of [`fetch_all`](Self::fetch_all).
    pub async fn fetch_all_chunked<V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError> {
        warn_if_chunked::<D>(values.len());
        let state_cond = self.condition.clone();
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = fetch_all_current_chunk_query::<D, V>(chunk, state_cond.as_ref())?
                .build_query_as()
                .fetch_all(self.executor.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }

    /// Filtered, paginated list, mapped through `L::from_dao`. The DTO's
    /// associated `Dao` must match the wrapper's bound DAO.
    pub async fn list<L, F: ListFilter>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<L>,
    ) -> Result<Vec<L>, SqlError>
    where
        L: ListDto<Dao = D>,
    {
        let base = select_current::<D>(condition, self.condition.as_ref())?
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

// ---------------------------------------------------------------------------
// Internal SQL builder.
// ---------------------------------------------------------------------------

fn select_current<D>(
    condition: impl SqlCondition,
    state_cond: Option<&Condition>,
) -> Result<SelectStatement, SqlError>
where
    D: CurrentDao,
{
    let mut where_cond = Condition::all().add(resolve_condition::<D>(condition)?);
    if let Some(s) = state_cond {
        where_cond = where_cond.add(s.clone());
    }
    let query = Query::select()
        .columns(D::columns())
        .from(D::current_table_name())
        .cond_where(where_cond)
        .take();
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;
    use ta_objects::Dao;

    #[ta_objects::typed(string)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestPartition;

    #[ta_objects::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::typed(string)]
    struct TestDefinedOn;

    #[Dao]
    #[dao(
        sql_table = "test_current_table",
        versioned(
            pk = "id",
            version_by = "defined_on",
            partition_by = "partition_id",
            current = "test_current_view"
        ),
        states(Active = TestStatus::Active)
    )]
    struct TestCurrentDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestStatus,
        defined_on: TestDefinedOn,
    }

    #[test]
    fn test_select_current_no_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_current::<TestCurrentDao>(&id, None)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains(r#"FROM "test_current_view""#), "{sql}");
        assert!(sql.contains(r#""partition_id" = 'p1'"#), "{sql}");
        assert!(!sql.contains(r#""status" = 'A'"#), "{sql}");
    }

    #[test]
    fn test_select_current_with_active_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let active = TestCurrentDao::active();
        let sql = select_current::<TestCurrentDao>(&id, Some(active.condition()))
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains(r#"FROM "test_current_view""#), "{sql}");
        assert!(sql.contains(r#""partition_id" = 'p1'"#), "{sql}");
        assert!(sql.contains(r#""status" = 'A'"#), "{sql}");
    }

    #[test]
    fn test_select_current_no_condition() {
        let sql = select_current::<TestCurrentDao>((), None)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains(r#"FROM "test_current_view""#), "{sql}");
    }
}
