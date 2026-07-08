//
// Copyright 2026 Tabs Data Inc.
//

//! Live-table CRUD surface. Reached via [`LiveEntry::live`] (blanket
//! impl on every [`QueryExecutor`](crate::QueryExecutor)). The
//! [`Live<E>`] wrapper's inherent methods do the work — no CTE, no
//! `_current` view, no time pinning, operations run directly against
//! the DAO's actual table.
//!
//! Reads are gated on [`LiveDao`] — append-only event-log tables
//! (`#[dao(... event_log, ...)]`) don't impl `LiveDao`, so the
//! compiler stops you from calling the bare `fetch_*` methods on them
//! and points you at `current()` / `versioned()` / `historic()` where
//! the version semantics are explicit.
//!
//! Insert/update/delete stay open to any `Dao` / `DaoCore` — every
//! table receives INSERTs (versioned tables get new version rows) and
//! updates/deletes are legitimate for backfills.

use crate::QueryExecutor;
use sea_query::{Query, SelectStatement};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{Dao, DaoCore, DaoUpdate, ListDto, ListFilter, LiveDao, StateFilter};
use td_database::domain::Domained;
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

query_handle! {
    /// Entry trait providing [`live`](LiveEntry::live) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Live`] CRUD handle.
    pub trait LiveEntry {
        fn live(self) -> Live<Self> {
            Live { executor: self }
        }
    }
}

/// Live-table CRUD wrapper. Constructed via [`LiveEntry::live`].
pub struct Live<E> {
    pub(crate) executor: E,
}

impl<E> Live<E>
where
    E: QueryExecutor,
{
    /// Insert one row.
    pub async fn insert<D: Dao>(self, dao: &D) -> Result<DynamicQueryResult, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = insert_query::<D>(dao)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    /// Fetch the single row matching `condition`. Errors if missing.
    pub async fn fetch_one<D: LiveDao>(self, condition: impl SqlCondition) -> Result<D, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_one(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch the row matching `condition`, or `None` if no row matches.
    pub async fn fetch_optional<D: LiveDao>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Option<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch every row matching `condition`. No state filter.
    pub async fn fetch_all<D: LiveDao>(
        self,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select::<D>(condition)?
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Update every row matching `condition` with the fields set on `dao`.
    pub async fn update<U: DaoUpdate, D: DaoCore>(
        self,
        dao: &U,
        condition: impl SqlCondition,
    ) -> Result<DynamicQueryResult, SqlError>
    where
        E: Domained<Domain = U::Db>,
    {
        let r = update_query::<U, D>(dao, condition)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    /// Delete every row matching `condition`.
    pub async fn delete<D: DaoCore>(
        self,
        condition: impl SqlCondition,
    ) -> Result<DynamicQueryResult, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = delete_query::<D>(condition)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Filtered, paginated list for a `ListDto`. Maps each row through
    /// `D::from_dao`.
    pub async fn list<F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: LiveDao,
        E: Domained<Domain = <<D as ListDto>::Dao as DaoCore>::Db>,
    {
        let base = Query::select()
            .columns(<D as ListDto>::Dao::columns())
            .from(<D as ListDto>::Dao::table())
            .cond_where(resolve_condition::<<D as ListDto>::Dao>(condition)?)
            .cond_where(filter.condition::<<D as ListDto>::Dao>()?)
            .take();
        let mut query = params.apply_to(base);
        let daos: Vec<<D as ListDto>::Dao> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<<D as ListDto>::Dao>)?;
        daos.iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    /// `fetch_all` with `state`'s condition ANDed into the WHERE.
    pub async fn fetch_all_in<D: LiveDao>(
        self,
        state: StateFilter<D>,
        condition: impl SqlCondition,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_in::<D>(&state, condition)?
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Find the nth most recent row matching `condition` and `state`,
    /// using the DAO's default ordering. `offset = 0` returns the most
    /// recent match, `offset = 1` the second most recent, and so on.
    pub async fn fetch_latest_before<D: LiveDao>(
        self,
        state: StateFilter<D>,
        condition: impl SqlCondition,
        offset: u64,
    ) -> Result<Option<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_latest_before::<D>(&state, condition, offset)?
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }
}

// SQL builder helpers — shape the statements the inherent methods above
// execute.

pub(crate) fn select<D>(condition: impl SqlCondition) -> Result<SelectStatement, SqlError>
where
    D: Dao,
{
    let query = Query::select()
        .from(D::table())
        .columns(D::columns())
        .cond_where(resolve_condition::<D>(condition)?)
        .order_by_columns(D::order_by())
        .take();
    Ok(query)
}

pub(crate) fn insert_query<D: Dao>(dao: &D) -> Result<sea_query::InsertStatement, SqlError> {
    let mut query = Query::insert();
    query
        .into_table(D::table())
        .columns(D::columns())
        .values(dao.insert_values())?;
    Ok(query)
}

pub(crate) fn update_query<U: DaoUpdate, D: DaoCore>(
    dao: &U,
    condition: impl SqlCondition,
) -> Result<sea_query::UpdateStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    let query = Query::update()
        .table(U::table())
        .values(dao.update_values())
        .cond_where(resolve_condition::<D>(condition)?)
        .take();
    Ok(query)
}

pub(crate) fn delete_query<D: DaoCore>(
    condition: impl SqlCondition,
) -> Result<sea_query::DeleteStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    let query = Query::delete()
        .from_table(D::table())
        .cond_where(resolve_condition::<D>(condition)?)
        .take();
    Ok(query)
}

pub(crate) fn select_in<D: LiveDao>(
    state: &StateFilter<D>,
    condition: impl SqlCondition,
) -> Result<SelectStatement, SqlError> {
    let query = select::<D>(condition)?
        .cond_where(state.condition().clone())
        .take();
    Ok(query)
}

pub(crate) fn select_latest_before<D: LiveDao>(
    state: &StateFilter<D>,
    condition: impl SqlCondition,
    offset: u64,
) -> Result<SelectStatement, SqlError> {
    let mut query = select::<D>(condition)?
        .cond_where(state.condition().clone())
        .limit(1)
        .take();
    if offset > 0 {
        query.offset(offset);
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;
    use ta_objects::Buildable;

    #[ta_objects::typed(string)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestName;

    #[ta_objects::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "test_basic", states(Active = TestStatus::Active))]
    struct TestDao {
        id: TestId,
        name: TestName,
        status: TestStatus,
    }

    #[test]
    fn test_select_with_condition() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select::<TestDao>(&id)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_no_condition() {
        let sql = select::<TestDao>(()).unwrap().to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE TRUE ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_insert_query_sql() {
        let dao = TestDao::builder()
            .id(TestId::try_from("i1").unwrap())
            .name(TestName::try_from("n1").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let sql = insert_query::<TestDao>(&dao)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"INSERT INTO "test_basic" ("id", "name", "status") VALUES ('i1', 'n1', 'A')"#
        );
    }
}
