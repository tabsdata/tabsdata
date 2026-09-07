//
// Copyright 2026 Tabsdata Inc.
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
//!
//! `upsert` / `insert_or_ignore` are gated on [`KeyedDao`] — only tables
//! that declare a logical key via `#[dao(key(...))]` can be
//! `INSERT … ON CONFLICT`'d, so the append-only / versioned tables
//! (which never conflict-replace) can't reach them.

use crate::QueryExecutor;
use sea_query::{IntoTableRef, LockBehavior, LockType, OnConflict, Query, SelectStatement};
use sqlx::postgres::PgQueryResult;
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Dao, DaoCore, DaoUpdate, FromDao, KeyedDao, ListDto, LiveDao, StateFilter};
use td_database::domain::Domained;
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
    pub async fn insert<D: Dao>(self, dao: &D) -> Result<PgQueryResult, SqlError>
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

    /// Upsert one row: insert it, and on a conflict over the DAO's
    /// [`key_columns`](KeyedDao::key_columns) replace its
    /// [`value_columns`](KeyedDao::value_columns) (every non-key column).
    pub async fn upsert<D: KeyedDao>(self, dao: &D) -> Result<PgQueryResult, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = upsert_query::<D>(dao)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    /// Insert one row, ignoring a conflict over the DAO's
    /// [`key_columns`](KeyedDao::key_columns) (`ON CONFLICT DO NOTHING`):
    /// a write-once / idempotent insert where the first row per key wins.
    pub async fn insert_or_ignore<D: KeyedDao>(self, dao: &D) -> Result<PgQueryResult, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = insert_or_ignore_query::<D>(dao)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))?;
        Ok(r)
    }

    /// Fetch the single row matching `condition`. Errors if missing.
    pub async fn fetch_one<D: LiveDao>(self, condition: impl SqlCondition<D>) -> Result<D, SqlError>
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
        condition: impl SqlCondition<D>,
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

    /// Assert a row matching `condition` exists — a clean "could not find"
    /// error instead of the raw fetch error path.
    pub async fn assert_exists<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let vals = condition.display_values();
        let found: Option<D> = self.fetch_optional(condition).await?;
        match found {
            Some(_) => Ok(()),
            None => {
                let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
                Err(SqlError::CouldNotFindEntity(
                    D::display_name().to_string(),
                    format_hint(&cols, &vals),
                ))
            }
        }
    }

    /// Assert no row matches `condition` — a clean "already exists" error
    /// instead of the unique-constraint violation a later write would raise.
    pub async fn assert_not_exists<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let vals = condition.display_values();
        let found: Option<D> = self.fetch_optional(condition).await?;
        match found {
            Some(_) => {
                let cols: Vec<String> = vals.iter().map(|(k, _)| k.clone()).collect();
                Err(SqlError::EntityAlreadyExists(
                    D::display_name().to_string(),
                    format_hint(&cols, &vals),
                ))
            }
            None => Ok(()),
        }
    }

    /// Fetch the last row matching `condition` in the DAO's default
    /// ordering (the ordering *reversed*, `LIMIT 1`), or `None` if no row
    /// matches. For a DAO ordered `seq ASC` this is the max-`seq` row.
    /// Mirror of [`fetch_latest_before`](Self::fetch_latest_before), which
    /// walks the default ordering forward (newest-first for DESC DAOs).
    pub async fn fetch_last<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_last::<D>(condition)?
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch the row matching `condition` **for update** (`SELECT … FOR
    /// UPDATE`, `LIMIT 1`), or `None` if none matches. The row lock
    /// serializes a concurrent read-modify-write on the same row behind
    /// this one.
    pub async fn fetch_optional_for_update<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_for_update::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch the single row matching `condition` **for update** (`SELECT …
    /// FOR UPDATE`, `LIMIT 1`), erroring if missing. The row lock serializes a
    /// concurrent read-modify-write on the same row behind this one.
    pub async fn fetch_one_for_update<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<D, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_for_update::<D>(condition)?
            .limit(1)
            .build_query_as()
            .fetch_one(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch every row matching `condition` **for update**. Use it wherever the
    /// rows feed a decision this transaction then writes back: the row locks
    /// make the read-decide-write atomic against a concurrent writer of them.
    /// Base-table DAOs only — PostgreSQL refuses `FOR UPDATE` through a view
    /// that outer-joins.
    pub async fn fetch_all_for_update<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let r = select_for_update::<D>(condition)?
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// Fetch every row matching `condition`. No state filter.
    pub async fn fetch_all<D: LiveDao>(
        self,
        condition: impl SqlCondition<D>,
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
        condition: impl SqlCondition<D>,
    ) -> Result<PgQueryResult, SqlError>
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
        condition: impl SqlCondition<D>,
    ) -> Result<PgQueryResult, SqlError>
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
    pub async fn list<D>(
        self,
        condition: impl SqlCondition<<D as FromDao>::Dao>,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as FromDao>::Dao: LiveDao,
        E: Domained<Domain = <<D as FromDao>::Dao as DaoCore>::Db>,
    {
        let base = Query::select()
            .columns(<D as FromDao>::Dao::columns())
            .from(<D as FromDao>::Dao::table())
            .cond_where(condition.resolve())
            .take();
        let mut query = params.apply_to(base);
        let daos: Vec<<D as FromDao>::Dao> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<<D as FromDao>::Dao>)?;
        daos.into_iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    /// `fetch_all` with `state`'s condition ANDed into the WHERE.
    pub async fn fetch_all_in<D: LiveDao>(
        self,
        state: StateFilter<D>,
        condition: impl SqlCondition<D>,
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

    /// [`fetch_all_in`](Self::fetch_all_in) as a claim: `FOR UPDATE OF <table>
    /// SKIP LOCKED`, so concurrent claimers take disjoint sets and a skipped
    /// row falls to a later pass. Base-table DAOs only — PostgreSQL refuses
    /// `FOR UPDATE` through a view that outer-joins.
    pub async fn claim_all_in<D: LiveDao>(
        self,
        state: StateFilter<D>,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
    {
        let mut query = select_in::<D>(&state, condition)?;
        let r = claim::<D>(&mut query)
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
        condition: impl SqlCondition<D>,
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

pub(crate) fn select<D>(condition: impl SqlCondition<D>) -> Result<SelectStatement, SqlError>
where
    D: Dao,
{
    let query = Query::select()
        .from(D::table())
        .columns(D::columns())
        .cond_where(condition.resolve())
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

/// `insert_query` with an `ON CONFLICT (key) DO UPDATE` clause replacing the
/// DAO's value columns.
pub(crate) fn upsert_query<D: KeyedDao>(dao: &D) -> Result<sea_query::InsertStatement, SqlError> {
    let mut on_conflict = OnConflict::columns(D::key_columns());
    on_conflict.update_columns(D::value_columns());
    let mut query = insert_query::<D>(dao)?;
    query.on_conflict(on_conflict);
    Ok(query)
}

/// `insert_query` with an `ON CONFLICT (key) DO NOTHING` clause.
pub(crate) fn insert_or_ignore_query<D: KeyedDao>(
    dao: &D,
) -> Result<sea_query::InsertStatement, SqlError> {
    let mut on_conflict = OnConflict::columns(D::key_columns());
    on_conflict.do_nothing();
    let mut query = insert_query::<D>(dao)?;
    query.on_conflict(on_conflict);
    Ok(query)
}

/// `select` with the DAO's ordering reversed and `LIMIT 1` — the last row
/// in default order.
pub(crate) fn select_last<D: Dao>(
    condition: impl SqlCondition<D>,
) -> Result<SelectStatement, SqlError> {
    let query = select::<D>(condition)?
        .clear_order_by()
        .order_by_columns(reverse_order::<D>())
        .limit(1)
        .take();
    Ok(query)
}

/// `select` with a `FOR UPDATE` row lock.
pub(crate) fn select_for_update<D: Dao>(
    condition: impl SqlCondition<D>,
) -> Result<SelectStatement, SqlError> {
    let query = select::<D>(condition)?.lock(LockType::Update).take();
    Ok(query)
}

/// Turn a select into a claim: `FOR UPDATE OF <table> SKIP LOCKED`, so a row
/// another transaction is already mutating is passed over instead of waited on.
/// `OF <table>` keeps the lock to the DAO's own rows — a correlated subquery's
/// tables are read, not claimed.
pub(crate) fn claim<D: Dao>(query: &mut SelectStatement) -> &mut SelectStatement {
    query.lock_with_tables_behavior(
        LockType::Update,
        [D::table().into_table_ref()],
        LockBehavior::SkipLocked,
    )
}

/// The DAO's default ordering with each column's direction flipped.
fn reverse_order<D: Dao>() -> impl Iterator<Item = (D::Iden, sea_query::Order)> {
    D::order_by().into_iter().map(|(col, ord)| {
        let flipped = match ord {
            sea_query::Order::Asc => sea_query::Order::Desc,
            sea_query::Order::Desc => sea_query::Order::Asc,
            other => other,
        };
        (col, flipped)
    })
}

pub(crate) fn update_query<U: DaoUpdate, D: DaoCore>(
    dao: &U,
    condition: impl SqlCondition<D>,
) -> Result<sea_query::UpdateStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    let query = Query::update()
        .table(U::table())
        .values(dao.update_values())
        .cond_where(condition.resolve())
        .take();
    Ok(query)
}

pub(crate) fn delete_query<D: DaoCore>(
    condition: impl SqlCondition<D>,
) -> Result<sea_query::DeleteStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    let query = Query::delete()
        .from_table(D::table())
        .cond_where(condition.resolve())
        .take();
    Ok(query)
}

pub(crate) fn select_in<D: LiveDao>(
    state: &StateFilter<D>,
    condition: impl SqlCondition<D>,
) -> Result<SelectStatement, SqlError> {
    let query = select::<D>(condition)?
        .cond_where(state.condition().clone())
        .take();
    Ok(query)
}

pub(crate) fn select_latest_before<D: LiveDao>(
    state: &StateFilter<D>,
    condition: impl SqlCondition<D>,
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
    use sea_query::PostgresQueryBuilder;
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestName(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "test_basic")]
    struct TestDao {
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "test_keyed", key(id))]
    struct TestKeyedDao {
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[test]
    fn test_select_with_condition() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select::<TestDao>(&id)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_no_condition() {
        let sql = select::<TestDao>(())
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE TRUE ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_insert_query_sql() {
        let dao = TestDao {
            id: TestId::try_from("i1").unwrap(),
            name: TestName::try_from("n1").unwrap(),
            status: TestStatus::Active,
        };
        let sql = insert_query::<TestDao>(&dao)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"INSERT INTO "test_basic" ("id", "name", "status") VALUES ('i1', 'n1', 'A')"#
        );
    }

    fn keyed_dao() -> TestKeyedDao {
        TestKeyedDao {
            id: TestId::try_from("i1").unwrap(),
            name: TestName::try_from("n1").unwrap(),
            status: TestStatus::Active,
        }
    }

    #[test]
    fn test_upsert_query_sql() {
        let sql = upsert_query::<TestKeyedDao>(&keyed_dao())
            .unwrap()
            .to_string(PostgresQueryBuilder);
        // conflict target = the declared key; replaced = every other column.
        assert_eq!(
            sql,
            r#"INSERT INTO "test_keyed" ("id", "name", "status") VALUES ('i1', 'n1', 'A') ON CONFLICT ("id") DO UPDATE SET "name" = "excluded"."name", "status" = "excluded"."status""#
        );
    }

    #[test]
    fn test_insert_or_ignore_query_sql() {
        let sql = insert_or_ignore_query::<TestKeyedDao>(&keyed_dao())
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"INSERT INTO "test_keyed" ("id", "name", "status") VALUES ('i1', 'n1', 'A') ON CONFLICT ("id") DO NOTHING"#
        );
    }

    #[test]
    fn test_select_last_reverses_order() {
        let id = TestId::try_from("abc").unwrap();
        // TestDao's default order is `id DESC` (no explicit order_by); the
        // last row reverses it to `id ASC`.
        let sql = select_last::<TestDao>(&id)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" ASC LIMIT 1"#
        );
    }

    #[test]
    fn test_select_for_update_emits_lock() {
        use sea_query::PostgresQueryBuilder;
        let id = TestId::try_from("abc").unwrap();
        let sql = select_for_update::<TestDao>(&id)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert!(sql.contains("FOR UPDATE"), "{sql}");
    }

    /// A claim locks only its own rows and skips ones another claimant holds.
    #[test]
    fn test_claim_emits_skip_locked() {
        use sea_query::PostgresQueryBuilder;
        let state = StateFilter::<TestDao>::of(TestStatus::Active);
        let mut query = select_in::<TestDao>(&state, &()).unwrap();
        let sql = claim::<TestDao>(&mut query).to_string(PostgresQueryBuilder);
        assert!(
            sql.contains(r#"FOR UPDATE OF "test_basic" SKIP LOCKED"#),
            "{sql}"
        );
    }
}
