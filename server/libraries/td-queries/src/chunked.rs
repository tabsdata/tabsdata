//
// Copyright 2026 Tabs Data Inc.
//

//! Chunked query execution for slices that may exceed database bind-parameter
//! limits.
//!
//! Each element in the slice is resolved via [`SqlCondition`] and combined
//! with OR. The slice is split into chunks of at most [`MAX_QUERY_CONDITIONS`]
//! elements, and one query is executed per chunk.

use crate::versioned::{Cte, latest_ranked_cte};
use sea_query::{Expr, ExprTrait, Query};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{CurrentDao, Dao, DaoCore, DaoUpdate, States, VersionedDao};
use td_database::dynamic::DynamicQueryResult;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use tracing::warn;

/// Maximum conditions per query.
///
/// Conservatively set well below database limits (SQLite: 32,766;
/// PostgreSQL: 65,535) to leave room for other parameters in the query
/// (pagination, ordering, additional WHERE clauses).
/// sea_query::Condition can cause stack overflow when nesting (this is, multiple OR conditions).
/// With 1000 OR conditions, this can already happen. So, queries should never reach this limit,
/// this is just a failsafe.
pub const MAX_QUERY_CONDITIONS: usize = 100;

/// Maximum rows per batch INSERT statement.
///
/// Capped so that `rows * columns` stays well below database bind-parameter
/// limits (SQLite: 32,766; PostgreSQL: 65,535).
const MAX_INSERT_ROWS: usize = 100;

fn resolve_or<D: DaoCore>(items: &[impl SqlCondition]) -> Result<sea_query::Condition, SqlError> {
    let mut cond = sea_query::Condition::any();
    for item in items {
        cond = cond.add(item.resolve::<D>()?);
    }
    Ok(cond)
}

/// Emit a warning when a chunked query exceeds MAX_QUERY_PARAMS. This is a
/// failsafe — callers should use JOINs or scoped queries instead of passing
/// unbounded slices.
fn warn_if_chunked<D: DaoCore>(len: usize) {
    if len > MAX_QUERY_CONDITIONS {
        let chunks = len.div_ceil(MAX_QUERY_CONDITIONS);
        warn!(
            entity = D::display_name(),
            values = len,
            max = MAX_QUERY_CONDITIONS,
            chunks,
            "Chunked query exceeded MAX_QUERY_PARAMS",
        );
    }
}

pub(crate) fn insert_vec_query<D: Dao>(daos: &[D]) -> Result<sea_query::InsertStatement, SqlError> {
    let mut query = Query::insert();
    query.into_table(D::table()).columns(D::columns());
    for dao in daos {
        query.values(dao.insert_values())?;
    }
    Ok(query)
}

pub(crate) fn fetch_all_chunk_query<D: Dao, V: SqlCondition>(
    chunk: &[V],
) -> Result<sea_query::SelectStatement, SqlError> {
    Ok(Query::select()
        .from(D::table())
        .columns(D::columns())
        .cond_where(resolve_or::<D>(chunk)?)
        .order_by_columns(D::order_by())
        .take())
}

pub(crate) fn update_chunk_query<U: DaoUpdate, D: DaoCore, V: SqlCondition>(
    dao: &U,
    chunk: &[V],
) -> Result<sea_query::UpdateStatement, SqlError> {
    Ok(Query::update()
        .table(U::table())
        .values(dao.update_values())
        .cond_where(resolve_or::<D>(chunk)?)
        .take())
}

pub(crate) fn delete_chunk_query<D: DaoCore, V: SqlCondition>(
    chunk: &[V],
) -> Result<sea_query::DeleteStatement, SqlError> {
    Ok(Query::delete()
        .from_table(D::table())
        .cond_where(resolve_or::<D>(chunk)?)
        .take())
}

pub(crate) fn fetch_all_at_chunk_query<
    const S: u8,
    D: VersionedDao + States<S>,
    V: SqlCondition,
>(
    chunk: &[V],
    on: &D::VersionOrder,
) -> Result<sea_query::WithQuery, SqlError> {
    let state_cond = resolve_condition::<D>(D::state())?;
    Ok(Query::select()
        .columns(D::columns())
        .from(Cte::Ranked)
        .and_where(Expr::col(Cte::Rn).eq(1))
        .cond_where(resolve_or::<D>(chunk)?)
        .cond_where(state_cond)
        .order_by_columns(D::order_by())
        .take()
        .with(latest_ranked_cte::<D>(on)?))
}

pub(crate) fn fetch_all_current_chunk_query<
    const S: u8,
    D: CurrentDao + States<S>,
    V: SqlCondition,
>(
    chunk: &[V],
) -> Result<sea_query::SelectStatement, SqlError> {
    let state_cond = resolve_condition::<D>(D::state())?;
    Ok(Query::select()
        .columns(D::columns())
        .from(D::current_table_name())
        .cond_where(resolve_or::<D>(chunk)?)
        .cond_where(state_cond)
        .take())
}

pub trait ChunkedQueries {
    /// Batch-insert multiple DAOs using multi-row INSERT statements.
    ///
    /// Rows are chunked to stay within database bind-parameter limits.
    /// Each chunk produces a single `INSERT INTO ... VALUES (...), (...), ...`.
    fn insert_vec<D: Dao>(
        self,
        daos: &[D],
    ) -> impl Future<Output = Result<DynamicQueryResult, SqlError>>;

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
    async fn insert_vec<D: Dao>(mut self, daos: &[D]) -> Result<DynamicQueryResult, SqlError> {
        let mut result = DynamicQueryResult::empty(&self.kind());
        for chunk in daos.chunks(MAX_INSERT_ROWS) {
            let r = insert_vec_query::<D>(chunk)?
                .build_query()
                .execute(self.reuse())
                .await
                .map_err(|e| SqlError::on_entity(D::display_name(), e))?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    async fn fetch_all_chunked<D: Dao, V: SqlCondition>(
        mut self,
        values: &[V],
    ) -> Result<Vec<D>, SqlError> {
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
    ) -> Result<DynamicQueryResult, SqlError> {
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
    ) -> Result<DynamicQueryResult, SqlError> {
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

    async fn fetch_all_at_chunked<const S: u8, D: VersionedDao + States<S>, V: SqlCondition>(
        mut self,
        values: &[V],
        on: &D::VersionOrder,
    ) -> Result<Vec<D>, SqlError> {
        warn_if_chunked::<D>(values.len());
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = fetch_all_at_chunk_query::<S, D, V>(chunk, on)?
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
        warn_if_chunked::<D>(values.len());
        let mut results = Vec::new();
        for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
            let r: Vec<D> = fetch_all_current_chunk_query::<S, D, V>(chunk)?
                .build_query_as()
                .fetch_all(self.reuse())
                .await
                .map_err(SqlError::on_read::<D>)?;
            results.extend(r);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Alias, Asterisk, SqliteQueryBuilder};
    use ta_objects::Buildable;

    // --- Basic test DAO ---

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
    #[dao(sql_table = "test_chunked", states(Active = TestStatus::Active))]
    struct TestDao {
        id: TestId,
        name: TestName,
        status: TestStatus,
    }

    // --- Versioned test DAO ---

    #[ta_objects::typed(string)]
    struct TestPartition;

    #[ta_objects::typed(i64)]
    struct TestDefinedOn;

    #[ta_objects::typed_enum]
    enum TestVStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(
        sql_table = "test_chunked_versioned",
        versioned(pk = "id", version_by = "defined_on", partition_by = "partition_id"),
        states(Active = TestVStatus::Active)
    )]
    struct TestVersionedDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestVStatus,
        defined_on: TestDefinedOn,
    }

    // --- Current test DAO ---

    #[ta_objects::Dao]
    #[dao(
        sql_table = "test_chunked_current_table",
        versioned(
            pk = "id",
            version_by = "defined_on",
            partition_by = "partition_id",
            current = "test_chunked_current_view"
        ),
        states(Active = TestVStatus::Active)
    )]
    struct TestCurrentDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestVStatus,
        defined_on: TestDefinedOn,
    }

    #[test]
    fn test_insert_vec_query() {
        let dao1 = TestDao::builder()
            .id(TestId::try_from("i1").unwrap())
            .name(TestName::try_from("n1").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let dao2 = TestDao::builder()
            .id(TestId::try_from("i2").unwrap())
            .name(TestName::try_from("n2").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let sql = insert_vec_query::<TestDao>(&[dao1, dao2])
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert_eq!(
            sql,
            r#"INSERT INTO "test_chunked" ("id", "name", "status") VALUES ('i1', 'n1', 'A'), ('i2', 'n2', 'A')"#
        );
    }

    #[test]
    fn test_resolve_or() {
        let ids = vec![
            TestId::try_from("a").unwrap(),
            TestId::try_from("b").unwrap(),
        ];
        let cond = resolve_or::<TestDao>(&ids).unwrap();
        let sql = Query::select()
            .column(Asterisk)
            .from(Alias::new("t"))
            .cond_where(cond)
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains("'a'") && sql.contains("'b'") && sql.contains("OR"),
            "should combine with OR: {sql}"
        );
    }

    #[test]
    fn test_fetch_all_chunk_query() {
        let ids = vec![
            TestId::try_from("x").unwrap(),
            TestId::try_from("y").unwrap(),
        ];
        let sql = fetch_all_chunk_query::<TestDao, TestId>(&ids)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_chunked""#),
            "should select from table: {sql}"
        );
        assert!(
            sql.contains("'x'") && sql.contains("'y'"),
            "should include both values: {sql}"
        );
        assert!(sql.contains("ORDER BY"), "should order: {sql}");
    }

    #[test]
    fn test_update_chunk_query() {
        let dao = TestDao::builder()
            .id(TestId::try_from("i1").unwrap())
            .name(TestName::try_from("updated").unwrap())
            .status(TestStatus::Active)
            .build()
            .unwrap();
        let ids = vec![
            TestId::try_from("i1").unwrap(),
            TestId::try_from("i2").unwrap(),
        ];
        let sql = update_chunk_query::<TestDao, TestDao, TestId>(&dao, &ids)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.starts_with(r#"UPDATE "test_chunked" SET"#),
            "should update table: {sql}"
        );
        assert!(
            sql.contains("'i1'") && sql.contains("'i2'"),
            "should include both condition values: {sql}"
        );
    }

    #[test]
    fn test_delete_chunk_query() {
        let ids = vec![
            TestId::try_from("d1").unwrap(),
            TestId::try_from("d2").unwrap(),
        ];
        let sql = delete_chunk_query::<TestDao, TestId>(&ids)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.starts_with(r#"DELETE FROM "test_chunked""#),
            "should delete from table: {sql}"
        );
        assert!(
            sql.contains("'d1'") && sql.contains("'d2'"),
            "should include both condition values: {sql}"
        );
    }

    #[test]
    fn test_fetch_all_at_chunk_query() {
        let ids = vec![
            TestPartition::try_from("p1").unwrap(),
            TestPartition::try_from("p2").unwrap(),
        ];
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql =
            fetch_all_at_chunk_query::<{ TestVersionedDao::All }, TestVersionedDao, TestPartition>(
                &ids, &on,
            )
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(sql.starts_with("WITH"), "should be a WITH query: {sql}");
        assert!(sql.contains("ROW_NUMBER"), "should use ROW_NUMBER: {sql}");
        assert!(
            sql.contains("'p1'") && sql.contains("'p2'"),
            "should include both partition values: {sql}"
        );
        assert!(sql.contains(r#""rn" = 1"#), "should filter rn = 1: {sql}");
    }

    #[test]
    fn test_fetch_all_current_chunk_query() {
        let ids = vec![
            TestPartition::try_from("p1").unwrap(),
            TestPartition::try_from("p2").unwrap(),
        ];
        let sql = fetch_all_current_chunk_query::<
            { TestCurrentDao::All },
            TestCurrentDao,
            TestPartition,
        >(&ids)
        .unwrap()
        .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_chunked_current_view""#),
            "should select from current view: {sql}"
        );
        assert!(
            sql.contains("'p1'") && sql.contains("'p2'"),
            "should include both values: {sql}"
        );
    }

    #[test]
    fn test_fetch_all_current_chunk_query_active_state() {
        let ids = vec![TestPartition::try_from("p1").unwrap()];
        let sql = fetch_all_current_chunk_query::<
            { TestCurrentDao::Active },
            TestCurrentDao,
            TestPartition,
        >(&ids)
        .unwrap()
        .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#""status" = 'A'"#),
            "should filter by active state: {sql}"
        );
    }
}
