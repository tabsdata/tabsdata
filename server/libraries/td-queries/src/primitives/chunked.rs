//
// Copyright 2026 Tabs Data Inc.
//

//! Helpers and per-chunk SQL builders for the `ChunkedQueries` trait
//! impl in [`crate::generic::chunked`].
//!
//! Each `*_chunk_query` returns a single statement covering one chunk
//! of bind values; the trait impl loops over chunks of size
//! [`MAX_QUERY_CONDITIONS`].

use crate::primitives::versioned::{Cte, latest_ranked_cte};
use sea_query::{Expr, ExprTrait, Query};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{CurrentDao, Dao, DaoCore, DaoUpdate, VersionedDao};
use tracing::warn;

/// Maximum conditions per query.
///
/// Conservatively set well below database limits (SQLite: 32,766;
/// PostgreSQL: 65,535) to leave room for other parameters in the query
/// (pagination, ordering, additional WHERE clauses).
/// `sea_query::Condition` can cause stack overflow when nesting (this is,
/// multiple OR conditions). With 1000 OR conditions, this can already
/// happen. So, queries should never reach this limit; this is just a
/// failsafe.
pub(crate) const MAX_QUERY_CONDITIONS: usize = 100;

/// Maximum rows per batch INSERT statement.
///
/// Capped so that `rows * columns` stays well below database bind-parameter
/// limits (SQLite: 32,766; PostgreSQL: 65,535).
pub(crate) const MAX_INSERT_ROWS: usize = 100;

pub(crate) fn resolve_or<D: DaoCore>(
    items: &[impl SqlCondition],
) -> Result<sea_query::Condition, SqlError> {
    let mut cond = sea_query::Condition::any();
    for item in items {
        cond = cond.add(item.resolve::<D>()?);
    }
    Ok(cond)
}

/// Emit a warning when a chunked query exceeds [`MAX_QUERY_CONDITIONS`].
/// This is a failsafe — callers should use JOINs or scoped queries
/// instead of passing unbounded slices.
pub(crate) fn warn_if_chunked<D: DaoCore>(len: usize) {
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

pub(crate) fn fetch_all_at_chunk_query<D: VersionedDao, V: SqlCondition>(
    chunk: &[V],
    on: &D::VersionOrder,
    state_cond: Option<&sea_query::Condition>,
) -> Result<sea_query::WithQuery, SqlError> {
    let mut q = Query::select()
        .columns(D::columns())
        .from(Cte::Ranked)
        .and_where(Expr::col(Cte::Rn).eq(1_i64))
        .cond_where(resolve_or::<D>(chunk)?)
        .order_by_columns(D::order_by())
        .take();
    if let Some(s) = state_cond {
        q.cond_where(s.clone());
    }
    Ok(q.with(latest_ranked_cte::<D>(on)?))
}

pub(crate) fn fetch_all_current_chunk_query<D: CurrentDao, V: SqlCondition>(
    chunk: &[V],
    state_cond: Option<&sea_query::Condition>,
) -> Result<sea_query::SelectStatement, SqlError> {
    let mut q = Query::select()
        .columns(D::columns())
        .from(D::current_table_name())
        .cond_where(resolve_or::<D>(chunk)?)
        .take();
    if let Some(s) = state_cond {
        q.cond_where(s.clone());
    }
    Ok(q)
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
        let sql = fetch_all_at_chunk_query::<TestVersionedDao, TestPartition>(&ids, &on, None)
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
        let sql = fetch_all_current_chunk_query::<TestCurrentDao, TestPartition>(&ids, None)
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
        let active = TestCurrentDao::active();
        let sql = fetch_all_current_chunk_query::<TestCurrentDao, TestPartition>(
            &ids,
            Some(active.condition()),
        )
        .unwrap()
        .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#""status" = 'A'"#),
            "should filter by active state: {sql}"
        );
    }
}
