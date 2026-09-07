//
// Copyright 2026 Tabsdata Inc.
//

//! Builders over a slice of conditions, OR-joined per chunk so a caller's
//! slice may exceed the bind-parameter limit.

use crate::QueryExecutor;
use crate::primitives::scd2::{close_query, open_interval_cond, visible_at_cond};
use sea_query::{Condition, InsertStatement, Query, SelectStatement, UpdateStatement};
use sqlx::FromRow;
use sqlx::postgres::PgRow;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Dao, InsertDao, Scd2Dao, SelectDao, TableDao, UpdateDao};
use td_database::sea_query::builder::SeaQueryExt;
use tracing::warn;

pub(crate) const MAX_QUERY_CONDITIONS: usize = 100;

pub(crate) const MAX_INSERT_ROWS: usize = 100;

pub(crate) fn resolve_or<D: Dao>(items: &[impl SqlCondition<D>]) -> Condition {
    items
        .iter()
        .fold(Condition::any(), |cond, item| cond.add(item.resolve()))
}

pub(crate) fn warn_if_chunked<D: Dao>(len: usize) {
    if len > MAX_QUERY_CONDITIONS {
        warn!(
            entity = D::display_name(),
            values = len,
            max = MAX_QUERY_CONDITIONS,
            chunks = len.div_ceil(MAX_QUERY_CONDITIONS),
            "chunked query exceeded the per-statement condition limit",
        );
    }
}

/// One statement per chunk of `values`; `D` names the entity in errors and
/// the chunking warning, `O` is the decoded row.
pub(crate) async fn fetch_chunked<D, O, E, C>(
    mut executor: E,
    values: &[C],
    statement: impl Fn(&[C]) -> SelectStatement,
) -> Result<Vec<O>, SqlError>
where
    D: Dao,
    O: Send + Unpin + for<'r> FromRow<'r, PgRow>,
    E: QueryExecutor,
{
    warn_if_chunked::<D>(values.len());
    let mut results = Vec::new();
    for chunk in values.chunks(MAX_QUERY_CONDITIONS) {
        let rows: Vec<O> = statement(chunk)
            .build_query_as()
            .fetch_all(executor.reuse())
            .await
            .map_err(SqlError::on_read::<D>)?;
        results.extend(rows);
    }
    Ok(results)
}

pub(crate) fn insert_vec_query<D: InsertDao>(daos: &[D]) -> Result<InsertStatement, SqlError> {
    let mut query = Query::insert();
    query.into_table(D::TABLE).columns(D::columns());
    for dao in daos {
        query.values(dao.insert_values())?;
    }
    Ok(query)
}

pub(crate) fn close_chunk_query<D: TableDao + Scd2Dao>(
    chunk: &[impl SqlCondition<D>],
    extra: &Condition,
    at: &D::VersionOrder,
) -> Result<UpdateStatement, SqlError> {
    close_query::<D>(
        ChunkCondition {
            chunk: resolve_or::<D>(chunk),
            extra: extra.clone(),
        },
        at,
    )
}

struct ChunkCondition {
    chunk: Condition,
    extra: Condition,
}

impl<D: Dao> SqlCondition<D> for ChunkCondition {
    fn resolve(&self) -> Condition {
        Condition::all()
            .add(self.chunk.clone())
            .add(self.extra.clone())
    }
}

pub(crate) fn update_chunk_query<U: UpdateDao>(
    dao: &U,
    chunk: &[impl SqlCondition<U::FilterBy>],
) -> UpdateStatement {
    Query::update()
        .table(U::TABLE)
        .values(dao.update_values())
        .cond_where(resolve_or::<U::FilterBy>(chunk))
        .take()
}

fn select_chunk<D: SelectDao>(chunk: &[impl SqlCondition<D>], extra: Condition) -> SelectStatement {
    Query::select()
        .columns(D::columns())
        .from(D::TABLE)
        .cond_where(resolve_or::<D>(chunk))
        .cond_where(extra)
        .order_by_columns(D::order_by())
        .take()
}

pub(crate) fn fetch_all_chunk_query<D: SelectDao>(
    chunk: &[impl SqlCondition<D>],
    extra: &Condition,
) -> SelectStatement {
    select_chunk::<D>(chunk, extra.clone())
}

pub(crate) fn fetch_all_at_chunk_query<D: SelectDao + Scd2Dao>(
    chunk: &[impl SqlCondition<D>],
    on: &D::VersionOrder,
    extra: &Condition,
) -> SelectStatement {
    select_chunk::<D>(chunk, visible_at_cond::<D>(on).add(extra.clone()))
}

pub(crate) fn fetch_all_current_chunk_query<D: SelectDao + Scd2Dao>(
    chunk: &[impl SqlCondition<D>],
    extra: &Condition,
) -> SelectStatement {
    select_chunk::<D>(
        chunk,
        Condition::all()
            .add(open_interval_cond::<D>())
            .add(extra.clone()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Alias, Asterisk, PostgresQueryBuilder};
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
    #[dao(table = "test_chunked", ops(select, insert, delete))]
    struct TestDao {
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestDefinedOn(i64);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestVStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[ta_objects::Dao]
    #[dao(table = "test_chunked_versioned", ops(select, insert), scd2)]
    struct TestScd2Dao {
        #[dao(pk)]
        pub id: TestId,
        #[dao(partition_by)]
        pub partition_id: TestPartition,
        pub status: TestVStatus,
        #[dao(valid_from)]
        pub defined_on: TestDefinedOn,
    }

    #[ta_objects::Dao]
    #[dao(table = "test_chunked_current_table", ops(select, insert), scd2)]
    struct TestCurrentDao {
        #[dao(pk)]
        pub id: TestId,
        #[dao(partition_by)]
        pub partition_id: TestPartition,
        pub status: TestVStatus,
        #[dao(valid_from)]
        pub defined_on: TestDefinedOn,
    }

    #[test]
    fn test_insert_vec_query() {
        let dao1 = TestDao {
            id: TestId::try_from("i1").unwrap(),
            name: TestName::try_from("n1").unwrap(),
            status: TestStatus::Active,
        };
        let dao2 = TestDao {
            id: TestId::try_from("i2").unwrap(),
            name: TestName::try_from("n2").unwrap(),
            status: TestStatus::Active,
        };
        let sql = insert_vec_query::<TestDao>(&[dao1, dao2])
            .unwrap()
            .to_string(PostgresQueryBuilder);
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
        let cond = resolve_or::<TestDao>(&ids);
        let sql = Query::select()
            .column(Asterisk)
            .from(Alias::new("t"))
            .cond_where(cond)
            .to_string(PostgresQueryBuilder);
        assert_eq!(sql, r#"SELECT * FROM "t" WHERE "id" = 'a' OR "id" = 'b'"#);
    }

    #[test]
    fn test_fetch_all_chunk_query() {
        let ids = vec![
            TestId::try_from("x").unwrap(),
            TestId::try_from("y").unwrap(),
        ];
        let sql = fetch_all_chunk_query::<TestDao>(&ids, &Condition::all())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_chunked" WHERE ("id" = 'x' OR "id" = 'y') AND TRUE ORDER BY "id" DESC"#
        );
    }

    #[ta_objects::Dao]
    #[dao(table = "test_chunked", ops(update(TestDao)))]
    struct TestUpdateDao {
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[test]
    fn test_update_chunk_query() {
        let dao = TestUpdateDao {
            id: TestId::try_from("i1").unwrap(),
            name: TestName::try_from("updated").unwrap(),
            status: TestStatus::Active,
        };
        let ids = vec![
            TestId::try_from("i1").unwrap(),
            TestId::try_from("i2").unwrap(),
        ];
        let sql = update_chunk_query::<TestUpdateDao>(&dao, &ids).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"UPDATE "test_chunked" SET "id" = 'i1', "name" = 'updated', "status" = 'A' WHERE "id" = 'i1' OR "id" = 'i2'"#
        );
    }

    #[test]
    fn test_fetch_all_at_chunk_query() {
        let ids = vec![
            TestPartition::try_from("p1").unwrap(),
            TestPartition::try_from("p2").unwrap(),
        ];
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = fetch_all_at_chunk_query::<TestScd2Dao>(&ids, &on, &Condition::all())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_chunked_versioned" WHERE ("partition_id" = 'p1' OR "partition_id" = 'p2') AND ("defined_on" <= 100 AND "valid_to" > 100 AND TRUE) ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_fetch_all_current_chunk_query() {
        let ids = vec![
            TestPartition::try_from("p1").unwrap(),
            TestPartition::try_from("p2").unwrap(),
        ];
        let sql = fetch_all_current_chunk_query::<TestCurrentDao>(&ids, &Condition::all())
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_chunked_current_table" WHERE ("partition_id" = 'p1' OR "partition_id" = 'p2') AND ("valid_to" = ('infinity'::timestamptz) AND TRUE) ORDER BY "id" DESC"#
        );
    }
}
