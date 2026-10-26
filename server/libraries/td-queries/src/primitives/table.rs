//
// Copyright 2026 Tabsdata Inc.
//

//! Select/insert/update/delete builders for an unversioned read of a
//! relation's own rows, with no time predicate.

use sea_query::{
    DeleteStatement, InsertStatement, IntoTableRef, LockBehavior, LockType, OnConflict, Query,
    SelectStatement, UpdateStatement,
};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Insertable, SelectDao, TableDao, UpdateDao, UpsertDao};

/// `FOR UPDATE OF <table> SKIP LOCKED`: a row another transaction is already
/// mutating is passed over instead of waited on; `OF` keeps the lock to the
/// DAO's own rows, so a joined table is read, not claimed.
pub(crate) fn claim<D: SelectDao>(query: &mut SelectStatement) -> &mut SelectStatement {
    query.lock_with_tables_behavior(
        LockType::Update,
        [D::TABLE.into_table_ref()],
        LockBehavior::SkipLocked,
    )
}

pub(crate) fn insert_query<D: Insertable>(dao: &D) -> Result<InsertStatement, SqlError> {
    let mut query = Query::insert();
    query
        .into_table(D::TABLE)
        .columns(D::columns())
        .values(dao.insert_values())?;
    Ok(query)
}

pub(crate) fn upsert_query<D: UpsertDao>(dao: &D) -> Result<InsertStatement, SqlError> {
    let mut on_conflict = OnConflict::columns(D::key_columns());
    on_conflict.update_columns(D::value_columns());
    let mut query = insert_query::<D>(dao)?;
    query.on_conflict(on_conflict);
    Ok(query)
}

pub(crate) fn insert_or_ignore_query<D: UpsertDao>(dao: &D) -> Result<InsertStatement, SqlError> {
    let mut on_conflict = OnConflict::columns(D::key_columns());
    on_conflict.do_nothing();
    let mut query = insert_query::<D>(dao)?;
    query.on_conflict(on_conflict);
    Ok(query)
}

pub(crate) fn update_query<U: UpdateDao>(
    dao: &U,
    condition: impl SqlCondition<U::FilterBy>,
) -> Result<UpdateStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            U::display_name().to_string(),
        ));
    }
    Ok(Query::update()
        .table(U::TABLE)
        .values(dao.update_values())
        .cond_where(condition.resolve())
        .take())
}

pub(crate) fn delete_query<D: TableDao>(
    condition: impl SqlCondition<D>,
) -> Result<DeleteStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    Ok(Query::delete()
        .from_table(D::TABLE)
        .cond_where(condition.resolve())
        .take())
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
    #[dao(table = "test_basic", ops(select, insert, delete))]
    struct TestDao {
        #[dao(pk)]
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[ta_objects::Dao]
    #[dao(table = "test_keyed", ops(select, insert, delete, upsert(TestId)))]
    struct TestKeyedDao {
        #[dao(pk)]
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
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
}
