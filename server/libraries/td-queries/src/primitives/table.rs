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

/// No ordering: the base a listing extends, since `ListQueryParams::apply_to`
/// only appends `ORDER BY`, and a DAO default order left in front of the
/// caller's would outrank the keyset predicate's column.
pub(crate) fn select_unordered<D: SelectDao>(condition: impl SqlCondition<D>) -> SelectStatement {
    Query::select()
        .from(D::TABLE)
        .columns(D::columns())
        .cond_where(condition.resolve())
        .take()
}

pub(crate) fn select<D: SelectDao>(condition: impl SqlCondition<D>) -> SelectStatement {
    select_unordered::<D>(condition)
        .order_by_columns(D::order_by())
        .take()
}

pub(crate) fn select_last<D: SelectDao>(condition: impl SqlCondition<D>) -> SelectStatement {
    select_unordered::<D>(condition)
        .order_by_columns(reverse_order::<D>())
        .limit(1)
        .take()
}

pub(crate) fn select_nth_last<D: SelectDao>(
    condition: impl SqlCondition<D>,
    offset: u64,
) -> SelectStatement {
    let mut query = select::<D>(condition).limit(1).take();
    if offset > 0 {
        query.offset(offset);
    }
    query
}

pub(crate) fn select_for_update<D: SelectDao>(condition: impl SqlCondition<D>) -> SelectStatement {
    select::<D>(condition)
        .lock_with_tables(LockType::Update, [D::TABLE.into_table_ref()])
        .take()
}

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

pub(crate) fn select_claim<D: SelectDao>(condition: impl SqlCondition<D>) -> SelectStatement {
    claim::<D>(&mut select::<D>(condition)).take()
}

fn reverse_order<D: SelectDao>() -> impl Iterator<Item = (D::Iden, sea_query::Order)> {
    D::order_by().into_iter().map(|(col, ord)| {
        let flipped = match ord {
            sea_query::Order::Asc => sea_query::Order::Desc,
            sea_query::Order::Desc => sea_query::Order::Asc,
            other => other,
        };
        (col, flipped)
    })
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
        pub id: TestId,
        pub name: TestName,
        pub status: TestStatus,
    }

    #[test]
    fn test_select_with_condition() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select::<TestDao>(&id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC"#
        );
    }

    #[test]
    fn test_select_no_condition() {
        let sql = select::<TestDao>(()).to_string(PostgresQueryBuilder);
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
        let sql = select_last::<TestDao>(&id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" ASC LIMIT 1"#
        );
    }

    #[test]
    fn test_select_for_update_emits_lock() {
        let id = TestId::try_from("abc").unwrap();
        let sql = select_for_update::<TestDao>(&id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "id" = 'abc' ORDER BY "id" DESC FOR UPDATE OF "test_basic""#
        );
    }

    #[test]
    fn test_claim_emits_skip_locked() {
        let mut query = select::<TestDao>(TestStatus::Active);
        let sql = claim::<TestDao>(&mut query).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "status" FROM "test_basic" WHERE "status" = 'A' ORDER BY "id" DESC FOR UPDATE OF "test_basic" SKIP LOCKED"#
        );
    }
}
