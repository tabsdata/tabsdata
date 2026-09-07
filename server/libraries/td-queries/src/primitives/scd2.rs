//
// Copyright 2026 Tabsdata Inc.
//

//! Select builders for versioned tables. A row of an [`Scd2Dao`] is visible
//! at `T` iff `version_order <= T AND valid_to > T`, a linear interval
//! predicate the caller's condition composes with, so the planner drives off
//! the condition's own indexes.

use sea_query::{
    Condition, Expr, ExprTrait, IntoColumnRef, Query, SelectStatement, UpdateStatement,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Scd2Dao, SelectDao, TableDao, TimelineDao, UpdateDao};

/// The `'infinity'` literal, never a bind parameter, is what matches the
/// `WHERE valid_to = 'infinity'` partial indexes.
pub(crate) fn open_interval_at(valid_to: impl IntoColumnRef) -> Expr {
    Expr::col(valid_to).eq(Expr::cust("'infinity'::timestamptz"))
}

pub(crate) fn open_interval_cond<D: Scd2Dao>() -> Expr {
    open_interval_at(D::VALID_TO)
}

pub(crate) fn visible_at<V: SeaValue>(
    version_order: impl IntoColumnRef,
    valid_to: impl IntoColumnRef,
    on: &V,
) -> Condition {
    Condition::all()
        .add(Expr::col(version_order).lte(on.as_sea_value()))
        .add(Expr::col(valid_to).gt(on.as_sea_value()))
}

pub(crate) fn visible_at_cond<D: Scd2Dao>(on: &D::VersionOrder) -> Condition {
    visible_at(D::VERSION_ORDER, D::VALID_TO, on)
}

fn select_where<D: SelectDao>(
    condition: impl SqlCondition<D>,
    extra: Condition,
) -> SelectStatement {
    Query::select()
        .columns(D::columns())
        .from(D::TABLE)
        .cond_where(Condition::all().add(condition.resolve()).add(extra))
        .take()
}

/// Every version at or before `on`, deliberately without a validity filter.
pub(crate) fn select_until<D: SelectDao + TimelineDao>(
    condition: impl SqlCondition<D>,
    on: &D::VersionOrder,
) -> SelectStatement {
    select_where::<D>(
        condition,
        Condition::all().add(Expr::col(D::VERSION_ORDER).lte(on.as_sea_value())),
    )
}

pub(crate) fn select_visible_at<D: SelectDao + Scd2Dao>(
    condition: impl SqlCondition<D>,
    on: &D::VersionOrder,
) -> SelectStatement {
    select_where::<D>(condition, visible_at_cond::<D>(on))
}

pub(crate) fn select_open<D: SelectDao + Scd2Dao>(
    condition: impl SqlCondition<D>,
) -> SelectStatement {
    select_where::<D>(condition, Condition::all().add(open_interval_cond::<D>()))
}

/// A payload update of the matching open rows; closed versions stay as written.
pub(crate) fn update_open_query<U: UpdateDao>(
    dao: &U,
    condition: impl SqlCondition<U::FilterBy>,
) -> Result<UpdateStatement, SqlError>
where
    U::FilterBy: Scd2Dao,
{
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            U::display_name().to_string(),
        ));
    }
    Ok(Query::update()
        .table(U::TABLE)
        .values(dao.update_values())
        .cond_where(condition.resolve())
        .cond_where(open_interval_cond::<U::FilterBy>())
        .take())
}

/// Ends the matching open intervals at `at` with no successor: the SCD2
/// deletion. A later insert into the partition reopens it.
pub(crate) fn close_query<D: TableDao + Scd2Dao>(
    condition: impl SqlCondition<D>,
    at: &D::VersionOrder,
) -> Result<UpdateStatement, SqlError> {
    if condition.matches_all() {
        return Err(SqlError::EmptyConditionRefused(
            D::display_name().to_string(),
        ));
    }
    Ok(Query::update()
        .table(D::TABLE)
        .value(D::VALID_TO, at.as_sea_value())
        .cond_where(
            Condition::all()
                .add(condition.resolve())
                .add(open_interval_cond::<D>()),
        )
        .take())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::Dao;
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_objects::and;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestDefinedOn(i64);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestVStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[Dao]
    #[dao(table = "test_scd2", ops(select, insert), scd2)]
    struct TestScd2Dao {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        status: TestVStatus,
        #[dao(scd2(valid_from))]
        defined_on: TestDefinedOn,
    }

    impl TestScd2Dao {
        pub fn active() -> impl SqlCondition<Self> {
            TestVStatus::Active
        }
    }

    #[test]
    fn test_select_at_all() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_until::<TestScd2Dao>(&id, &on).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_scd2" WHERE "partition_id" = 'p1' AND "defined_on" <= 100"#
        );
    }

    #[test]
    fn test_select_at_active() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_until::<TestScd2Dao>(and!(&id, TestScd2Dao::active()), &on)
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_scd2" WHERE "partition_id" = 'p1' AND "status" = 'A' AND "defined_on" <= 100"#
        );
    }

    #[test]
    fn test_select_visible_at() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_visible_at::<TestScd2Dao>(and!(&id, TestScd2Dao::active()), &on)
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_scd2" WHERE "partition_id" = 'p1' AND "status" = 'A' AND ("defined_on" <= 100 AND "valid_to" > 100)"#
        );
    }

    #[test]
    fn test_select_open() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_open::<TestScd2Dao>(and!(&id, TestScd2Dao::active()))
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_scd2" WHERE "partition_id" = 'p1' AND "status" = 'A' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_close_query() {
        let id = TestPartition::try_from("p1").unwrap();
        let at = TestDefinedOn::try_from(100i64).unwrap();
        let sql = close_query::<TestScd2Dao>(&id, &at)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"UPDATE "test_scd2" SET "valid_to" = 100 WHERE "partition_id" = 'p1' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[Dao]
    #[dao(table = "test_scd2", ops(update(TestScd2Dao)))]
    struct TestScd2Update {
        status: Option<TestVStatus>,
    }

    #[test]
    fn test_update_open_query_reaches_open_rows_only() {
        let id = TestPartition::try_from("p1").unwrap();
        let update = TestScd2Update {
            status: Some(TestVStatus::Active),
        };
        let sql = update_open_query(&update, &id)
            .unwrap()
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"UPDATE "test_scd2" SET "status" = 'A' WHERE "partition_id" = 'p1' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_update_open_query_refuses_match_all() {
        let update = TestScd2Update { status: None };
        assert!(matches!(
            update_open_query(&update, ()),
            Err(SqlError::EmptyConditionRefused(_))
        ));
    }

    #[test]
    fn test_close_query_refuses_match_all() {
        let at = TestDefinedOn::try_from(100i64).unwrap();
        assert!(matches!(
            close_query::<TestScd2Dao>((), &at),
            Err(SqlError::EmptyConditionRefused(_))
        ));
    }
}
