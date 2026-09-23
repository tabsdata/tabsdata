//
// Copyright 2026 Tabsdata Inc.
//

use crate::generic::as_of::AsOf;
use crate::generic::chunked::Scd2Chunked;
use crate::primitives::scd2::select_open;
use crate::primitives::scd2::{close_query, open_query, update_open_query};
use crate::{QueryExecutor, Serves, Writes};
use sqlx::postgres::PgQueryResult;
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{
    AddressesOneVersion, Dao, InsertableFrom, ListDto, Scd2Dao, Scd2OpenDao, SelectDao, TableDao,
    UpdateDao, single_row,
};
use td_database::access::LockInstant;
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

/// The only door to an interval table: the open intervals
/// (`valid_to = 'infinity'`, served by the partial indexes) until
/// [`as_of`](Self::as_of) moves the instant.
pub struct Scd2<E> {
    pub(crate) executor: E,
}

impl<E> Scd2<E> {
    /// The caller's condition composed with the visibility interval at `on`,
    /// so cost follows the condition's selectivity.
    #[allow(clippy::wrong_self_convention)]
    pub fn as_of<V>(self, on: &V) -> AsOf<'_, E, V> {
        AsOf {
            on,
            executor: self.executor,
        }
    }

    pub fn chunked(self) -> Scd2Chunked<E> {
        Scd2Chunked {
            executor: self.executor,
        }
    }

    /// A new version open from `at`, the unit of work's lock instant: the
    /// trigger closes the partition's previous open row at the same instant.
    pub async fn open<D: Scd2OpenDao + InsertableFrom<E::Scope>>(
        self,
        dao: &D,
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + Writes,
    {
        open_query::<D>(dao, at)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &dao.column_display_values()))
    }

    /// A payload update of the matching open rows; closed versions are history.
    pub async fn update<U: UpdateDao>(
        self,
        dao: &U,
        condition: impl SqlCondition<U::FilterBy>,
    ) -> Result<PgQueryResult, SqlError>
    where
        U::FilterBy: Scd2Dao,
        E: Serves<U> + Writes,
    {
        update_open_query(dao, condition)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<U>(e, &dao.column_display_values()))
    }

    /// Ends the matching open intervals at `at` with no successor: the SCD2
    /// deletion. A later insert into the partition reopens it.
    pub async fn close<D: TableDao + Scd2Dao>(
        self,
        condition: impl SqlCondition<D>,
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: Serves<D> + Writes,
    {
        let values = condition.display_values();
        close_query::<D>(condition, at)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| SqlError::on_write::<D>(e, &values))
    }

    pub async fn fetch_one<D: SelectDao + Scd2Dao>(
        self,
        condition: impl AddressesOneVersion<D>,
    ) -> Result<D, SqlError>
    where
        E: Serves<D>,
    {
        self.fetch_optional(condition)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    pub async fn fetch_optional<D: SelectDao + Scd2Dao>(
        self,
        condition: impl AddressesOneVersion<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        let rows = select_open::<D>(condition)
            .limit(2)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        single_row::<D>(rows)
    }

    /// The first open row in the DAO's default order, of however many match.
    pub async fn fetch_first<D: SelectDao + Scd2Dao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_open::<D>(condition)
            .order_by_columns(D::order_by())
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn fetch_all<D: SelectDao + Scd2Dao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_open::<D>(condition)
            .order_by_columns(D::order_by())
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn assert_not_exists<D: SelectDao + Scd2Dao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<(), SqlError>
    where
        E: Serves<D>,
    {
        let vals = condition.display_values();
        match self.fetch_first::<D>(condition).await? {
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

    /// The cross-project surface: a project-blind caller narrows by its
    /// viewable projects in `condition`, so this asks only for the domain.
    pub async fn list<T>(
        self,
        condition: impl SqlCondition<T::Dao>,
        params: &ListQueryParams<T>,
    ) -> Result<Vec<T>, SqlError>
    where
        T: ListDto,
        T::Dao: Scd2Dao,
        E: QueryExecutor + Domained<Domain = <T::Dao as Dao>::Db>,
    {
        let daos: Vec<T::Dao> = params
            .apply_to(select_open::<T::Dao>(condition))
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<T::Dao>)?;
        daos.into_iter()
            .map(|dao| T::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::Dao;
    use ta_objects::{IntervalColumns, Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_objects::and;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestValidFrom(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestValidTo(String);

    #[derive(Debug, Clone, sqlx::FromRow)]
    struct Scd2Interval {
        valid_from: TestValidFrom,
        valid_to: TestValidTo,
    }

    impl IntervalColumns for Scd2Interval {
        type ValidFrom = TestValidFrom;
        type ValidTo = TestValidTo;
    }

    #[Dao]
    #[dao(table = "test_current_table", ops(insert), scd2)]
    struct TestCurrentInsertDao {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        status: TestStatus,
    }

    #[Dao]
    #[dao(table = "test_current_table", ops(select), scd2)]
    struct TestCurrentDao {
        #[dao(flatten)]
        base: TestCurrentInsertDao,
        #[dao(flatten)]
        interval: Scd2Interval,
    }

    impl TestCurrentDao {
        pub fn active() -> impl SqlCondition<Self> {
            TestStatus::Active
        }
    }

    #[test]
    fn test_select_open_no_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_open::<TestCurrentDao>(&id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_select_open_with_active_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_open::<TestCurrentDao>(and!(&id, TestCurrentDao::active()))
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "status" = 'A' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_select_open_no_condition() {
        let sql = select_open::<TestCurrentDao>(()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "valid_from", "valid_to" FROM "test_current_table" WHERE TRUE AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }
}
