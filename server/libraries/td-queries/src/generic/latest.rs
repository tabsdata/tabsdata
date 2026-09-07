//
// Copyright 2026 Tabsdata Inc.
//

use crate::generic::chunked::LatestChunked;
use crate::primitives::scd2::select_open;
use crate::{QueryExecutor, Serves};
use ta_objects::errors::{SqlError, format_hint};
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{AddressesOneVersion, Dao, ListDto, Scd2Dao, SelectDao, single_row};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

/// At-now surface over the open SCD2 intervals (`valid_to = 'infinity'`,
/// served by the partial indexes).
pub struct Latest<E> {
    pub(crate) executor: E,
}

impl<E> Latest<E> {
    pub fn chunked(self) -> LatestChunked<E> {
        LatestChunked {
            executor: self.executor,
        }
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
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
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
    struct TestDefinedOn(String);

    #[Dao]
    #[dao(table = "test_current_table", ops(select, insert), scd2)]
    struct TestCurrentDao {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        status: TestStatus,
        #[dao(valid_from)]
        defined_on: TestDefinedOn,
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
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_select_open_with_active_state() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_open::<TestCurrentDao>(and!(&id, TestCurrentDao::active()))
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_current_table" WHERE "partition_id" = 'p1' AND "status" = 'A' AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }

    #[test]
    fn test_select_open_no_condition() {
        let sql = select_open::<TestCurrentDao>(()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_current_table" WHERE TRUE AND "valid_to" = ('infinity'::timestamptz)"#
        );
    }
}
