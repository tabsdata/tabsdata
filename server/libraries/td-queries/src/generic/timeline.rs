//
// Copyright 2026 Tabsdata Inc.
//

//! The version log of a `TimelineDao`: every version row a condition
//! matches, no time predicate. The only door to a `timeline` relation; the
//! point-in-time reads of an `scd2` one are [`scd2`](crate::Queries::scd2).

use crate::generic::chunked::TimelineChunked;
use crate::generic::data_version::DataVersion;
use crate::primitives::scd2::select_until;
use crate::primitives::table::{select, select_last, select_unordered};
use crate::{QueryExecutor, Serves};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{AddressesOneRow, Dao, ListDto, SelectDao, TimelineDao, single_row};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::list::params::ListQueryParams;

pub struct Timeline<E> {
    pub(crate) executor: E,
}

impl<E> Timeline<E> {
    /// The log read by position over one partition at `on`: `HEAD~k`,
    /// `INITIAL+k`, ranges.
    pub fn at<'a, V, P>(self, on: &'a V, partition: &'a P) -> DataVersion<'a, E, V, P> {
        DataVersion {
            on,
            partition,
            executor: self.executor,
        }
    }

    pub fn chunked(self) -> TimelineChunked<E> {
        TimelineChunked {
            executor: self.executor,
        }
    }

    /// One row of the log by a key that pins it, in practice the version id.
    pub async fn fetch_one<D: SelectDao + TimelineDao>(
        self,
        condition: impl AddressesOneRow<D, E::Scope>,
    ) -> Result<D, SqlError>
    where
        E: Serves<D>,
    {
        self.fetch_optional(condition)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    pub async fn fetch_optional<D: SelectDao + TimelineDao>(
        self,
        condition: impl AddressesOneRow<D, E::Scope>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        let rows = select::<D>(condition)
            .limit(2)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)?;
        single_row::<D>(rows)
    }

    pub async fn fetch_all<D: SelectDao + TimelineDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select::<D>(condition)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn fetch_first<D: SelectDao + TimelineDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        select::<D>(condition)
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }

    pub async fn fetch_last<D: SelectDao + TimelineDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_last::<D>(condition)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
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
        T::Dao: TimelineDao,
        E: QueryExecutor + Domained<Domain = <T::Dao as Dao>::Db>,
    {
        let daos: Vec<T::Dao> = params
            .apply_to(select_unordered::<T::Dao>(condition))
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<T::Dao>)?;
        daos.into_iter()
            .map(|dao| T::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    /// Every version at or before `on`, the audit trail up to an instant.
    pub async fn list_until<T, V>(
        self,
        on: &V,
        condition: impl SqlCondition<T::Dao>,
        params: &ListQueryParams<T>,
    ) -> Result<Vec<T>, SqlError>
    where
        T: ListDto,
        T::Dao: TimelineDao<VersionOrder = V>,
        E: QueryExecutor + Domained<Domain = <T::Dao as Dao>::Db>,
    {
        let daos: Vec<T::Dao> = params
            .apply_to(select_until::<T::Dao>(condition, on))
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
    use ta_objects::{Dao, Dto, FromDao, Typed, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestDefinedOn(String);

    #[Dao]
    #[dao(table = "test_versions_table", ops(select, insert), timeline)]
    struct TestVersionDao {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        #[dao(timeline(version_by), order_by)]
        defined_on: TestDefinedOn,
    }

    #[Dto]
    #[dto(list(on = TestVersionDao))]
    struct TestVersionDto {
        #[dto(list(pagination_by = "+"))]
        id: TestId,
        #[dto(list(order_by))]
        defined_on: TestDefinedOn,
    }

    impl FromDao for TestVersionDto {
        type Dao = TestVersionDao;

        fn from_dao(dao: TestVersionDao) -> Result<Self, TdError> {
            Ok(Self {
                id: dao.id,
                defined_on: dao.defined_on,
            })
        }
    }

    /// The listing base carries no ordering of its own: `apply_to` only
    /// appends, so a DAO default order left in front would outrank the
    /// keyset predicate's column and break paging.
    #[test]
    fn test_list_base_orders_by_the_pagination_column() {
        let partition = TestPartition::try_from("p1").unwrap();
        let params: ListQueryParams<TestVersionDto> = (&ListParams {
            len: 2,
            filter: Default::default(),
            order_by: None,
            previous: None,
            next: Some("i2".to_string()),
            pagination_id: Some("i2".to_string()),
        })
            .try_into()
            .unwrap();
        let sql = params
            .apply_to(select_unordered::<TestVersionDao>(&partition))
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "defined_on" FROM "test_versions_table" WHERE "partition_id" = 'p1' AND "id" > 'i2' ORDER BY "id" ASC LIMIT 3"#
        );
    }
}
