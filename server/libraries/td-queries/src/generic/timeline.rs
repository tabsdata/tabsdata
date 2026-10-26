//
// Copyright 2026 Tabsdata Inc.
//

//! The version-log door: [`Reader`] over every version row of a
//! `TimelineDao`, no time predicate. The only door to a `timeline` relation;
//! the point-in-time reads of an `scd2` one are [`scd2`](crate::Queries::scd2).

use crate::generic::data_version::{DataVersion, Heads};
use crate::generic::grain::{AllVersions, Until};
use crate::generic::reader::Reader;

impl<E> Reader<E, AllVersions> {
    /// The log read by position over one partition at `on`: `HEAD~k`,
    /// `INITIAL+k`, ranges.
    pub fn at<'a, V, P>(self, on: &'a V, partition: &'a P) -> DataVersion<'a, E, V, P> {
        DataVersion {
            on,
            partition,
            executor: self.executor,
        }
    }

    /// The newest row of each of `partitions` at `on`.
    pub fn heads_at<'a, V, P>(self, on: &'a V, partitions: &'a [P]) -> Heads<'a, E, V, P> {
        Heads {
            on,
            partitions,
            executor: self.executor,
        }
    }

    /// Every version at or before `on`, the audit trail up to an instant.
    pub fn until<V>(self, on: &V) -> Reader<E, Until<'_, V>> {
        Reader {
            executor: self.executor,
            grain: Until(on),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::grain::select_in;
    use crate::primitives::listing::listing;
    use sea_query::PostgresQueryBuilder;
    use ta_objects::{Dao, Dto, FromDao, Typed, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_error::TdError;
    use td_objects::dxo::crudl::ListParams;
    use td_objects::list::params::ListQueryParams;
    use td_objects::types::basic::PageLen;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestId(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct TestPartition(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    #[typed(sealed)]
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

    /// The listing base carries no ordering of its own: `listing` only
    /// appends, so a DAO default order left in front would outrank the
    /// keyset predicate's column and break paging.
    #[test]
    fn test_list_base_orders_by_the_pagination_column() {
        let partition = TestPartition::try_from("p1").unwrap();
        let params: ListQueryParams<TestVersionDto> = (ListParams {
            len: PageLen::try_from(2).unwrap(),
            next: Some("i2".to_string()),
            pagination_id: Some("i2".to_string()),
            ..ListParams::default()
        })
        .try_into()
        .unwrap();
        let base = select_in::<TestVersionDao, _>(&AllVersions, &partition);
        let sql = listing(&AllVersions, base, &params).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "partition_id", "defined_on" FROM "test_versions_table" WHERE "partition_id" = 'p1' AND "id" > 'i2' ORDER BY "id" ASC LIMIT 3"#
        );
    }
}
