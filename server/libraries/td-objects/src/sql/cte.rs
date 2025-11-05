//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{Queries, gen_where_clause};
use crate::table_ref::{Version, Versions};
use crate::types::{AsDynSqlEntities, DataAccessObject, SqlEntity, States, Versioned};
use std::ops::Deref;
use td_error::TdError;
use tracing::trace;

pub const LATEST_VERSIONS_CTE: &str = "latest_versions";

/// Common table expressions (CTEs) used in queries to select versioned views of objects.
pub trait CteQueries<'a, E> {
    fn select_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;

    fn find_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        e: &'a [E],
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

impl<'a, Q, E> CteQueries<'a, E> for Q
where
    Q: Deref<Target = dyn Queries>,
    E: AsDynSqlEntities,
{
    fn select_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        // Build CTEs to find needed data
        query_builder.push("WITH ");
        ranked_versions_at::<D>(LATEST_VERSIONS_CTE, &mut query_builder, natural_order_by);
        select_ranked_versions_at::<S, D>(LATEST_VERSIONS_CTE, &mut query_builder)?;

        // Build query to select the data from the generated CTEs
        let columns = D::fields();
        let select = format!("SELECT {} FROM {}", columns.join(", "), LATEST_VERSIONS_CTE);

        // And add it to the builder
        query_builder.push(select);

        // Where clause
        gen_where_clause::<D, E>(&mut query_builder, std::slice::from_ref(e))?;

        // And last, order by
        query_builder.push(" ");
        query_builder.push(<D as DataAccessObject>::order_by());

        trace!("select_versions: sql: {}", query_builder.sql());
        Ok(query_builder)
    }

    fn find_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        e: &'a [E],
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        // Build CTEs to find needed data
        query_builder.push("WITH ");
        ranked_versions_at::<D>(LATEST_VERSIONS_CTE, &mut query_builder, natural_order_by);
        select_ranked_versions_at::<S, D>(LATEST_VERSIONS_CTE, &mut query_builder)?;

        // Build query to select the data from the generated CTEs
        let columns = D::fields();
        let select = format!("SELECT {} FROM {}", columns.join(", "), LATEST_VERSIONS_CTE);

        // And add it to the builder
        query_builder.push(select);

        // Where clause
        if e.is_empty() {
            // Safeguard so empty lookups don't find all rows
            query_builder.push(" WHERE 1 = 0");
        } else {
            gen_where_clause::<D, E>(&mut query_builder, e)?;
        }

        // And last, order by
        query_builder.push(" ");
        query_builder.push(<D as DataAccessObject>::order_by());

        trace!("find_active_versions: sql: {}", query_builder.sql());
        Ok(query_builder)
    }
}

/// CTEs to find the latest versions of objects at a given time.
///
/// It has to be a Versions table with:
/// - Versioned by a field P, to find the latest versions of each P at the given time.
///   With this, we can group by the versions of the same object.
/// - Natural order by a field O, to sort the results by that order.
/// - Status field S, to filter the results by that status.
pub(crate) fn ranked_versions_at<'a, D>(
    cte_table_prefix: &str,
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    natural_order_by: Option<&'a <D as Versioned>::Order>,
) where
    D: DataAccessObject + Versioned,
{
    let table = D::sql_table();
    let partition_field = D::partition_by();
    let natural_order_field = <D as Versioned>::order_by();

    // Build CTEs containing ranked versions ordered by defined_on DESC and
    // first field DESC (in case of ties, knowing that ids are sortable too).
    query_builder.push(format!("{cte_table_prefix}_ranked AS ("));
    query_builder.push(format!(
        r#"
            SELECT
                v.*,
                ROW_NUMBER() OVER (PARTITION BY v.{partition_field} ORDER BY v.{natural_order_field} DESC, v.id DESC) AS rn,
                COUNT(*) OVER (PARTITION BY v.{partition_field}) AS total_count
            FROM
                {table} v
        "#
    ));
    if let Some(natural_order_by) = natural_order_by {
        query_builder.push(format!("WHERE v.{natural_order_field} <= "));
        natural_order_by.push_bind(query_builder);
    }
    query_builder.push(" ),");
}

pub(crate) fn select_ranked_versions_at<'a, const S: u8, D>(
    cte_table_prefix: &str,
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
) -> Result<(), TdError>
where
    D: DataAccessObject + Versioned + States<S>,
{
    // Build CTEs containing only the latest versions, which are the ones with rn = 1
    query_builder.push(format!("{cte_table_prefix} AS ("));
    query_builder.push(format!(
        r#"
            SELECT
                rv.*
            FROM
                {cte_table_prefix}_ranked rv
            WHERE
                rv.rn = 1
        "#
    ));

    // With the given statuses needed
    state_where::<S, D>(query_builder)?;

    query_builder.push(")");

    Ok(())
}

/// Common table expressions (CTEs) used in queries to select versioned views of objects.
pub trait TableQueries<'a> {
    fn select_table_data_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        table_id: &'a <D as Versioned>::Partition,
        versions: &'a Versions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;

    fn find_relative_offset<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        table_id: &'a <D as Versioned>::Partition,
        versions: &'a Versions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>;
}

impl<'a, Q> TableQueries<'a> for Q
where
    Q: Deref<Target = dyn Queries>,
{
    fn select_table_data_versions_at<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        table_id: &'a <D as Versioned>::Partition,
        versions: &'a Versions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        query_builder.push("WITH ");
        ranked_versions_at::<D>(LATEST_VERSIONS_CTE, &mut query_builder, natural_order_by);
        select_table_data_versions_at::<S, D>(
            LATEST_VERSIONS_CTE,
            &mut query_builder,
            table_id,
            versions,
        )?;

        let columns = D::fields();
        let select = format!("SELECT {} FROM {}", columns.join(", "), LATEST_VERSIONS_CTE);
        query_builder.push(select);

        trace!(
            "select_table_data_versions_at: sql: {}",
            query_builder.sql()
        );
        Ok(query_builder)
    }

    fn find_relative_offset<const S: u8, D>(
        &self,
        natural_order_by: Option<&'a <D as Versioned>::Order>,
        table_id: &'a <D as Versioned>::Partition,
        versions: &'a Versions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Versioned + States<S>,
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        query_builder.push("WITH ");
        ranked_versions_at::<D>(LATEST_VERSIONS_CTE, &mut query_builder, natural_order_by);
        select_table_data_versions_at::<S, D>(
            LATEST_VERSIONS_CTE,
            &mut query_builder,
            table_id,
            versions,
        )?;

        let select = format!("SELECT rn FROM {LATEST_VERSIONS_CTE}");
        query_builder.push(select);

        trace!("find_relative_offset: sql: {}", query_builder.sql());
        Ok(query_builder)
    }
}

/// CTE to find the latest versions of table data versions at a given time.
///
/// CARE: Only allowed D is TableDataVersionDB (or its variants, WithNames, etc.).
fn select_table_data_versions_at<'a, const S: u8, D>(
    cte_table_prefix: &str,
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    table_id: &'a <D as Versioned>::Partition,
    versions: &'a Versions,
) -> Result<(), TdError>
where
    D: DataAccessObject + Versioned + States<S>,
{
    let table_id_field = D::partition_by();

    query_builder.push(format!("{cte_table_prefix} AS ("));
    query_builder.push(format!(
        r#"
            SELECT
                rv.*
            FROM
                {cte_table_prefix}_ranked rv
        "#
    ));

    query_builder.push(" WHERE ");
    query_builder.push(format!("rv.{table_id_field} = "));
    table_id.push_bind(query_builder);

    // Build the where clause for the versions
    match versions {
        Versions::None => {
            query_builder.push(" AND rv.rn = 1 ");
        }
        Versions::Single(version) => match version {
            Version::Fixed(id) => {
                query_builder.push(" AND rv.id = ");
                query_builder.push_bind(id);
            }
            Version::Head(back) => {
                query_builder.push(" AND rv.rn = ");
                query_builder.push_bind((-back + 1) as i64);
            }
            Version::Initial(forward) => {
                query_builder.push(" AND rv.rn = total_count - ");
                query_builder.push_bind(*forward as i64);
            }
        },
        Versions::List(versions) => {
            query_builder.push(" AND ");
            let mut separated = query_builder.separated(" OR ");
            versions.iter().for_each(|version| match version {
                Version::Fixed(id) => {
                    separated.push(" rv.id = ");
                    separated.push_bind_unseparated(id);
                }
                Version::Head(back) => {
                    // We are not really using this, as we do not know which of these is
                    // present or not and we actually need it.
                    separated.push(" rv.rn = ");
                    separated.push_bind_unseparated((-back + 1) as i64);
                }
                Version::Initial(forward) => {
                    // We are not really using this, as we do not know which of these is
                    // present or not and we actually need it.
                    separated.push(" rv.rn = total_count - ");
                    separated.push_bind_unseparated(*forward as i64);
                }
            });
        }
        Versions::Range(from, to) => {
            // Ranges only include versions between older-newer. So doing HEAD~0..HEAD~2 gives
            // an empty result, but HEAD~2..HEAD~0 gives the last three versions, if any.
            // And so INITIAL~1..INITIAL~3 gives the second and third versions, if any, but
            // INITIAL~3..INITIAL~1 gives an empty result.
            // The range of combining HEAD and INITIAL is defined by the actual versions present.
            match from {
                Version::Fixed(id) => {
                    query_builder.push(" AND rv.rn <= ");
                    query_builder.push(format!(
                        "(SELECT rn FROM {cte_table_prefix}_ranked WHERE id = "
                    ));
                    query_builder.push_bind(id);
                    query_builder.push(" ) ");
                }
                Version::Head(back) => {
                    query_builder.push(" AND rv.rn <= ");
                    query_builder.push_bind((-back + 1) as i64);
                }
                Version::Initial(forward) => {
                    query_builder.push(" AND rv.rn <= total_count - ");
                    query_builder.push_bind(*forward as i64);
                }
            }
            match to {
                Version::Fixed(id) => {
                    query_builder.push(" AND rv.rn >= ");
                    query_builder.push(format!(
                        "(SELECT rn FROM {cte_table_prefix}_ranked WHERE id = "
                    ));
                    query_builder.push_bind(id);
                    query_builder.push(" ) ");
                }
                Version::Head(back) => {
                    query_builder.push(" AND rv.rn >= ");
                    query_builder.push_bind((-back + 1) as i64);
                }
                Version::Initial(forward) => {
                    query_builder.push(" AND rv.rn >= total_count - ");
                    query_builder.push_bind(*forward as i64);
                }
            }
            // Order by rn DESC, so the older versions come first
            query_builder.push(" ORDER BY rv.rn DESC ");
        }
    }

    state_where::<S, D>(query_builder)?;
    query_builder.push(")");

    Ok(())
}

fn state_where<const S: u8, D>(
    query_builder: &mut sqlx::QueryBuilder<sqlx::Sqlite>,
) -> Result<(), TdError>
where
    D: DataAccessObject + States<S>,
{
    let status = D::state();
    if !status.is_empty() {
        query_builder.push(" AND (");
        let mut separated = query_builder.separated(" OR ");
        for status in status {
            let field = D::sql_field_for_type(status.type_id())?;
            separated.push(format!("rv.{field} = "));
            status.push_bind_unseparated(&mut separated);
        }
        query_builder.push(")");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::dependency::defs::{DependencyDB, DependencyDBWithNames};
    use crate::dxo::function::defs::{FunctionDB, FunctionDBWithNames};
    use crate::dxo::table::defs::{TableDB, TableDBWithNames};
    use crate::dxo::trigger::defs::{TriggerDB, TriggerDBWithNames};
    use crate::sql::cte::CteQueries;
    use crate::sql::{DaoQueries, Insert};
    use crate::types::timestamp::AtTime;
    use chrono::DateTime;
    use chrono::Utc;
    use sqlx::Execute;
    use std::sync::LazyLock;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(id)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestPartition;

    #[td_type::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "test_table",
        versioned(order_by = "defined_on", partition_by = "partition_id"),
        states(
            All = &[],
            Active = &[&TestStatus::Active],
        )
    )]
    struct TestDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestStatus,
        defined_on: AtTime,
    }

    static AT_BEFORE: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::try_from(
            "2025-04-02T08:19:52.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    });
    static AT_04_08: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::try_from(
            "2025-04-02T08:19:53.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    });
    static AT_0C: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::try_from(
            "2025-04-02T08:19:54.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    });
    static FIXTURE_DAOS: LazyLock<Vec<TestDao>> = LazyLock::new(|| {
        vec![
            TestDao {
                id: TestId::try_from("00000000000000000000000004").unwrap(),
                partition_id: TestPartition::try_from("0").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: AT_04_08.clone(),
            },
            TestDao {
                id: TestId::try_from("00000000000000000000000008").unwrap(),
                partition_id: TestPartition::try_from("1").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: AT_04_08.clone(),
            },
            TestDao {
                id: TestId::try_from("0000000000000000000000000C").unwrap(),
                partition_id: TestPartition::try_from("1").unwrap(),
                status: TestStatus::try_from("D").unwrap(),
                defined_on: AT_0C.clone(),
            },
        ]
    });

    #[test]
    fn test_versions_defined_on_none() {
        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<TestDao>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ TestDao::Active }, TestDao>("test", &mut query_builder)
            .unwrap();
        let query = query_builder.build();

        let expected = "test_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n         ),test AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                test_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n         AND (rv.status = ?))";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_versions_defined_on_some() {
        let mut query_builder = sqlx::QueryBuilder::default();
        let defined_on = &AtTime::now();
        ranked_versions_at::<TestDao>("test", &mut query_builder, Some(defined_on));
        select_ranked_versions_at::<{ TestDao::Active }, TestDao>("test", &mut query_builder)
            .unwrap();
        let query = query_builder.build();

        let expected = "test_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),test AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                test_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n         AND (rv.status = ?))";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_versions_defined_on_dao_partition_by() {
        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<FunctionDB>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ FunctionDB::All }, FunctionDB>("test", &mut query_builder)
            .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(FunctionDB::sql_table()));
        assert!(query.sql().contains(FunctionDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<FunctionDBWithNames>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ FunctionDBWithNames::All }, FunctionDBWithNames>(
            "test",
            &mut query_builder,
        )
        .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(FunctionDBWithNames::sql_table()));
        assert!(query.sql().contains(FunctionDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<TableDB>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ TableDB::All }, TableDB>("test", &mut query_builder).unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(TableDB::sql_table()));
        assert!(query.sql().contains(TableDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<TableDBWithNames>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
            "test",
            &mut query_builder,
        )
        .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(TableDBWithNames::sql_table()));
        assert!(query.sql().contains(TableDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<DependencyDB>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ DependencyDB::All }, DependencyDB>(
            "test",
            &mut query_builder,
        )
        .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(DependencyDB::sql_table()));
        assert!(query.sql().contains(DependencyDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<DependencyDBWithNames>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ DependencyDBWithNames::All }, DependencyDBWithNames>(
            "test",
            &mut query_builder,
        )
        .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(DependencyDBWithNames::sql_table()));
        assert!(query.sql().contains(DependencyDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<TriggerDB>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ TriggerDB::All }, TriggerDB>("test", &mut query_builder)
            .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(TriggerDB::sql_table()));
        assert!(query.sql().contains(TriggerDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        ranked_versions_at::<TriggerDBWithNames>("test", &mut query_builder, None);
        select_ranked_versions_at::<{ TriggerDBWithNames::All }, TriggerDBWithNames>(
            "test",
            &mut query_builder,
        )
        .unwrap();
        let query = query_builder.build();
        assert!(query.sql().contains(TriggerDBWithNames::sql_table()));
        assert!(query.sql().contains(TriggerDBWithNames::partition_by()));
    }

    #[test]
    fn test_select_versions_at_none_sql() {
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(None, &())
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n         ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        )SELECT id, partition_id, status, defined_on FROM latest_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_select_versions_at_defined_on_sql() {
        let defined_on = &AtTime::now();
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(Some(defined_on), &())
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        )SELECT id, partition_id, status, defined_on FROM latest_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_select_versions_at_defined_on_where_sql() {
        let defined_on = &AtTime::now();
        let by = TestId::try_from("00000000000000000000000000").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(Some(defined_on), &by)
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        )SELECT id, partition_id, status, defined_on FROM latest_versions WHERE (id = ?) ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_none_fetch_all(db: DbPool) {
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::Active }, TestDao>(None, &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_at_none_where_fetch_all(db: DbPool) {
        // 00, no matches
        let by = TestId::try_from("00000000000000000000000000").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(None, &by)
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_at_defined_on_before_fetch_all(db: DbPool) {
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(Some(AT_BEFORE.deref()), &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_at_defined_on_04_08_fetch_all(db: DbPool) {
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(Some(AT_04_08.deref()), &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 2);
        // Order by id DESC
        assert_eq!(result[0], FIXTURE_DAOS[1]);
        assert_eq!(result[1], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_at_new_active(db: DbPool) -> Result<(), TdError> {
        let new = TestDaoBuilder::default()
            .id(TestId::try_from("0000000000000000000000000S")?)
            // Same partition ID as 04
            .partition_id(TestPartition::try_from("0")?)
            .status(TestStatus::try_from("A")?)
            .defined_on(AtTime::try_from(
                "2025-04-02T08:19:55.543+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            )?)
            .build()?;
        DaoQueries::default()
            .insert::<TestDao>(&new)?
            .build()
            .execute(&db)
            .await
            .unwrap();

        // At now, we get only the new one
        let mut query_builder =
            DaoQueries::default().select_versions_at::<{ TestDao::Active }, TestDao>(None, &())?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], new);

        // But right before, we do get the one we had
        let at = &AtTime::try_from(
            "2025-04-02T08:19:55.542+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )?;
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::Active }, TestDao>(Some(at), &())?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    #[tokio::test]
    async fn test_select_versions_at_defined_on_04_08_fetch_one(db: DbPool) {
        let by = TestId::try_from("00000000000000000000000004").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_versions_at::<{ TestDao::All }, TestDao>(Some(AT_04_08.deref()), &by)
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_select_versions_at_types(db: DbPool) -> Result<(), TdError> {
        async fn test_query<const S: u8, D>(db: &DbPool) -> Result<(), TdError>
        where
            D: DataAccessObject + Versioned + States<S>,
        {
            let mut query_builder = DaoQueries::default().select_versions_at::<S, D>(None, &())?;
            let _result: Vec<D> = query_builder.build_query_as().fetch_all(db).await.unwrap();
            Ok(())
        }

        test_query::<{ FunctionDB::All }, FunctionDB>(&db).await?;
        test_query::<{ FunctionDBWithNames::All }, FunctionDBWithNames>(&db).await?;
        test_query::<{ TableDB::All }, TableDB>(&db).await?;
        test_query::<{ TableDBWithNames::All }, TableDBWithNames>(&db).await?;
        test_query::<{ DependencyDB::All }, DependencyDB>(&db).await?;
        test_query::<{ DependencyDBWithNames::All }, DependencyDBWithNames>(&db).await?;
        test_query::<{ TriggerDB::All }, TriggerDB>(&db).await?;
        test_query::<{ TriggerDBWithNames::All }, TriggerDBWithNames>(&db).await?;

        Ok(())
    }
}
