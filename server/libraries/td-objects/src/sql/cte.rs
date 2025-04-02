//
// Copyright 2025 Tabs Data Inc.
//

use crate::gen_where_clause;
use crate::sql::{Queries, QueryError};
use crate::types::basic::AtTime;
use crate::types::{DataAccessObject, PartitionBy, SqlEntity};
use std::ops::Deref;

#[cfg(feature = "td-test")]
use std::println as trace;
#[cfg(not(feature = "td-test"))]
use tracing::trace;

const LATEST_VERSIONS_CTE: &str = "latest_versions";

/// Common table expressions (CTEs) used in queries to select versioned views of objects.
pub trait CteQueries<'a, E> {
    #[allow(dead_code)]
    fn select_active_versions_at<D>(
        &self,
        defined_on: Option<&'a AtTime>,
        e: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
    where
        D: DataAccessObject + PartitionBy;
}

macro_rules! impl_select_active_versions_at {
    (
        [$($E:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens, unused_variables, unused_mut, unused_assignments)]
        impl<'a, Q, $($E),*> CteQueries<'a, ($(&'a $E),*)> for Q
        where
            Q: Deref<Target = dyn Queries>,
            $($E: SqlEntity),*
        {
            fn select_active_versions_at<D>(
                &self,
                defined_on: Option<&'a AtTime>,
                ($($E),*): &'a ($(&'a $E),*),
            ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
            where
                D: DataAccessObject + PartitionBy,
            {
                let mut query_builder = sqlx::QueryBuilder::default();

                // Build CTEs to find needed data
                query_builder.push("WITH ");
                versions_defined_on::<D>(LATEST_VERSIONS_CTE, &mut query_builder, defined_on);

                // Build query to select the data from the generated CTEs
                let columns = D::fields();
                let select = format!("SELECT {} FROM {}", columns.join(", "), LATEST_VERSIONS_CTE);

                // And add it to the builder
                query_builder.push(select);

                // Where clause
                gen_where_clause!(query_builder, D, $($E),*);

                // And last, order by
                query_builder.push(" ");
                query_builder.push(D::order_by());

                trace!("select_active_versions: sql: {}", query_builder.sql());
                Ok(query_builder)
            }
        }
    };
}

all_the_tuples!(impl_select_active_versions_at);

/// CTE to find the latest versions of objects at a given time.
///
/// It has to be a Versions table with:
/// - Partitioned by a field P, to find the latest versions of each P at the given time.
///   With this, we can group by the versions of the same object.
/// - defined_on (TODO: make it configurable)
/// - status (A being active, which are the versions want to find) (TODO: make it configurable)
pub(crate) fn versions_defined_on<'a, D>(
    cte_table_prefix: &str,
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    defined_on: Option<&'a AtTime>,
) where
    D: DataAccessObject + PartitionBy,
{
    let table = D::sql_table();
    let partition_field = D::partition_by();

    // Build CTEs containing ranked versions ordered by defined_on DESC
    query_builder.push(format!("{cte_table_prefix}_ranked AS ("));
    query_builder.push(format!(
        r#"
            SELECT
                v.*,
                ROW_NUMBER() OVER (PARTITION BY v.{partition_field} ORDER BY v.defined_on DESC) AS rn
            FROM
                {table} v
        "#
    ));
    if let Some(defined_on) = defined_on {
        query_builder.push("WHERE v.defined_on <= ");
        query_builder.push_bind(defined_on);
    }
    query_builder.push(" ),");

    // Build CTEs containing only the latest versions, which are the ones with rn = 1, and status = 'A'
    query_builder.push(format!("{cte_table_prefix} AS ("));
    query_builder.push(format!(
        r#"
            SELECT
                rv.*
            FROM
                {cte_table_prefix}_ranked rv
            WHERE
                rv.rn = 1 AND rv.status = 'A'
        "#
    ));
    query_builder.push(")");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::cte::CteQueries;
    use crate::sql::{DaoQueries, Insert};
    use crate::types::dependency::{DependencyVersionDB, DependencyVersionDBWithNames};
    use crate::types::function::{FunctionVersionDB, FunctionVersionDBWithNames};
    use crate::types::table::{TableVersionDB, TableVersionDBWithNames};
    use crate::types::trigger::{TriggerVersionDB, TriggerVersionDBWithNames};
    use chrono::DateTime;
    use chrono::Utc;
    use lazy_static::lazy_static;
    use sqlx::Execute;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(id)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestPartition;

    #[td_type::typed(string)]
    struct TestStatus;

    #[Dao(sql_table = "test_table", partition_by = "partition_id")]
    struct TestDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestStatus,
        defined_on: AtTime,
    }

    lazy_static! {
        static ref TEST_QUERIES: DaoQueries = DaoQueries::default();
    }

    lazy_static! {
        static ref AT_BEFORE: AtTime = AtTime::try_from(
            "2025-04-02T08:19:52.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap()
        )
        .unwrap();
        static ref AT_04_08: AtTime = AtTime::try_from(
            "2025-04-02T08:19:53.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap()
        )
        .unwrap();
        static ref AT_0C: AtTime = AtTime::try_from(
            "2025-04-02T08:19:54.543+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap()
        )
        .unwrap();
        static ref FIXTURE_DAOS: Vec<TestDao> = vec![
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
        ];
    }

    #[test]
    fn test_versions_defined_on_none() {
        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<TestDao>("test", &mut query_builder, None);
        let query = query_builder.build();

        let expected = "test_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n         ),test AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                test_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1 AND rv.status = 'A'\
        \n        )";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_versions_defined_on_some() {
        let mut query_builder = sqlx::QueryBuilder::default();
        let defined_on = &AtTime::now().await;
        versions_defined_on::<TestDao>("test", &mut query_builder, Some(defined_on));
        let query = query_builder.build();

        let expected =   "test_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),test AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                test_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1 AND rv.status = 'A'\
        \n        )";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_versions_defined_on_dao_partition_by() {
        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<FunctionVersionDB>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(FunctionVersionDB::sql_table()));
        assert!(query.sql().contains(FunctionVersionDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<FunctionVersionDBWithNames>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query
            .sql()
            .contains(FunctionVersionDBWithNames::sql_table()));
        assert!(query
            .sql()
            .contains(FunctionVersionDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<TableVersionDB>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(TableVersionDB::sql_table()));
        assert!(query.sql().contains(TableVersionDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<TableVersionDBWithNames>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(TableVersionDBWithNames::sql_table()));
        assert!(query
            .sql()
            .contains(TableVersionDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<DependencyVersionDB>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(DependencyVersionDB::sql_table()));
        assert!(query.sql().contains(DependencyVersionDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<DependencyVersionDBWithNames>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query
            .sql()
            .contains(DependencyVersionDBWithNames::sql_table()));
        assert!(query
            .sql()
            .contains(DependencyVersionDBWithNames::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<TriggerVersionDB>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(TriggerVersionDB::sql_table()));
        assert!(query.sql().contains(TriggerVersionDB::partition_by()));

        let mut query_builder = sqlx::QueryBuilder::default();
        versions_defined_on::<TriggerVersionDBWithNames>("test", &mut query_builder, None);
        let query = query_builder.build();
        assert!(query.sql().contains(TriggerVersionDBWithNames::sql_table()));
        assert!(query
            .sql()
            .contains(TriggerVersionDBWithNames::partition_by()));
    }

    #[test]
    fn test_select_active_versions_at_none_sql() {
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(None, &())
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n         ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1 AND rv.status = 'A'\
        \n        )\
        SELECT id, partition_id, status, defined_on FROM latest_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_select_active_versions_at_defined_on_sql() {
        let defined_on = &AtTime::now().await;
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(Some(defined_on), &())
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1 AND rv.status = 'A'\
        \n        )\
        SELECT id, partition_id, status, defined_on FROM latest_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[tokio::test]
    async fn test_select_active_versions_at_defined_on_where_sql() {
        let defined_on = &AtTime::now().await;
        let by = &TestId::try_from("00000000000000000000000000").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(Some(defined_on), &(by))
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1 AND rv.status = 'A'\
        \n        )\
        SELECT id, partition_id, status, defined_on FROM latest_versions WHERE id = ? ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_none_fetch_all(db: DbPool) {
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(None, &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_at_none_where_fetch_all(db: DbPool) {
        // 00, no matches
        let by = &TestId::try_from("00000000000000000000000000").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(None, &(by))
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_at_defined_on_before_fetch_all(db: DbPool) {
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(Some(AT_BEFORE.deref()), &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_at_defined_on_04_08_fetch_all(db: DbPool) {
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(Some(AT_04_08.deref()), &())
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 2);
        // Order by id DESC
        assert_eq!(result[0], FIXTURE_DAOS[1]);
        assert_eq!(result[1], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_at_new_active(db: DbPool) -> Result<(), TdError> {
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
        TEST_QUERIES
            .insert::<TestDao>(&new)?
            .build()
            .execute(&db)
            .await
            .unwrap();

        // At now, we get only the new one
        let mut query_builder = TEST_QUERIES.select_active_versions_at::<TestDao>(None, &())?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], new);

        // But right before, we do get the one we had
        let at = &AtTime::try_from(
            "2025-04-02T08:19:55.542+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )?;
        let mut query_builder = TEST_QUERIES.select_active_versions_at::<TestDao>(Some(at), &())?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = "test_cte"))]
    async fn test_select_active_versions_at_defined_on_04_08_fetch_one(db: DbPool) {
        let by = &TestId::try_from("00000000000000000000000004").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_active_versions_at::<TestDao>(Some(AT_04_08.deref()), &(by))
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_DAOS[0]);
    }

    #[td_test::test(sqlx)]
    async fn test_select_active_versions_at_types(db: DbPool) -> Result<(), TdError> {
        async fn test_query<D>(db: &DbPool) -> Result<(), TdError>
        where
            D: DataAccessObject + PartitionBy,
        {
            let mut query_builder = TEST_QUERIES.select_active_versions_at::<D>(None, &())?;
            let _result: Vec<D> = query_builder.build_query_as().fetch_all(db).await.unwrap();
            Ok(())
        }

        test_query::<FunctionVersionDB>(&db).await?;
        test_query::<FunctionVersionDBWithNames>(&db).await?;
        test_query::<TableVersionDB>(&db).await?;
        test_query::<TableVersionDBWithNames>(&db).await?;
        test_query::<DependencyVersionDB>(&db).await?;
        test_query::<DependencyVersionDBWithNames>(&db).await?;
        test_query::<TriggerVersionDB>(&db).await?;
        test_query::<TriggerVersionDBWithNames>(&db).await?;

        Ok(())
    }
}
