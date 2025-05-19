//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::cte::{ranked_versions_at, select_ranked_versions_at};
use crate::sql::{Queries, QueryError};
use crate::types::{DataAccessObject, PartitionBy, Recursive, SqlEntity, VersionedAt};
use std::ops::Deref;
use tracing::trace;

const LATEST_REFERENCE_VERSIONS_CTE: &str = "latest_reference_versions";
const LATEST_RECURSION_VERSIONS_CTE: &str = "latest_recursion_versions";
const RECURSIVE_VERSIONS_CTE: &str = "recursive_versions";

impl<Q: Deref<Target = dyn Queries>> RecursiveQueries for Q {}

pub trait RecursiveQueries {
    /// Builds all CTEs and select queries to get the recursive versions of a given object with a
    /// given Function.
    ///
    /// Generics:
    /// - `D`: The DataAccessObject to find the recursive versions of.
    /// - `R`: The DataAccessObject to find the latest versions of the recursive objects.
    /// - `E`: The type of the Initial WHERE to find the recursive versions of.
    ///
    /// CTEs:
    /// - `latest_function_versions`: to find the latest versions of the functions.
    /// - `latest_recursion_versions`: to find the latest versions of the recursive objects
    ///   (dependencies, triggers).
    /// - `recursive_versions`: to find all the versions recursively, starting with
    ///   `latest_function_versions` and recursing with `latest_recursion_versions`.
    /// - `SELECT`: to select the data from the `recursive_versions` CTE.
    ///
    /// Example query for
    ///   D: DependencyVersionsDB, R: FunctionVersionDB, I: FunctionVersionId, E: FunctionId:
    /// ```sql
    /// WITH
    /// ranked_function_versions AS (
    ///     SELECT
    ///         fv.*,
    ///         ROW_NUMBER() OVER (PARTITION BY fv.function_id ORDER BY fv.id DESC) AS rn
    ///     FROM
    ///         function_versions fv
    /// ),
    /// latest_function_versions AS (
    ///     SELECT
    ///         rfv.*
    ///     FROM
    ///         ranked_function_versions rfv
    ///     WHERE
    ///         rfv.rn = 1 AND rfv.status = 'A'
    /// ),
    /// ranked_dependency_versions AS (
    ///     SELECT
    ///         dv.*,
    ///         ROW_NUMBER() OVER (PARTITION BY dv.dependency_id ORDER BY dv.id DESC) AS rn
    ///     FROM
    ///         dependency_versions dv
    /// ),
    /// latest_dependency_versions AS (
    ///     SELECT
    ///         rdv.*
    ///     FROM
    ///         ranked_dependency_versions rdv
    ///     WHERE
    ///         rdv.rn = 1 AND rdv.status = 'A'
    /// ),
    /// recursive_versions AS (
    ///     SELECT
    ///         d.*
    ///     FROM
    ///         latest_dependency_versions d
    ///     INNER JOIN
    ///         latest_function_versions fv ON fv.id = d.function_version_id
    ///     WHERE
    ///         fv.function_id = ?
    ///
    /// UNION ALL
    ///
    ///     SELECT
    ///         d.*
    ///     FROM
    ///         latest_dependency_versions d
    ///     INNER JOIN
    ///         latest_function_versions fv ON fv.id = d.table_function_version_id
    ///     WHERE
    ///         fv.function_id = ?
    ///
    /// UNION ALL
    ///
    ///     SELECT
    ///         d.*
    ///     FROM
    ///         latest_dependency_versions d
    ///     INNER JOIN
    ///         recursive_versions rd ON rd.function_version_id = d.table_function_version_id
    /// )
    /// SELECT DISTINCT
    ///     rd.*
    /// FROM
    ///     recursive_versions rd;
    /// ```
    fn select_recursive_versions_at<'a, D, R, E>(
        &self,
        d_defined_on: Option<&'a <D as VersionedAt>::Order>,
        d_status: Option<&'a [&'a <D as VersionedAt>::Condition]>,
        r_defined_on: Option<&'a <R as VersionedAt>::Order>,
        r_status: Option<&'a [&'a <R as VersionedAt>::Condition]>,
        direct_reference_entity: &'a E,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, QueryError>
    where
        D: DataAccessObject + Recursive + PartitionBy + VersionedAt,
        R: DataAccessObject + PartitionBy + VersionedAt,
        E: SqlEntity, // has to be in R
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        // Build CTEs to find needed data
        query_builder.push("WITH ");
        ranked_versions_at::<R>(
            LATEST_REFERENCE_VERSIONS_CTE,
            &mut query_builder,
            r_defined_on,
        );
        select_ranked_versions_at::<R>(LATEST_REFERENCE_VERSIONS_CTE, &mut query_builder, r_status);
        query_builder.push(",");
        ranked_versions_at::<D>(
            LATEST_RECURSION_VERSIONS_CTE,
            &mut query_builder,
            d_defined_on,
        );
        select_ranked_versions_at::<D>(LATEST_RECURSION_VERSIONS_CTE, &mut query_builder, d_status);
        query_builder.push(",");

        recursive_versions_sql::<D, R, E>(&mut query_builder, direct_reference_entity)?;
        query_builder.push(" ");

        // Build query to select the data from the generated CTEs
        let select_columns = D::fields().join(", ");
        let select = format!("SELECT DISTINCT {select_columns} FROM {RECURSIVE_VERSIONS_CTE}");

        // And add it to the builder
        query_builder.push(select);

        // And last, order by
        query_builder.push(" ");
        query_builder.push(<D as DataAccessObject>::order_by());

        trace!(
            "select_active_recursive_versions_at: sql: {}",
            query_builder.sql()
        );
        Ok(query_builder)
    }
}

/// CTE to find all the versions recursively of a given object.
fn recursive_versions_sql<'a, D, R, E>(
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    direct_reference_entity: &'a E,
) -> Result<(), QueryError>
where
    D: Recursive + PartitionBy,
    R: DataAccessObject,
    E: SqlEntity, // has to be in R
{
    // Starting point to find recursive versions
    let starting_at = R::sql_field_for_type::<E>().ok_or(QueryError::TypeNotFound(
        std::any::type_name::<E>().to_string(),
    ))?;

    // Baseline to find initial versions
    let recursion_ref = R::sql_field_for_type::<D::Recursive>().ok_or(QueryError::TypeNotFound(
        std::any::type_name::<D::Recursive>().to_string(),
    ))?;

    // And columns to recurse on the initial found versions
    let recurse_up = D::recurse_up();
    let recurse_down = D::recurse_down();

    query_builder.push(format!("{RECURSIVE_VERSIONS_CTE} AS ("));
    // Direct Version upstream
    query_builder.push(format!(
        r#"
            SELECT
                d.*
            FROM
                {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN
                {LATEST_REFERENCE_VERSIONS_CTE} fv ON fv.{recursion_ref} = d.{recurse_up}
        "#
    ));
    query_builder.push(format!("WHERE fv.{starting_at} = "));
    query_builder.push_bind(direct_reference_entity.value());

    query_builder.push(" UNION ALL ");

    // Direct Version downstream
    query_builder.push(format!(
        r#"
        SELECT
                d.*
            FROM
                {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN
                {LATEST_REFERENCE_VERSIONS_CTE} fv ON fv.{recursion_ref} = d.{recurse_down}
        "#
    ));
    query_builder.push(format!("WHERE fv.{starting_at} = "));
    query_builder.push_bind(direct_reference_entity.value());

    query_builder.push(" UNION ALL ");

    // And then all downstream recursive
    query_builder.push(format!(
        r#"
            SELECT
                d.*
            FROM
                {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN
                {RECURSIVE_VERSIONS_CTE} rd ON rd.{recurse_down} = d.{recurse_up}
        "#
    ));

    query_builder.push(")");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{DaoQueries, Insert};
    use crate::types::basic::AtTime;
    use crate::types::basic::{CollectionId, FunctionId};
    use crate::types::dependency::{DependencyVersionDB, DependencyVersionDBWithNames};
    use crate::types::function::FunctionVersionDB;
    use crate::types::trigger::{TriggerVersionDB, TriggerVersionDBWithNames};
    use chrono::DateTime;
    use chrono::Utc;
    use lazy_static::lazy_static;
    use sqlx::Execute;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(string)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestPartition;

    #[td_type::typed(string)]
    struct TestStatus;

    #[td_type::typed(string)]
    struct TestRecursion;

    // partitioning by id means there are no groups, each entry is independent
    #[Dao]
    #[dao(
        sql_table = "test_table",
        partition_by = "id",
        versioned_at(order_by = "defined_on", condition_by = "status"),
        recursive(up = "current", down = "downstream")
    )]
    struct TestDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestStatus,
        current: TestRecursion,
        downstream: TestRecursion,
        defined_on: AtTime,
    }

    #[Dao]
    #[dao(
        sql_table = "test_table_reference",
        partition_by = "partition_id",
        versioned_at(order_by = "defined_on", condition_by = "status")
    )]
    struct RecursionReference {
        id: TestId,
        partition_id: TestPartition,
        reference_id: TestRecursion,
        status: TestStatus,
        defined_on: AtTime,
    }

    lazy_static! {
        static ref TEST_QUERIES: DaoQueries = DaoQueries::default();
    }

    fn time(i: usize) -> AtTime {
        AtTime::try_from(
            format!("2025-04-02T08:19:5{}.543+00:00", i)
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    }

    lazy_static! {
        static ref FIXTURE_TEST_DAOS: Vec<TestDao> = vec![
            TestDao {
                id: TestId::try_from("AAA").unwrap(),
                partition_id: TestPartition::try_from("p_10").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_0").unwrap(),
                downstream: TestRecursion::try_from("ref_1").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("BBB").unwrap(),
                partition_id: TestPartition::try_from("p_11").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_2").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("CCC").unwrap(),
                partition_id: TestPartition::try_from("p_11").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_3").unwrap(),
                defined_on: time(1),
            },
            TestDao {
                id: TestId::try_from("DDD").unwrap(),
                partition_id: TestPartition::try_from("p_12").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_4").unwrap(),
                defined_on: time(2),
            },
            TestDao {
                id: TestId::try_from("EEE").unwrap(),
                partition_id: TestPartition::try_from("p_13").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_4").unwrap(),
                downstream: TestRecursion::try_from("ref_5").unwrap(),
                defined_on: time(3),
            },
        ];
    }

    lazy_static! {
        static ref FIXTURE_RECURSION_REF: Vec<RecursionReference> = vec![
            RecursionReference {
                id: TestId::try_from("MMM").unwrap(),
                partition_id: TestPartition::try_from("p_0").unwrap(),
                reference_id: TestRecursion::try_from("ref_0").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("NNN").unwrap(),
                partition_id: TestPartition::try_from("p_1").unwrap(),
                reference_id: TestRecursion::try_from("ref_1").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("OOO").unwrap(),
                partition_id: TestPartition::try_from("p_2").unwrap(),
                reference_id: TestRecursion::try_from("ref_2").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("PPP").unwrap(),
                partition_id: TestPartition::try_from("p_2").unwrap(),
                reference_id: TestRecursion::try_from("ref_3").unwrap(),
                status: TestStatus::try_from("D").unwrap(),
                defined_on: time(1),
            },
            RecursionReference {
                id: TestId::try_from("QQQ").unwrap(),
                partition_id: TestPartition::try_from("p_3").unwrap(),
                reference_id: TestRecursion::try_from("ref_4").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(2),
            },
            RecursionReference {
                id: TestId::try_from("RQQQ").unwrap(),
                partition_id: TestPartition::try_from("p_4").unwrap(),
                reference_id: TestRecursion::try_from("ref_5").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(3),
            },
        ];
    }

    #[test]
    fn test_recursive_active_versions_sql() {
        let mut query_builder = sqlx::QueryBuilder::default();

        let id = &TestId::try_from("AAA").unwrap();
        recursive_versions_sql::<TestDao, RecursionReference, _>(&mut query_builder, id).unwrap();
        let query = query_builder.build();

        let expected = "recursive_versions AS (\
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.current\
        \n        WHERE fv.id = ? UNION ALL \
        \n        SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.downstream\
        \n        WHERE fv.id = ? UNION ALL \
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                recursive_versions rd ON rd.downstream = d.current\
        \n        )";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_select_active_recursive_versions_at_none_sql() {
        let id = TestId::try_from("AAA").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_reference_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table_reference v\
        \n         ),latest_reference_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_reference_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),latest_recursion_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n         ),latest_recursion_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_recursion_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),recursive_versions AS (\
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.current\
        \n        WHERE fv.id = ? UNION ALL \
        \n        SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.downstream\
        \n        WHERE fv.id = ? UNION ALL \
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                recursive_versions rd ON rd.downstream = d.current\
        \n        ) SELECT DISTINCT id, partition_id, status, current, downstream, defined_on FROM recursive_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_select_active_recursive_versions_at_sql() {
        let at = time(0);
        let id = TestRecursion::try_from("ref_X").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                Some(&at),
                None,
                Some(&at),
                None,
                &id,
            )
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH latest_reference_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.partition_id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table_reference v\
        \n        WHERE v.defined_on <= ? ),latest_reference_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_reference_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),latest_recursion_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.id ORDER BY v.defined_on DESC) AS rn\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_recursion_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_recursion_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),recursive_versions AS (\
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.current\
        \n        WHERE fv.reference_id = ? UNION ALL \
        \n        SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                latest_reference_versions fv ON fv.reference_id = d.downstream\
        \n        WHERE fv.reference_id = ? UNION ALL \
        \n            SELECT\
        \n                d.*\
        \n            FROM\
        \n                latest_recursion_versions d\
        \n            INNER JOIN\
        \n                recursive_versions rd ON rd.downstream = d.current\
        \n        ) SELECT DISTINCT id, partition_id, status, current, downstream, defined_on FROM recursive_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_none_fetch_all(db: DbPool) {
        let id = TestId::try_from("MMM").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // MMM: AAA -> BBB -> CCC
        //                    DDD -> EEE
        assert_eq!(result.len(), 5);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[3], FIXTURE_TEST_DAOS[1]);
        assert_eq!(result[4], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_none_fetch_all_upstream(db: DbPool) {
        let id = TestId::try_from("NNN").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // NNN: BBB -> BBB
        //             CCC
        //             DDD -> EEE
        //             AAA (direct upstream)
        assert_eq!(result.len(), 5);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[3], FIXTURE_TEST_DAOS[1]);
        assert_eq!(result[4], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_at_time_fetch_all(db: DbPool) {
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                Some(&at),
                None,
                Some(&at),
                None,
                &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // MMM: AAA -> BBB -> CCC (the rest didn't exist yet)
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[1]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_none_last_in_stream(db: DbPool) {
        let id = TestId::try_from("RRR").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // RRR: EEE (direct upstream)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_none_deleted(db: DbPool) {
        // OOO is active, but PPP has the same partition_id and is deleted
        let id = TestId::try_from("OOO").unwrap();
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    async fn test_select_active_versions_heavy(db: DbPool) -> Result<(), TdError> {
        let size: usize = 1000;

        let mut daos = vec![];
        for i in 0..size {
            let reference = RecursionReference {
                id: TestId::try_from(format!("YYY{:04}", i))?,
                partition_id: TestPartition::try_from(format!("x_p_{:04}", i))?,
                reference_id: TestRecursion::try_from(format!("x_ref_{:04}", i))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            TEST_QUERIES
                .insert(&reference)?
                .build()
                .execute(&db)
                .await
                .unwrap();
            let dao = TestDao {
                id: TestId::try_from(format!("ZZZ{:04}", i))?,
                partition_id: TestPartition::try_from(format!("x_p_{:04}", i))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("x_ref_{:04}", i))?,
                downstream: TestRecursion::try_from(format!("x_ref_{:04}", i + 1))?,
                defined_on: time(5),
            };
            TEST_QUERIES
                .insert(&dao)?
                .build()
                .execute(&db)
                .await
                .unwrap();
            daos.push(dao);
        }

        let id = TestId::try_from(format!("YYY{:04}", 0))?;
        let mut query_builder = TEST_QUERIES
            .select_recursive_versions_at::<TestDao, RecursionReference, _>(
                None, None, None, None, &id,
            )?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), size);
        for i in 0..size {
            assert_eq!(result[i], daos[size - 1 - i]);
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_select_active_versions_at_types(db: DbPool) -> Result<(), TdError> {
        async fn test_query<D, R, E>(db: &DbPool, recursion_ref: &E) -> Result<(), TdError>
        where
            D: DataAccessObject + Recursive + PartitionBy + VersionedAt,
            R: DataAccessObject + PartitionBy + VersionedAt,
            E: SqlEntity,
        {
            let mut query_builder = TEST_QUERIES.select_recursive_versions_at::<D, R, E>(
                None,
                None,
                None,
                None,
                recursion_ref,
            )?;
            let _result: Vec<D> = query_builder.build_query_as().fetch_all(db).await.unwrap();
            Ok(())
        }

        test_query::<DependencyVersionDB, FunctionVersionDB, _>(&db, &FunctionId::default())
            .await?;
        test_query::<DependencyVersionDB, FunctionVersionDB, _>(&db, &CollectionId::default())
            .await?;
        test_query::<DependencyVersionDBWithNames, FunctionVersionDB, _>(
            &db,
            &FunctionId::default(),
        )
        .await?;
        test_query::<DependencyVersionDBWithNames, FunctionVersionDB, _>(
            &db,
            &CollectionId::default(),
        )
        .await?;

        test_query::<TriggerVersionDB, FunctionVersionDB, _>(&db, &FunctionId::default()).await?;
        test_query::<TriggerVersionDB, FunctionVersionDB, _>(&db, &CollectionId::default()).await?;
        test_query::<TriggerVersionDBWithNames, FunctionVersionDB, _>(&db, &FunctionId::default())
            .await?;
        test_query::<TriggerVersionDBWithNames, FunctionVersionDB, _>(
            &db,
            &CollectionId::default(),
        )
        .await?;

        Ok(())
    }
}
