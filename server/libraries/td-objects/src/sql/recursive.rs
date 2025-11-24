//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::{DataAccessObject, Recursive, States, Versioned};
use crate::sql::Queries;
use crate::sql::cte::{ranked_versions_at, select_ranked_versions_at};
use crate::types::SqlEntity;
use crate::types::basic::Depth;
use std::ops::Deref;
use td_error::TdError;
use tracing::trace;

const LATEST_REFERENCE_VERSIONS_CTE: &str = "latest_reference_versions";
const LATEST_RECURSION_VERSIONS_CTE: &str = "latest_recursion_versions";
const RECURSIVE_VERSIONS_CTE: &str = "recursive_versions";

impl<Q: Deref<Target = dyn Queries>> RecursiveQueries for Q {}

#[derive(Debug, Clone, Copy, Default)]
pub struct RecursionOptions {
    /// Maximum recursive depth when going upstream (toward ancestors)
    pub max_upstream_depth: Option<Depth>,

    /// Maximum recursive depth when going downstream (toward descendants)
    pub max_downstream_depth: Option<Depth>,
}

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
    ///   D: DependencyVersionsDB, R: FunctionDB, I: FunctionId, E: FunctionId:
    /// ```sql
    /// WITH
    /// ranked_function_versions AS (
    ///     SELECT
    ///         fv.*,
    ///         ROW_NUMBER() OVER (PARTITION BY fv.function_id ORDER BY fv.id DESC) AS rn,
    ///         COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count
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
    ///         ROW_NUMBER() OVER (PARTITION BY dv.dependency_id ORDER BY dv.id DESC) AS rn,
    ///         COUNT(*) OVER (PARTITION BY v.partition_id) AS total_count
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
    fn select_recursive_versions_at<'a, const DS: u8, D, const RS: u8, R>(
        &self,
        d_defined_on: Option<&'a <D as Versioned>::Order>,
        r_defined_on: Option<&'a <R as Versioned>::Order>,
        direct_reference_entity: &'a dyn SqlEntity, // has to be in R
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Recursive + Versioned + States<DS>,
        R: DataAccessObject + Versioned + States<RS>,
    {
        self.select_recursive_versions_at_with::<DS, D, RS, R>(
            d_defined_on,
            r_defined_on,
            direct_reference_entity,
            RecursionOptions {
                max_upstream_depth: Some(Depth::ZERO),
                max_downstream_depth: None,
            },
        )
    }

    fn select_recursive_versions_at_with<'a, const DS: u8, D, const RS: u8, R>(
        &self,
        d_defined_on: Option<&'a <D as Versioned>::Order>,
        r_defined_on: Option<&'a <R as Versioned>::Order>,
        direct_reference_entity: &'a dyn SqlEntity, // has to be in R
        options: RecursionOptions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Recursive + Versioned + States<DS>,
        R: DataAccessObject + Versioned + States<RS>,
    {
        let mut query_builder = sqlx::QueryBuilder::default();

        // Build CTEs to find needed data
        query_builder.push("WITH RECURSIVE ");
        ranked_versions_at::<R>(
            LATEST_REFERENCE_VERSIONS_CTE,
            &mut query_builder,
            r_defined_on,
        );
        select_ranked_versions_at::<RS, R>(LATEST_REFERENCE_VERSIONS_CTE, &mut query_builder)?;
        query_builder.push(",");
        ranked_versions_at::<D>(
            LATEST_RECURSION_VERSIONS_CTE,
            &mut query_builder,
            d_defined_on,
        );
        select_ranked_versions_at::<DS, D>(LATEST_RECURSION_VERSIONS_CTE, &mut query_builder)?;
        query_builder.push(",");

        recursive_versions_sql::<D, R>(&mut query_builder, direct_reference_entity, options)?;
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
fn recursive_versions_sql<'a, D, R>(
    query_builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>,
    direct_reference_entity: &'a dyn SqlEntity,
    options: RecursionOptions,
) -> Result<(), TdError>
where
    D: Recursive + Versioned,
    R: DataAccessObject,
{
    let starting_at = R::sql_field_for_type(direct_reference_entity.type_id())?;
    let recursion_ref = R::sql_field_for_type(std::any::TypeId::of::<D::Recursive>())?;
    let recurse_up = D::recurse_up();
    let recurse_down = D::recurse_down();

    //
    // ──────────────────────────────────────────────
    // UPSTREAM RECURSION CTE
    // ──────────────────────────────────────────────
    //
    query_builder.push(format!(
        "upstream_versions AS (
            SELECT
                d.*,
                CAST(d.{recurse_down} AS TEXT) AS path_up,
                1 AS depth_up
            FROM {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN {LATEST_REFERENCE_VERSIONS_CTE} f
                ON f.{recursion_ref} = d.{recurse_down}
            WHERE f.{starting_at} = "
    ));
    direct_reference_entity.push_bind(query_builder);
    query_builder.push(format!(
        " AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fx WHERE fx.{recursion_ref} = d.{recurse_up})"
    ));

    if let Some(max_up) = options.max_upstream_depth {
        query_builder.push(format!(" AND 0 < {max_up} "));
    }

    query_builder.push(format!(
        "
            UNION ALL
            SELECT
                d.*,
                r.path_up || ',' || d.{recurse_up} AS path_up,
                r.depth_up + 1 AS depth_up
            FROM {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN upstream_versions r
                ON r.{recurse_up} = d.{recurse_down}
            WHERE instr(r.path_up, d.{recurse_up}) = 0
              AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fx WHERE fx.{recursion_ref} = d.{recurse_down})
              AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fy WHERE fy.{recursion_ref} = d.{recurse_up})
        "
    ));

    if let Some(max_up) = options.max_upstream_depth {
        query_builder.push(format!(" AND r.depth_up < {max_up} "));
    }

    query_builder.push(") , "); // end of upstream CTE

    //
    // ──────────────────────────────────────────────
    // DOWNSTREAM RECURSION CTE
    // ──────────────────────────────────────────────
    //
    query_builder.push(format!(
        "downstream_versions AS (
            SELECT
                d.*,
                CAST(d.{recurse_up} AS TEXT) AS path_down,
                1 AS depth_down
            FROM {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN {LATEST_REFERENCE_VERSIONS_CTE} f
                ON f.{recursion_ref} = d.{recurse_up}
            WHERE f.{starting_at} = "
    ));
    direct_reference_entity.push_bind(query_builder);
    query_builder.push(format!(
        " AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fx WHERE fx.{recursion_ref} = d.{recurse_down})"
    ));

    if let Some(max_down) = options.max_downstream_depth {
        query_builder.push(format!(" AND 0 < {max_down} "));
    }

    query_builder.push(format!(
        "
            UNION ALL
            SELECT
                d.*,
                r.path_down || ',' || d.{recurse_down} AS path_down,
                r.depth_down + 1 AS depth_down
            FROM {LATEST_RECURSION_VERSIONS_CTE} d
            INNER JOIN downstream_versions r
                ON r.{recurse_down} = d.{recurse_up}
            WHERE instr(r.path_down, d.{recurse_down}) = 0
              AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fx WHERE fx.{recursion_ref} = d.{recurse_down})
              AND EXISTS (SELECT 1 FROM {LATEST_REFERENCE_VERSIONS_CTE} fy WHERE fy.{recursion_ref} = d.{recurse_up})
        "
    ));

    if let Some(max_down) = options.max_downstream_depth {
        query_builder.push(format!(" AND r.depth_down < {max_down} "));
    }

    query_builder.push("), "); // end of downstream CTE

    //
    // ──────────────────────────────────────────────
    // FINAL MERGE QUERY
    // ──────────────────────────────────────────────
    //
    query_builder.push(format!(
        "{RECURSIVE_VERSIONS_CTE} AS (
            SELECT * FROM upstream_versions
            UNION ALL
            SELECT * FROM downstream_versions
        )",
    ));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::dependency::{DependencyDB, DependencyDBWithNames};
    use crate::dxo::function::FunctionDB;
    use crate::dxo::trigger::{TriggerDB, TriggerDBWithNames};
    use crate::sql::{DaoQueries, Insert};
    use crate::types::basic::{AtTime, CollectionId, FunctionId};
    use chrono::{DateTime, Utc};
    use sqlx::Execute;
    use std::sync::LazyLock;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_type::Dao;

    #[td_type::typed(string)]
    struct TestId;

    #[td_type::typed(string)]
    struct TestPartition;

    #[td_type::typed_enum]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[td_type::typed(string)]
    struct TestRecursion;

    // partitioning by id means there are no groups, each entry is independent
    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "test_table",
        versioned(order_by = "defined_on", partition_by = "id"),
        recursive(up = "current", down = "downstream"),
        states(
            Active = &[&TestStatus::Active],
        )
    )]
    struct TestDao {
        id: TestId,
        status: TestStatus,
        current: TestRecursion,
        downstream: TestRecursion,
        defined_on: AtTime,
    }

    #[Dao]
    #[dao(
        sql_table = "test_table_reference",
        versioned(order_by = "defined_on", partition_by = "reference_id"),
        states(
            Active = &[&TestStatus::Active],
        )
    )]
    struct RecursionReference {
        id: TestId,
        reference_id: TestRecursion,
        status: TestStatus,
        defined_on: AtTime,
    }

    fn time(i: usize) -> AtTime {
        AtTime::try_from(
            format!("2025-04-02T08:19:5{i}.543+00:00")
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    }

    static FIXTURE_TEST_DAOS: LazyLock<Vec<TestDao>> = LazyLock::new(|| {
        vec![
            TestDao {
                id: TestId::try_from("AAA").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_0").unwrap(),
                downstream: TestRecursion::try_from("ref_1").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("BBB").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_2").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("CCC").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_3").unwrap(),
                defined_on: time(1),
            },
            TestDao {
                id: TestId::try_from("DDD").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_4").unwrap(),
                defined_on: time(2),
            },
            TestDao {
                id: TestId::try_from("EEE").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursion::try_from("ref_4").unwrap(),
                downstream: TestRecursion::try_from("ref_5").unwrap(),
                defined_on: time(3),
            },
        ]
    });

    static _FIXTURE_RECURSION_REF: LazyLock<Vec<RecursionReference>> = LazyLock::new(|| {
        vec![
            RecursionReference {
                id: TestId::try_from("MMM").unwrap(),
                reference_id: TestRecursion::try_from("ref_0").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("NNN").unwrap(),
                reference_id: TestRecursion::try_from("ref_1").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("OOO").unwrap(),
                reference_id: TestRecursion::try_from("ref_2").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(0),
            },
            RecursionReference {
                id: TestId::try_from("QQQ").unwrap(),
                reference_id: TestRecursion::try_from("ref_3").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(1),
            },
            RecursionReference {
                id: TestId::try_from("PPP").unwrap(),
                reference_id: TestRecursion::try_from("ref_2").unwrap(),
                status: TestStatus::try_from("D").unwrap(),
                defined_on: time(2),
            },
            RecursionReference {
                id: TestId::try_from("RRR").unwrap(),
                reference_id: TestRecursion::try_from("ref_4").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(3),
            },
            RecursionReference {
                id: TestId::try_from("SSS").unwrap(),
                reference_id: TestRecursion::try_from("ref_5").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                defined_on: time(3),
            },
        ]
    });

    #[test]
    fn test_recursive_active_versions_sql() {
        let mut query_builder = sqlx::QueryBuilder::default();
        let id = &TestId::try_from("AAA").unwrap();
        recursive_versions_sql::<TestDao, RecursionReference>(
            &mut query_builder,
            id,
            RecursionOptions {
                max_upstream_depth: None,
                max_downstream_depth: None,
            },
        )
        .unwrap();
        let query = query_builder.build();

        let expected = "upstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.downstream AS TEXT) AS path_up,\
        \n                1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.downstream\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.current)\
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_up || ',' || d.current AS path_up,\
        \n                r.depth_up + 1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN upstream_versions r\
        \n                ON r.current = d.downstream\
        \n            WHERE instr(r.path_up, d.current) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n        ) , downstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.current AS TEXT) AS path_down,\
        \n                1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.current\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_down || ',' || d.downstream AS path_down,\
        \n                r.depth_down + 1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN downstream_versions r\
        \n                ON r.downstream = d.current\
        \n            WHERE instr(r.path_down, d.downstream) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n        ), recursive_versions AS (\
        \n            SELECT * FROM upstream_versions\
        \n            UNION ALL\
        \n            SELECT * FROM downstream_versions\
        \n        )";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_select_active_recursive_versions_some_depth() {
        let mut query_builder = sqlx::QueryBuilder::default();
        let id = &TestId::try_from("AAA").unwrap();
        recursive_versions_sql::<TestDao, RecursionReference>(
            &mut query_builder,
            id,
            RecursionOptions {
                max_upstream_depth: Some(Depth::try_from(2).unwrap()),
                max_downstream_depth: Some(Depth::try_from(3).unwrap()),
            },
        )
        .unwrap();
        let query = query_builder.build();

        let expected = "upstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.downstream AS TEXT) AS path_up,\
        \n                1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.downstream\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.current) AND 0 < 2 \
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_up || ',' || d.current AS path_up,\
        \n                r.depth_up + 1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN upstream_versions r\
        \n                ON r.current = d.downstream\
        \n            WHERE instr(r.path_up, d.current) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n         AND r.depth_up < 2 ) , downstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.current AS TEXT) AS path_down,\
        \n                1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.current\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream) AND 0 < 3 \
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_down || ',' || d.downstream AS path_down,\
        \n                r.depth_down + 1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN downstream_versions r\
        \n                ON r.downstream = d.current\
        \n            WHERE instr(r.path_down, d.downstream) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n         AND r.depth_down < 3 ), recursive_versions AS (\
        \n            SELECT * FROM upstream_versions\
        \n            UNION ALL\
        \n            SELECT * FROM downstream_versions\
        \n        )";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_select_active_recursive_versions_at_none_sql() {
        let id = TestId::try_from("AAA").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id,
            )
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH RECURSIVE latest_reference_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.reference_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.reference_id) AS total_count\
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
        \n                ROW_NUMBER() OVER (PARTITION BY v.id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n         ),latest_recursion_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_recursion_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),upstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.downstream AS TEXT) AS path_up,\
        \n                1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.downstream\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.current) AND 0 < 0 \
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_up || ',' || d.current AS path_up,\
        \n                r.depth_up + 1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN upstream_versions r\
        \n                ON r.current = d.downstream\
        \n            WHERE instr(r.path_up, d.current) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n         AND r.depth_up < 0 ) , downstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.current AS TEXT) AS path_down,\
        \n                1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.current\
        \n            WHERE f.id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_down || ',' || d.downstream AS path_down,\
        \n                r.depth_down + 1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN downstream_versions r\
        \n                ON r.downstream = d.current\
        \n            WHERE instr(r.path_down, d.downstream) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n        ), recursive_versions AS (\
        \n            SELECT * FROM upstream_versions\
        \n            UNION ALL\
        \n            SELECT * FROM downstream_versions\
        \n        ) SELECT DISTINCT id, status, current, downstream, defined_on FROM recursive_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[test]
    fn test_select_active_recursive_versions_at_sql() {
        let at = time(0);
        let id = TestRecursion::try_from("ref_X").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                Some(&at),
                Some(&at),
                &id,
            )
            .unwrap();
        let query = query_builder.build();

        let expected = "WITH RECURSIVE latest_reference_versions_ranked AS (\
        \n            SELECT\
        \n                v.*,\
        \n                ROW_NUMBER() OVER (PARTITION BY v.reference_id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.reference_id) AS total_count\
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
        \n                ROW_NUMBER() OVER (PARTITION BY v.id ORDER BY v.defined_on DESC, v.id DESC) AS rn,\
        \n                COUNT(*) OVER (PARTITION BY v.id) AS total_count\
        \n            FROM\
        \n                test_table v\
        \n        WHERE v.defined_on <= ? ),latest_recursion_versions AS (\
        \n            SELECT\
        \n                rv.*\
        \n            FROM\
        \n                latest_recursion_versions_ranked rv\
        \n            WHERE\
        \n                rv.rn = 1\
        \n        ),upstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.downstream AS TEXT) AS path_up,\
        \n                1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.downstream\
        \n            WHERE f.reference_id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.current) AND 0 < 0 \
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_up || ',' || d.current AS path_up,\
        \n                r.depth_up + 1 AS depth_up\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN upstream_versions r\
        \n                ON r.current = d.downstream\
        \n            WHERE instr(r.path_up, d.current) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n         AND r.depth_up < 0 ) , downstream_versions AS (\
        \n            SELECT\
        \n                d.*,\
        \n                CAST(d.current AS TEXT) AS path_down,\
        \n                1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN latest_reference_versions f\
        \n                ON f.reference_id = d.current\
        \n            WHERE f.reference_id = ? AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n            UNION ALL\
        \n            SELECT\
        \n                d.*,\
        \n                r.path_down || ',' || d.downstream AS path_down,\
        \n                r.depth_down + 1 AS depth_down\
        \n            FROM latest_recursion_versions d\
        \n            INNER JOIN downstream_versions r\
        \n                ON r.downstream = d.current\
        \n            WHERE instr(r.path_down, d.downstream) = 0\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fx WHERE fx.reference_id = d.downstream)\
        \n              AND EXISTS (SELECT 1 FROM latest_reference_versions fy WHERE fy.reference_id = d.current)\
        \n        ), recursive_versions AS (\
        \n            SELECT * FROM upstream_versions\
        \n            UNION ALL\
        \n            SELECT * FROM downstream_versions\
        \n        ) SELECT DISTINCT id, status, current, downstream, defined_on FROM recursive_versions ORDER BY 1 DESC";
        assert_eq!(query.sql(), expected);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    #[tokio::test]
    async fn test_select_active_versions_none_fetch_all(db: DbPool) {
        let id = TestId::try_from("MMM").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id,
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
    #[tokio::test]
    async fn test_select_active_versions_none_fetch_all_upstream(db: DbPool) {
        let id = TestId::try_from("SSS").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at_with::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id, RecursionOptions {
                    max_upstream_depth: None,
                    max_downstream_depth: Some(Depth::ZERO),
                }
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // SSS: EEE -> DDD -> AAA
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    #[tokio::test]
    async fn test_select_active_versions_at_time_fetch_all(db: DbPool) {
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                Some(&at),
                Some(&at),
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
    #[tokio::test]
    async fn test_select_active_versions_break_downstream_by_status_ref(db: DbPool) {
        let at = time(2);
        let id = TestId::try_from("MMM").unwrap();
        // PPP is D at time 2, so it will stop the recursion
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{TestDao::Active}, TestDao, { RecursionReference::Active }, RecursionReference>(
                Some(&at),
                Some(&at),
                &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // MMM: AAA -> CCC (as PPP is D, which should be pointing to BBB)
        assert_eq!(result.len(), 2);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    #[tokio::test]
    async fn test_select_active_versions_none_last_in_stream(db: DbPool) {
        let id = TestId::try_from("SSS").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at_with::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id,RecursionOptions {
                    max_upstream_depth: Some(Depth::try_from(1).unwrap()),
                    max_downstream_depth: None,
                }
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        // SSS: EEE (direct upstream)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    #[tokio::test]
    async fn test_select_active_versions_none_deleted(db: DbPool) {
        // OOO is active, but PPP has the same partition_id and is deleted
        let id = TestId::try_from("OOO").unwrap();
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id,
            )
            .unwrap();
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = "test_recursive"))]
    #[tokio::test]
    async fn test_select_active_versions_heavy(db: DbPool) -> Result<(), TdError> {
        let size: usize = 1000;

        let mut daos = vec![];
        // we need size+1 so the recursion ref of 1000 is valid
        for i in 0..size + 1 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("YYY{i:04}"))?,
                reference_id: TestRecursion::try_from(format!("x_ref_{i:04}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            DaoQueries::default()
                .insert(&reference)?
                .build()
                .execute(&db)
                .await
                .unwrap();
            let dao = TestDao {
                id: TestId::try_from(format!("ZZZ{i:04}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("x_ref_{i:04}"))?,
                downstream: TestRecursion::try_from(format!("x_ref_{:04}", i + 1))?,
                defined_on: time(5),
            };
            DaoQueries::default()
                .insert(&dao)?
                .build()
                .execute(&db)
                .await
                .unwrap();
            daos.push(dao);
        }

        let id = TestId::try_from(format!("YYY{:04}", 0))?;
        let mut query_builder = DaoQueries::default()
            .select_recursive_versions_at::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                None, None, &id,
            )?;
        let result: Vec<TestDao> = query_builder.build_query_as().fetch_all(&db).await.unwrap();
        assert_eq!(result.len(), size);
        for i in 0..size {
            assert_eq!(result[i], daos[size - 1 - i]);
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_select_active_versions_at_types(db: DbPool) -> Result<(), TdError> {
        async fn test_query<const DS: u8, D, const RS: u8, R>(
            db: &DbPool,
            recursion_ref: &dyn SqlEntity,
        ) -> Result<(), TdError>
        where
            D: DataAccessObject + Recursive + Versioned + States<DS>,
            R: DataAccessObject + Versioned + States<RS>,
        {
            let mut query_builder = DaoQueries::default()
                .select_recursive_versions_at::<DS, D, RS, R>(None, None, recursion_ref)?;
            let _result: Vec<D> = query_builder.build_query_as().fetch_all(db).await.unwrap();
            Ok(())
        }

        test_query::<{ DependencyDB::Active }, DependencyDB, { FunctionDB::Available }, FunctionDB>(&db, &FunctionId::default()).await?;
        test_query::<{ DependencyDB::Active }, DependencyDB, { FunctionDB::Available }, FunctionDB>(&db, &CollectionId::default()).await?;
        test_query::<
            { DependencyDBWithNames::Active },
            DependencyDBWithNames,
            { FunctionDB::Available },
            FunctionDB,
        >(&db, &FunctionId::default())
        .await?;
        test_query::<
            { DependencyDBWithNames::Active },
            DependencyDBWithNames,
            { FunctionDB::Available },
            FunctionDB,
        >(&db, &CollectionId::default())
        .await?;

        test_query::<{ TriggerDB::Active }, TriggerDB, { FunctionDB::Available }, FunctionDB>(
            &db,
            &FunctionId::default(),
        )
        .await?;
        test_query::<{ TriggerDB::Active }, TriggerDB, { FunctionDB::Available }, FunctionDB>(
            &db,
            &CollectionId::default(),
        )
        .await?;
        test_query::<
            { TriggerDBWithNames::Active },
            TriggerDBWithNames,
            { FunctionDB::Available },
            FunctionDB,
        >(&db, &FunctionId::default())
        .await?;
        test_query::<
            { TriggerDBWithNames::Active },
            TriggerDBWithNames,
            { FunctionDB::Available },
            FunctionDB,
        >(&db, &CollectionId::default())
        .await?;

        Ok(())
    }
}
