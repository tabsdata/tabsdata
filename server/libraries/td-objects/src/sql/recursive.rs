//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::{DataAccessObject, Recursive, States, Versioned};
use crate::sql::{Queries, gen_where_clause};
use crate::types::SqlEntity;
use crate::types::basic::Depth;
use crate::types::recursion::RecursionOptions;
use std::ops::Deref;
use td_error::TdError;
use tm_sql::sql;

impl<Q: Deref<Target = dyn Queries>> RecursiveQueries for Q {}

pub trait RecursiveQueries {
    fn select_recursive_versions_at<'a, const DS: u8, D, const RS: u8, R>(
        &self,
        d_defined_on: Option<&'a <D as Versioned>::Order>,
        r_defined_on: Option<&'a <R as Versioned>::Order>,
        direct_reference_entity: &'a dyn SqlEntity, // has to be in R
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Recursive + Versioned + States<DS>,
        R: DataAccessObject + Versioned + States<RS>,
        <D as Versioned>::Partition: 'a,
        <R as Versioned>::Partition: 'a,
    {
        self.select_recursive_versions_at_with::<DS, D, RS, R>(
            d_defined_on,
            r_defined_on,
            direct_reference_entity,
            &RecursionOptions::new(Some(Depth::ZERO), None),
        )
    }

    fn select_recursive_versions_at_with<'a, const DS: u8, D, const RS: u8, R>(
        &self,
        d_defined_on: Option<&'a <D as Versioned>::Order>,
        r_defined_on: Option<&'a <R as Versioned>::Order>,
        direct_reference_entity: &'a dyn SqlEntity, // has to be in R
        options: &RecursionOptions,
    ) -> Result<sqlx::QueryBuilder<'a, sqlx::Sqlite>, TdError>
    where
        D: DataAccessObject + Recursive + Versioned + States<DS>,
        R: DataAccessObject + Versioned + States<RS>,
        <D as Versioned>::Partition: 'a,
        <R as Versioned>::Partition: 'a,
    {
        let starting_at = R::sql_field_for_type(direct_reference_entity.type_id())?;
        let recursion_ref = R::sql_field_for_type(std::any::TypeId::of::<D::Recursive>())?;

        let query = sql! {
            "WITH RECURSIVE

            -- referenced versions
            reference_versions AS (
                SELECT rv.*
                FROM " %{R::sql_table()} " rv
                WHERE
                "[ gen_where_clause::<R, _>(&mut builder, R::state())?; ] {}"
                AND id = (
                    SELECT MAX(id)
                    FROM " %{R::sql_table()} " rv2
                    WHERE rv2." %{R::partition_by()} " = rv." %{R::partition_by()} "
                        AND rv2." %{<R as Versioned>::order_by()} " = (
                            SELECT MAX(" %{<R as Versioned>::order_by()} ")
                            FROM " %{R::sql_table()} " rv3
                            WHERE rv3." %{R::partition_by()} " = rv2." %{R::partition_by()}
                                [if let Some(r_defined_on) = r_defined_on] {
                                    " AND rv3." %{<R as Versioned>::order_by()} " <= " {r_defined_on}
                                }"
                    )
                )
            ),

            -- recursion versions
            recursion_versions AS (
                SELECT rv.*
                FROM " %{D::sql_table()} " rv
                WHERE id = (
                    SELECT MAX(id)
                    FROM " %{D::sql_table()} " rv2
                    WHERE rv2." %{D::partition_by()} " = rv." %{D::partition_by()} "
                        AND rv2." %{<D as Versioned>::order_by()} " = (
                            SELECT MAX(" %{<D as Versioned>::order_by()} ")
                            FROM " %{D::sql_table()} " rv3
                            WHERE rv3." %{D::partition_by()} " = rv2." %{D::partition_by()} "
                                "[if let Some(d_defined_on) = d_defined_on] {
                                    " AND rv3." %{<D as Versioned>::order_by()} " <= " {d_defined_on}
                                }"
                    )
                )
                AND "[ gen_where_clause::<D, _>(&mut builder, D::state())?; ] {}"
            ),

            -- upstream recursion
            upstream_versions AS (
                SELECT
                    rcv.*,
                    CAST(rcv." %{D::recurse_down()} " AS TEXT) AS path_up,
                    1 AS depth_up
                FROM recursion_versions rcv
                INNER JOIN reference_versions rfv ON rfv." %{recursion_ref} " = rcv." %{D::recurse_down()} "
                WHERE rfv." %{starting_at} " = " {direct_reference_entity} "
                    AND EXISTS (SELECT 1 FROM reference_versions rfv2 WHERE rfv2." %{recursion_ref} " = rcv." %{D::recurse_up()} ")"
                [if let Some(max_up) = options.max_upstream_depth()] {
                    " AND 0 < " %{max_up}
                }"
                UNION ALL
                SELECT
                    rcv.*,
                    uv.path_up || ',' || rcv." %{D::recurse_up()} " AS path_up,
                    uv.depth_up + 1 AS depth_up
                FROM recursion_versions rcv
                INNER JOIN upstream_versions uv
                    ON uv." %{D::recurse_up()} " = rcv." %{D::recurse_down()} "
                WHERE instr(uv.path_up, rcv." %{D::recurse_up()} ") = 0
                    AND EXISTS (SELECT 1 FROM reference_versions fx WHERE fx." %{recursion_ref} " = rcv." %{D::recurse_down()} ")
                    AND EXISTS (SELECT 1 FROM reference_versions fy WHERE fy." %{recursion_ref} " = rcv." %{D::recurse_up()} ")"
                [if let Some(max_up) = options.max_upstream_depth()] {
                    " AND uv.depth_up < " %{max_up}
                }"
            ),

            -- downstream recursion
            downstream_versions AS (
                SELECT
                    rcv.*,
                    CAST(rcv." %{D::recurse_up()} " AS TEXT) AS path_down,
                    1 AS depth_down
                FROM recursion_versions rcv
                INNER JOIN reference_versions rfv ON rfv." %{recursion_ref} " = rcv." %{D::recurse_up()} "
                WHERE rfv." %{starting_at} " = " {direct_reference_entity} "
                    AND EXISTS (SELECT 1 FROM reference_versions rfv2 WHERE rfv2." %{recursion_ref} " = rcv." %{D::recurse_down()} ")"
                [if let Some(max_down) = options.max_downstream_depth()] {
                    " AND 0 < " %{max_down}
                }"
                UNION ALL
                SELECT
                    rcv.*,
                    dv.path_down || ',' || dv." %{D::recurse_down()} " AS path_down,
                    dv.depth_down + 1 AS depth_down
                FROM recursion_versions rcv
                INNER JOIN downstream_versions dv
                    ON dv." %{D::recurse_down()} " = rcv." %{D::recurse_up()} "
                WHERE instr(dv.path_down, rcv." %{D::recurse_down()} ") = 0
                  AND EXISTS (SELECT 1 FROM reference_versions fx WHERE fx." %{recursion_ref} " = rcv." %{D::recurse_down()} ")
                  AND EXISTS (SELECT 1 FROM reference_versions fy WHERE fy." %{recursion_ref} " = rcv." %{D::recurse_up()} ")"
                [if let Some(max_down) = options.max_downstream_depth()] {
                    " AND dv.depth_down < " %{max_down}
                }"
            ),

            -- final merge
            recursive_versions AS (
                SELECT * FROM upstream_versions
                UNION ALL
                SELECT * FROM downstream_versions
            )
            SELECT DISTINCT " %{D::fields().join(", ")} "
            FROM recursive_versions
            ORDER BY " %{<D as DataAccessObject>::order_by()} "
            "
        };
        Ok(query)
    }
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
            Active = [TestStatus::Active],
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
            Active = [TestStatus::Active],
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
                None, None, &id, &RecursionOptions::new(
                     None,
                     Some(Depth::ZERO),
                )
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
            .select_recursive_versions_at::<{ TestDao::Active }, TestDao, { RecursionReference::Active }, RecursionReference>(
                Some(&at),
                Some(&at),
                &id,
            )
            .unwrap();
        println!("{}", query_builder.sql());
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
                None, None, &id, &RecursionOptions::new(
                    Some(Depth::try_from(1).unwrap()),
                    None
                ))
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
