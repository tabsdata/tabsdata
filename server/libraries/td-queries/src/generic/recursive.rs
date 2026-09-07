//
// Copyright 2026 Tabsdata Inc.
//

use crate::Serves;
use crate::generic::as_of::AsOf;
use crate::generic::scd2::Scd2;
use crate::primitives::recursive::{
    Cte, Start, build_and_execute_recursive, is_direction_enabled, reachable_partitions_cte,
    recursion_versions_cte, reference_state_cond, reference_versions_cte, versions_cte_current,
};
use sea_query::{Condition, WithClause};
use std::marker::PhantomData;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{Dao, HasColumn, RecursiveDao, Scd2Dao, SelectDao, TimelineDao};
use td_objects::types::recursion::RecursionOptions;

/// Recursion over open intervals: one row per partition, no time travel.
pub struct Scd2Recursive<'a, D, R: Dao, E> {
    recursion: RecursionOptions,
    start: Option<Start<'a, R>>,
    edges: Condition,
    nodes: Condition,
    start_filter: Option<Condition>,
    executor: E,
    _phantom: PhantomData<D>,
}

/// Recursion at a point in time, over visible-at interval scans.
pub struct AsOfRecursive<'a, D: TimelineDao, R: Dao, E> {
    recursion: RecursionOptions,
    start: Option<Start<'a, R>>,
    on: &'a D::VersionOrder,
    edges: Condition,
    nodes: Condition,
    start_filter: Option<Condition>,
    executor: E,
    _phantom: PhantomData<D>,
}

impl<E> Scd2<E> {
    /// One row per partition feeds the CTE: no ranking, no time travel.
    pub fn recursive<'a, D, R>(self, recursion: RecursionOptions) -> Scd2Recursive<'a, D, R, E>
    where
        D: SelectDao + Scd2Dao + RecursiveDao,
        R: SelectDao + Scd2Dao,
    {
        Scd2Recursive {
            recursion,
            start: None,
            edges: Condition::all(),
            nodes: Condition::all(),
            start_filter: None,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, D, R, E> Scd2Recursive<'a, D, R, E>
where
    D: SelectDao + Scd2Dao + RecursiveDao,
    R: SelectDao + Scd2Dao + Dao<Db = D::Db> + HasColumn<D::Key>,
    E: Serves<D> + Serves<R>,
{
    pub fn start_at<V: SeaValue>(mut self, value: &'a V) -> Self
    where
        R: HasColumn<V>,
    {
        self.start = Some(Start::of(vec![value]));
        self
    }

    /// The CTE anchors on `start_col IN (..)` and walks from every starting
    /// point in one query.
    pub fn start_at_any<V: SeaValue>(mut self, values: Vec<&'a V>) -> Self
    where
        R: HasColumn<V>,
    {
        self.start = Some(Start::of(values));
        self
    }

    pub fn where_edges(mut self, cond: impl SqlCondition<D>) -> Self {
        self.edges = cond.resolve();
        self
    }

    pub fn where_nodes(mut self, cond: impl SqlCondition<R>) -> Self {
        self.nodes = cond.resolve();
        self
    }

    /// Starting rows match `nodes` or this.
    pub fn where_start(mut self, cond: impl SqlCondition<R>) -> Self {
        self.start_filter = Some(cond.resolve());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<D>, SqlError> {
        let up_enabled = is_direction_enabled(self.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(self.recursion.max_downstream_depth());

        let Some(start) = self.start else {
            return Ok(vec![]);
        };
        if !up_enabled && !down_enabled {
            return Ok(vec![]);
        }

        let nodes = reference_state_cond(&start, &self.nodes, self.start_filter.as_ref());

        let with = WithClause::new()
            .recursive(true)
            .cte(versions_cte_current::<R>(nodes, Cte::ReferenceVersions))
            .cte(versions_cte_current::<D>(
                self.edges,
                Cte::RecursionVersions,
            ))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self.executor,
            with,
            &self.recursion,
            &start,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

impl<'a, E, V> AsOf<'a, E, V> {
    /// Visible-at base CTEs over both DAOs, bounded to the reachable
    /// partitions.
    pub fn recursive<D, R>(self, recursion: RecursionOptions) -> AsOfRecursive<'a, D, R, E>
    where
        D: SelectDao + Scd2Dao<VersionOrder = V> + RecursiveDao,
        R: SelectDao + Scd2Dao<VersionOrder = V>,
    {
        AsOfRecursive {
            recursion,
            start: None,
            on: self.on,
            edges: Condition::all(),
            nodes: Condition::all(),
            start_filter: None,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, D, R, E> AsOfRecursive<'a, D, R, E>
where
    D: SelectDao + Scd2Dao + RecursiveDao,
    R: SelectDao + Scd2Dao<VersionOrder = D::VersionOrder> + Dao<Db = D::Db> + HasColumn<D::Key>,
    E: Serves<D> + Serves<R>,
{
    pub fn start_at<V: SeaValue>(mut self, value: &'a V) -> Self
    where
        R: HasColumn<V>,
    {
        self.start = Some(Start::of(vec![value]));
        self
    }

    /// The CTE anchors on `start_col IN (..)` and walks from every starting
    /// point in one query.
    pub fn start_at_any<V: SeaValue>(mut self, values: Vec<&'a V>) -> Self
    where
        R: HasColumn<V>,
    {
        self.start = Some(Start::of(values));
        self
    }

    pub fn where_edges(mut self, cond: impl SqlCondition<D>) -> Self {
        self.edges = cond.resolve();
        self
    }

    pub fn where_nodes(mut self, cond: impl SqlCondition<R>) -> Self {
        self.nodes = cond.resolve();
        self
    }

    /// Starting rows match `nodes` or this.
    pub fn where_start(mut self, cond: impl SqlCondition<R>) -> Self {
        self.start_filter = Some(cond.resolve());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<D>, SqlError> {
        let up_enabled = is_direction_enabled(self.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(self.recursion.max_downstream_depth());

        let Some(start) = self.start else {
            return Ok(vec![]);
        };
        if !up_enabled && !down_enabled {
            return Ok(vec![]);
        }

        let with = WithClause::new()
            .recursive(true)
            .cte(reachable_partitions_cte::<D, R, _>(
                &start,
                self.on,
                up_enabled,
                down_enabled,
            ))
            .cte(reference_versions_cte::<D, R, _>(
                &start,
                self.on,
                &self.nodes,
                self.start_filter.as_ref(),
            ))
            .cte(recursion_versions_cte::<D, _>(self.on, &self.edges))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self.executor,
            with,
            &self.recursion,
            &start,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Queries;
    use crate::primitives::recursive::versions_cte_basic;
    use crate::primitives::recursive::{
        Direction, direction_versions_cte, recursive_versions_cte, versioned_visible_cte,
    };
    use chrono::{DateTime, Utc};
    use sea_query::{Asterisk, Query, WithClause};
    use std::sync::LazyLock;
    use ta_objects::Dao;
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::dependency::{DependencyDB, DependencyDBWithNames};
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::trigger::{TriggerDB, TriggerDBWithNames};
    use td_objects::types::basic::{AtTime, CollectionId, Depth, FunctionId};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct TestId(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct TestRecursion(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    #[typed(try_from = TestRecursion)]
    struct TestRecursionUp(String);

    // partitioning by id means there are no groups, each entry is independent
    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "test_table", ops(select, insert), scd2, recursive)]
    struct TestDao {
        #[dao(pk, partition_by)]
        id: TestId,
        status: TestStatus,
        #[dao(recursive(up))]
        current: TestRecursionUp,
        #[dao(recursive(down))]
        downstream: TestRecursion,
        #[dao(scd2(valid_from))]
        defined_on: AtTime,
    }

    impl TestDao {
        pub fn active() -> impl SqlCondition<Self> {
            TestStatus::Active
        }
    }

    #[Dao]
    #[dao(table = "test_table_reference", ops(select, insert), scd2)]
    struct RecursionReference {
        #[dao(pk)]
        id: TestId,
        #[dao(partition_by)]
        reference_id: TestRecursion,
        status: TestStatus,
        #[dao(scd2(valid_from))]
        defined_on: AtTime,
    }

    impl RecursionReference {
        pub fn active() -> impl SqlCondition<Self> {
            TestStatus::Active
        }
    }

    fn time(i: usize) -> AtTime {
        AtTime::from(
            format!("2025-04-02T08:19:5{i}.543+00:00")
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
    }

    /// Time later than any fixture or test insert — safe default for
    /// snapshot tests that don't care about the upper bound.
    static LATER: LazyLock<AtTime> = LazyLock::new(|| time(9));

    static FIXTURE_TEST_DAOS: LazyLock<Vec<TestDao>> = LazyLock::new(|| {
        vec![
            TestDao {
                id: TestId::try_from("AAA").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursionUp::try_from("ref_0").unwrap(),
                downstream: TestRecursion::try_from("ref_1").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("BBB").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursionUp::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_2").unwrap(),
                defined_on: time(0),
            },
            TestDao {
                id: TestId::try_from("CCC").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursionUp::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_3").unwrap(),
                defined_on: time(1),
            },
            TestDao {
                id: TestId::try_from("DDD").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursionUp::try_from("ref_1").unwrap(),
                downstream: TestRecursion::try_from("ref_4").unwrap(),
                defined_on: time(2),
            },
            TestDao {
                id: TestId::try_from("EEE").unwrap(),
                status: TestStatus::try_from("A").unwrap(),
                current: TestRecursionUp::try_from("ref_4").unwrap(),
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
    fn test_is_direction_enabled_none_means_unlimited() {
        assert!(is_direction_enabled(None));
    }

    #[test]
    fn test_is_direction_enabled_zero_means_disabled() {
        assert!(!is_direction_enabled(Some(Depth::ZERO)));
    }

    #[test]
    fn test_is_direction_enabled_positive_means_enabled() {
        assert!(is_direction_enabled(Some(Depth::try_from(1).unwrap())));
        assert!(is_direction_enabled(Some(Depth::try_from(5).unwrap())));
    }

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "test_basic_table", ops(select, insert, delete), recursive)]
    struct BasicTestDao {
        id: TestId,
        #[dao(recursive(up))]
        current: TestRecursionUp,
        #[dao(recursive(down))]
        downstream: TestRecursion,
    }

    #[Dao]
    #[dao(table = "test_basic_reference", ops(select, insert, delete))]
    struct BasicReference {
        id: TestId,
        reference_id: TestRecursion,
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_both_directions_disabled(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::ZERO),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_both_directions_unlimited(db: TdPool) {
        let id = TestId::try_from("RRR").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Upstream: DDD(ref_1→ref_4) → AAA(ref_0→ref_1)
        // Downstream: EEE(ref_4→ref_5)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_none_fetch_all(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
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

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_none_fetch_all_upstream(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // SSS: EEE -> DDD -> AAA
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_upstream_only_downstream_disabled(db: TdPool) {
        let id = TestId::try_from("RRR").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // RRR → ref_4. Upstream: DDD (current=ref_1, downstream=ref_4) → AAA (current=ref_0, downstream=ref_1)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_downstream_only_upstream_disabled(db: TdPool) {
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // NNN → ref_1. Downstream: BBB(ref_1→ref_2), CCC(ref_1→ref_3), DDD(ref_1→ref_4)
        // Then DDD→EEE(ref_4→ref_5)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[2], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[3], FIXTURE_TEST_DAOS[1]); // BBB
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_downstream_depth_limit_1(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::try_from(1).unwrap()),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Depth 1: AAA only (base case from ref_0)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_downstream_depth_limit_2(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::try_from(2).unwrap()),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Depth 1: AAA. Depth 2: BBB, CCC, DDD. Depth 3 blocked.
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[1], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[2], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[3], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_upstream_depth_limit_2(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(2).unwrap()),
                Some(Depth::ZERO),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Depth 1: EEE. Depth 2: DDD. Depth 3 blocked.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_none_last_in_stream(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // SSS: EEE (direct upstream)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_both_directions_with_depth_limits(db: TdPool) {
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                Some(Depth::try_from(1).unwrap()),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Upstream depth=1: AAA (downstream=ref_1)
        // Downstream depth=1: BBB, CCC, DDD (current=ref_1)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[1], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[2], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[3], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_asymmetric_depth_limits(db: TdPool) {
        // NNN is at ref_1 in the middle. Use upstream=1, downstream=2
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                Some(Depth::try_from(2).unwrap()),
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // Upstream depth=1: AAA (downstream=ref_1)
        // Downstream depth=1: BBB, CCC, DDD (current=ref_1)
        // Downstream depth=2: EEE (current=ref_4, from DDD downstream=ref_4)
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[2], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[3], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[4], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_at_time_fetch_all(db: TdPool) {
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&at)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // MMM: AAA -> BBB -> CCC (the rest didn't exist yet)
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[1]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_at_time_before_any_data(db: TdPool) {
        // Use a time before all fixture data was inserted
        let early = AtTime::from(
            "2020-01-01T00:00:00.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        );
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&early)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_break_downstream_by_status_ref(db: TdPool) {
        let at = time(2);
        let id = TestId::try_from("MMM").unwrap();
        // PPP is D at time 2, so it will stop the recursion
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&at)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .where_edges(TestDao::active())
            .where_nodes(RecursionReference::active())
            .fetch_all()
            .await
            .unwrap();
        // MMM: AAA -> CCC (as PPP is D, which should be pointing to BBB)
        assert_eq!(result.len(), 2);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_none_deleted(db: TdPool) {
        // OOO is active, but PPP has the same partition_id and is deleted
        let id = TestId::try_from("OOO").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_direct_reference_state_relaxes_only_entry_point(
        db: TdPool,
    ) -> Result<(), TdError> {
        // Deleted reference DEL → ref_del. Active filter excludes it.
        let deleted_ref = RecursionReference {
            id: TestId::try_from("DEL")?,
            reference_id: TestRecursion::try_from("ref_del")?,
            status: TestStatus::try_from("D")?,
            defined_on: time(5),
        };
        db.scd2().insert(&deleted_ref).await?;
        let target_ref = RecursionReference {
            id: TestId::try_from("TGT")?,
            reference_id: TestRecursion::try_from("ref_tgt")?,
            status: TestStatus::try_from("A")?,
            defined_on: time(5),
        };
        db.scd2().insert(&target_ref).await?;
        let dao = TestDao {
            id: TestId::try_from("OV1")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursionUp::try_from("ref_del")?,
            downstream: TestRecursion::try_from("ref_tgt")?,
            defined_on: time(5),
        };
        db.scd2().insert(&dao).await?;

        let id = TestId::try_from("DEL")?;
        // Without start_filter: DEL is deleted, Active filter excludes it.
        let empty: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .where_edges(TestDao::active())
            .where_nodes(RecursionReference::active())
            .fetch_all()
            .await?;
        assert_eq!(empty.len(), 0);

        // With start_filter(All): the entry point passes regardless of state.
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .where_edges(TestDao::active())
            .where_nodes(RecursionReference::active())
            .where_start(())
            .fetch_all()
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("OV1")?);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_deleted_dao_excluded_from_recursion(db: TdPool) -> Result<(), TdError> {
        // Insert a deleted DAO in the middle of a chain to verify the
        // recursion_versions CTE filters on D-side state, not just R-side.
        // Chain: ref_ds0 → ref_ds1 → ref_ds2, but the DAO at ref_ds1 is deleted.
        for i in 0..3u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("DS{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_ds{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            db.scd2().insert(&reference).await?;
        }
        let dao_a = TestDao {
            id: TestId::try_from("DSA")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursionUp::try_from("ref_ds0")?,
            downstream: TestRecursion::try_from("ref_ds1")?,
            defined_on: time(5),
        };
        db.scd2().insert(&dao_a).await?;
        // This DAO is deleted — should be excluded by Active filter on D
        let dao_b = TestDao {
            id: TestId::try_from("DSB")?,
            status: TestStatus::try_from("D")?,
            current: TestRecursionUp::try_from("ref_ds1")?,
            downstream: TestRecursion::try_from("ref_ds2")?,
            defined_on: time(5),
        };
        db.scd2().insert(&dao_b).await?;

        let id = TestId::try_from("DS0")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .where_edges(TestDao::active())
            .where_nodes(RecursionReference::active())
            .fetch_all()
            .await?;
        // DSA is active and found; DSB is deleted so the chain stops — ref_ds2 is never reached
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("DSA")?);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_nonexistent_reference(db: TdPool) {
        let id = TestId::try_from("XXX").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // XXX doesn't exist in reference table
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_downstream_from_leaf_node(db: TdPool) {
        let id = TestId::try_from("QQQ").unwrap();
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await
            .unwrap();
        // ref_3 is a leaf: no dao has current=ref_3, so base case is empty
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_single_node_no_recursion_neighbors(db: TdPool) -> Result<(), TdError> {
        // Create an isolated node with no edges leading anywhere
        let reference = RecursionReference {
            id: TestId::try_from("ISO")?,
            reference_id: TestRecursion::try_from("ref_iso")?,
            status: TestStatus::try_from("A")?,
            defined_on: time(5),
        };
        db.scd2().insert(&reference).await?;
        let dao = TestDao {
            id: TestId::try_from("IF0")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursionUp::try_from("ref_iso")?,
            downstream: TestRecursion::try_from("ref_nowhere")?,
            defined_on: time(5),
        };
        db.scd2().insert(&dao).await?;

        let id = TestId::try_from("ISO")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .start_at(&id)
            .fetch_all()
            .await?;
        // ref_nowhere doesn't have a reference entry, so downstream base case fails EXISTS check
        // No upstream edges point to ref_iso either
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // Create a 4-node cycle: CF0→CF1→CF2→CF3→(back to ref_c0)
        for i in 0..4u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("CC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_c{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            db.scd2().insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("CF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursionUp::try_from(format!("ref_c{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_c{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            db.scd2().insert(&dao).await?;
        }

        let id = TestId::try_from("CC0")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await?;
        // CF0→CF1→CF2→CF3→(cycle back to ref_c0 blocked)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, TestId::try_from("CF2")?);
        assert_eq!(result[1].id, TestId::try_from("CF1")?);
        assert_eq!(result[2].id, TestId::try_from("CF0")?);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_upstream_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // Create 3-node cycle for upstream traversal
        for i in 0..3u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("UC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_uc{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            db.scd2().insert(&reference).await?;
        }
        for i in 0..3u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("UF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursionUp::try_from(format!("ref_uc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_uc{}", (i + 1) % 3))?,
                defined_on: time(5),
            };
            db.scd2().insert(&dao).await?;
        }

        let id = TestId::try_from("UC0")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .start_at(&id)
            .fetch_all()
            .await?;
        // Upstream from ref_uc0: UF2(downstream=ref_uc0) → UF1(downstream=ref_uc2) → cycle blocked
        assert_eq!(result.len(), 2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_bidirectional_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // 4-node cycle, query with both directions enabled
        for i in 0..4u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("BC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_bc{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            db.scd2().insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("BF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursionUp::try_from(format!("ref_bc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_bc{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            db.scd2().insert(&dao).await?;
        }

        let id = TestId::try_from("BC0")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .start_at(&id)
            .fetch_all()
            .await?;
        // Both directions should terminate without infinite loop
        // Downstream: BF0→BF1→BF2→(cycle back blocked)
        // Upstream: BF3(downstream=ref_bc0)→BF2(downstream=ref_bc3)→(cycle back blocked)
        assert!(result.len() <= 4);
        assert!(!result.is_empty());
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_diamond_deduplication(db: TdPool) -> Result<(), TdError> {
        // Create diamond: ref_d0 → ref_d1, ref_d0 → ref_d2, ref_d1 → ref_d3, ref_d2 → ref_d3, ref_d3 → ref_d4
        for i in 0..5u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("D0{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_d{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            db.scd2().insert(&reference).await?;
        }
        let diamond_edges: Vec<(&str, &str, &str)> = vec![
            ("DA1", "ref_d0", "ref_d1"),
            ("DB1", "ref_d0", "ref_d2"),
            ("DC1", "ref_d1", "ref_d3"),
            ("DD1", "ref_d2", "ref_d3"),
            ("DE1", "ref_d3", "ref_d4"),
        ];
        for (id, current, downstream) in &diamond_edges {
            let dao = TestDao {
                id: TestId::try_from(*id)?,
                status: TestStatus::try_from("A")?,
                current: TestRecursionUp::try_from(*current)?,
                downstream: TestRecursion::try_from(*downstream)?,
                defined_on: time(5),
            };
            db.scd2().insert(&dao).await?;
        }

        let id = TestId::try_from("D00")?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await?;
        // DE1 reached via two paths but DISTINCT deduplicates
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].id, TestId::try_from("DE1")?);
        assert_eq!(result[1].id, TestId::try_from("DD1")?);
        assert_eq!(result[2].id, TestId::try_from("DC1")?);
        assert_eq!(result[3].id, TestId::try_from("DB1")?);
        assert_eq!(result[4].id, TestId::try_from("DA1")?);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_select_active_versions_at_types(db: TdPool) -> Result<(), TdError> {
        use ta_objects::ReachableFrom;
        use td_database::access::InProject;
        use td_database::domain::Apiserver;
        async fn test_query<D, R, V>(
            db: &TdPool,
            filter: impl SqlCondition<D>,
            nodes: impl SqlCondition<R>,
            recursion_ref: &V,
        ) -> Result<(), TdError>
        where
            D: SelectDao
                + Scd2Dao<VersionOrder = AtTime>
                + RecursiveDao
                + Dao<Db = Apiserver>
                + ReachableFrom<InProject>,
            R: SelectDao
                + Scd2Dao<VersionOrder = AtTime>
                + Dao<Db = Apiserver>
                + ReachableFrom<InProject>
                + HasColumn<D::Key>
                + HasColumn<V>,
            V: SeaValue,
        {
            let _result: Vec<D> = db
                .scd2()
                .as_of(&*LATER)
                .recursive::<D, R>(RecursionOptions::new(Some(Depth::ZERO), None))
                .start_at(recursion_ref)
                .where_edges(filter)
                .where_nodes(nodes)
                .fetch_all()
                .await?;
            Ok(())
        }

        test_query::<DependencyDB, FunctionDB, _>(&db, (), (), &FunctionId::default()).await?;
        test_query::<DependencyDB, FunctionDB, _>(&db, (), (), &CollectionId::default()).await?;
        test_query::<DependencyDBWithNames, FunctionDB, _>(&db, (), (), &FunctionId::default())
            .await?;
        test_query::<DependencyDBWithNames, FunctionDB, _>(&db, (), (), &CollectionId::default())
            .await?;

        test_query::<TriggerDB, FunctionDB, _>(
            &db,
            TriggerDB::active(),
            (),
            &FunctionId::default(),
        )
        .await?;
        test_query::<TriggerDB, FunctionDB, _>(
            &db,
            TriggerDB::active(),
            (),
            &CollectionId::default(),
        )
        .await?;
        test_query::<TriggerDBWithNames, FunctionDB, _>(
            &db,
            TriggerDBWithNames::active(),
            (),
            &FunctionId::default(),
        )
        .await?;
        test_query::<TriggerDBWithNames, FunctionDB, _>(
            &db,
            TriggerDBWithNames::active(),
            (),
            &CollectionId::default(),
        )
        .await?;

        Ok(())
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_heavy(db: TdPool) -> Result<(), TdError> {
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
            db.scd2().insert(&reference).await?;
            let dao = TestDao {
                id: TestId::try_from(format!("ZZZ{i:04}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursionUp::try_from(format!("x_ref_{i:04}"))?,
                downstream: TestRecursion::try_from(format!("x_ref_{:04}", i + 1))?,
                defined_on: time(5),
            };
            db.scd2().insert(&dao).await?;
            daos.push(dao);
        }

        let id = TestId::try_from(format!("YYY{:04}", 0))?;
        let result: Vec<TestDao> = db
            .scd2()
            .as_of(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .start_at(&id)
            .fetch_all()
            .await?;
        assert_eq!(result.len(), size);
        for i in 0..size {
            assert_eq!(result[i], daos[size - 1 - i]);
        }
        Ok(())
    }

    mod sql {
        use super::*;
        use crate::primitives::recursive::{Start, reachable_partitions_cte};
        use sea_query::PostgresQueryBuilder;

        #[test]
        fn test_reachable_partitions_cte_emits_recursive_walk() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reachable_partitions_cte::<TestDao, RecursionReference, _>(
                &Start::of(vec![&id]),
                &*LATER,
                true,
                true,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Reachable)
                .to_owned()
                .with(WithClause::new().recursive(true).cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH RECURSIVE "reachable" AS (SELECT "rv"."reference_id" AS "partition_id" FROM "test_table_reference" AS "rv" WHERE "rv"."id" IN ('MMM') AND "rv"."defined_on" <= '2025-04-02 08:19:59.543000 +00:00' UNION (SELECT (CASE WHEN ("rcv"."downstream" = "fx"."partition_id") THEN "rcv"."current" ELSE "rcv"."downstream" END) AS "partition_id" FROM "test_table" AS "rcv" INNER JOIN "reachable" AS "fx" ON "rcv"."downstream" = "fx"."partition_id" OR "rcv"."current" = "fx"."partition_id" WHERE "rcv"."defined_on" <= '2025-04-02 08:19:59.543000 +00:00')) SELECT * FROM "reachable""#
            );
        }

        #[test]
        fn test_reachable_partitions_cte_only_upstream() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reachable_partitions_cte::<TestDao, RecursionReference, _>(
                &Start::of(vec![&id]),
                &*LATER,
                true,
                false,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Reachable)
                .to_owned()
                .with(WithClause::new().recursive(true).cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH RECURSIVE "reachable" AS (SELECT "rv"."reference_id" AS "partition_id" FROM "test_table_reference" AS "rv" WHERE "rv"."id" IN ('MMM') AND "rv"."defined_on" <= '2025-04-02 08:19:59.543000 +00:00' UNION (SELECT "rcv"."current" AS "partition_id" FROM "test_table" AS "rcv" INNER JOIN "reachable" AS "fx" ON "rcv"."downstream" = "fx"."partition_id" WHERE "rcv"."defined_on" <= '2025-04-02 08:19:59.543000 +00:00')) SELECT * FROM "reachable""#
            );
        }

        #[test]
        fn test_reference_versions_cte_filtered_by_reachable() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reference_versions_cte::<TestDao, RecursionReference, _>(
                &Start::of(vec![&id]),
                &*LATER,
                &Condition::all(),
                None,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::ReferenceVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "reference_versions" AS (SELECT "rv".* FROM "test_table_reference" AS "rv" WHERE "rv"."defined_on" <= '2025-04-02 08:19:59.543000 +00:00' AND "rv"."valid_to" > '2025-04-02 08:19:59.543000 +00:00' AND "rv"."reference_id" IN (SELECT "partition_id" FROM "reachable")) SELECT * FROM "reference_versions""#
            );
        }

        #[test]
        fn test_versions_cte_basic() {
            let cte = versions_cte_basic::<BasicTestDao>(Cte::ReferenceVersions);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::ReferenceVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "reference_versions" AS (SELECT "rv".* FROM "test_basic_table" AS "rv") SELECT * FROM "reference_versions""#
            );
        }

        #[test]
        fn test_versioned_visible_cte_with_defined_on() {
            let state_cond =
                ta_objects::sql_condition::SqlCondition::<TestDao>::resolve(&TestStatus::Active);
            let on = AtTime::from(
                "2025-04-02T08:19:50.543+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            );
            let cte = versioned_visible_cte::<TestDao, _>(
                &on,
                state_cond,
                Condition::all(),
                Cte::RecursionVersions,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursionVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "recursion_versions" AS (SELECT "rv".* FROM "test_table" AS "rv" WHERE "rv"."defined_on" <= '2025-04-02 08:19:50.543000 +00:00' AND "rv"."valid_to" > '2025-04-02 08:19:50.543000 +00:00' AND "status" = 'A') SELECT * FROM "recursion_versions""#
            );
        }

        #[test]
        fn test_direction_versions_cte_upstream() {
            let recursion = RecursionOptions::new(None, Some(Depth::ZERO));
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &Start::of(vec![&direct_ref]),
                Direction::Upstream,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::UpstreamVersions)
                .to_owned()
                .with(
                    WithClause::new()
                        .recursive(true)
                        .cte(versions_cte_basic::<BasicReference>(Cte::ReferenceVersions))
                        .cte(versions_cte_basic::<BasicTestDao>(Cte::RecursionVersions))
                        .cte(cte)
                        .to_owned(),
                )
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH RECURSIVE "reference_versions" AS (SELECT "rv".* FROM "test_basic_reference" AS "rv") , "recursion_versions" AS (SELECT "rv".* FROM "test_basic_table" AS "rv") , "upstream_versions" AS (SELECT "rcv".*, 1 AS "depth_up", CONCAT(',', CAST("rcv"."downstream" AS TEXT), ',') AS "path_up" FROM "recursion_versions" AS "rcv" INNER JOIN "reference_versions" AS "rfv" ON "rfv"."reference_id" = "rcv"."downstream" WHERE "rfv"."id" IN ('MMM') AND EXISTS(SELECT 1 FROM "reference_versions" AS "rfv2" WHERE "rfv2"."reference_id" = "rcv"."current") UNION ALL (SELECT "rcv".*, "uv"."depth_up" + 1 AS "depth_up", CONCAT("uv"."path_up", CAST("rcv"."current" AS TEXT), ',') AS "path_up" FROM "recursion_versions" AS "rcv" INNER JOIN "upstream_versions" AS "uv" ON "uv"."current" = "rcv"."downstream" WHERE EXISTS(SELECT 1 FROM "reference_versions" AS "fx" WHERE "fx"."reference_id" = "rcv"."downstream") AND EXISTS(SELECT 1 FROM "reference_versions" AS "fy" WHERE "fy"."reference_id" = "rcv"."current") AND STRPOS("uv"."path_up", CONCAT(',', "rcv"."current", ',')) = 0)) SELECT * FROM "upstream_versions""#
            );
        }

        #[test]
        fn test_direction_versions_cte_downstream() {
            let recursion = RecursionOptions::new(Some(Depth::ZERO), None);
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &Start::of(vec![&direct_ref]),
                Direction::Downstream,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::DownstreamVersions)
                .to_owned()
                .with(
                    WithClause::new()
                        .recursive(true)
                        .cte(versions_cte_basic::<BasicReference>(Cte::ReferenceVersions))
                        .cte(versions_cte_basic::<BasicTestDao>(Cte::RecursionVersions))
                        .cte(cte)
                        .to_owned(),
                )
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH RECURSIVE "reference_versions" AS (SELECT "rv".* FROM "test_basic_reference" AS "rv") , "recursion_versions" AS (SELECT "rv".* FROM "test_basic_table" AS "rv") , "downstream_versions" AS (SELECT "rcv".*, 1 AS "depth_down", CONCAT(',', CAST("rcv"."current" AS TEXT), ',') AS "path_down" FROM "recursion_versions" AS "rcv" INNER JOIN "reference_versions" AS "rfv" ON "rfv"."reference_id" = "rcv"."current" WHERE "rfv"."id" IN ('MMM') AND EXISTS(SELECT 1 FROM "reference_versions" AS "rfv2" WHERE "rfv2"."reference_id" = "rcv"."downstream") UNION ALL (SELECT "rcv".*, "dv"."depth_down" + 1 AS "depth_down", CONCAT("dv"."path_down", CAST("dv"."downstream" AS TEXT), ',') AS "path_down" FROM "recursion_versions" AS "rcv" INNER JOIN "downstream_versions" AS "dv" ON "dv"."downstream" = "rcv"."current" WHERE EXISTS(SELECT 1 FROM "reference_versions" AS "fx" WHERE "fx"."reference_id" = "rcv"."downstream") AND EXISTS(SELECT 1 FROM "reference_versions" AS "fy" WHERE "fy"."reference_id" = "rcv"."current") AND STRPOS("dv"."path_down", CONCAT(',', "rcv"."downstream", ',')) = 0)) SELECT * FROM "downstream_versions""#
            );
        }

        #[test]
        fn test_direction_versions_cte_with_depth_limit() {
            let recursion =
                RecursionOptions::new(Some(Depth::ZERO), Some(Depth::try_from(3).unwrap()));
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &Start::of(vec![&direct_ref]),
                Direction::Downstream,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::DownstreamVersions)
                .to_owned()
                .with(
                    WithClause::new()
                        .recursive(true)
                        .cte(versions_cte_basic::<BasicReference>(Cte::ReferenceVersions))
                        .cte(versions_cte_basic::<BasicTestDao>(Cte::RecursionVersions))
                        .cte(cte)
                        .to_owned(),
                )
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH RECURSIVE "reference_versions" AS (SELECT "rv".* FROM "test_basic_reference" AS "rv") , "recursion_versions" AS (SELECT "rv".* FROM "test_basic_table" AS "rv") , "downstream_versions" AS (SELECT "rcv".*, 1 AS "depth_down", CONCAT(',', CAST("rcv"."current" AS TEXT), ',') AS "path_down" FROM "recursion_versions" AS "rcv" INNER JOIN "reference_versions" AS "rfv" ON "rfv"."reference_id" = "rcv"."current" WHERE "rfv"."id" IN ('MMM') AND EXISTS(SELECT 1 FROM "reference_versions" AS "rfv2" WHERE "rfv2"."reference_id" = "rcv"."downstream") UNION ALL (SELECT "rcv".*, "dv"."depth_down" + 1 AS "depth_down", CONCAT("dv"."path_down", CAST("dv"."downstream" AS TEXT), ',') AS "path_down" FROM "recursion_versions" AS "rcv" INNER JOIN "downstream_versions" AS "dv" ON "dv"."downstream" = "rcv"."current" WHERE EXISTS(SELECT 1 FROM "reference_versions" AS "fx" WHERE "fx"."reference_id" = "rcv"."downstream") AND EXISTS(SELECT 1 FROM "reference_versions" AS "fy" WHERE "fy"."reference_id" = "rcv"."current") AND STRPOS("dv"."path_down", CONCAT(',', "rcv"."downstream", ',')) = 0 AND "dv"."depth_down" < 3)) SELECT * FROM "downstream_versions""#
            );
        }

        #[test]
        fn test_recursive_versions_cte_both() {
            let cte = recursive_versions_cte(true, true);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursiveVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "recursive_versions" AS (SELECT * FROM "upstream_versions" UNION ALL (SELECT * FROM "downstream_versions")) SELECT * FROM "recursive_versions""#
            );
        }

        #[test]
        fn test_recursive_versions_cte_upstream_only() {
            let cte = recursive_versions_cte(true, false);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursiveVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "recursive_versions" AS (SELECT * FROM "upstream_versions") SELECT * FROM "recursive_versions""#
            );
        }

        #[test]
        fn test_recursive_versions_cte_downstream_only() {
            let cte = recursive_versions_cte(false, true);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursiveVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"WITH "recursive_versions" AS (SELECT * FROM "downstream_versions") SELECT * FROM "recursive_versions""#
            );
        }
    }
}
