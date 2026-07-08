//
// Copyright 2026 Tabs Data Inc.
//

use crate::QueryExecutor;
use crate::generic::current::Current;
use crate::generic::historic::Historic;
use crate::primitives::recursive::{
    Cte, build_and_execute_recursive, is_direction_enabled, reachable_partitions_cte,
    recursion_versions_cte, reference_versions_cte, versions_cte_basic, versions_cte_current,
};
use sea_query::{Condition, Expr, ExprTrait, IntoTableRef, WithClause};
use std::marker::PhantomData;
use ta_objects::SeaValue;
use ta_objects::StateFilter;
use ta_objects::errors::SqlError;
use ta_objects::{CurrentDao, Dao, DaoCore, Recursive, VersionedDao};
use td_database::domain::Domained;
use td_objects::types::recursion::RecursionOptions;

query_handle! {
    /// Entry trait providing top-level [`recursive`](RecursiveEntry::recursive)
    /// for non-versioned tables. Versioned recursion goes through
    /// [`Current::recursive`] / [`Historic::recursive`] instead.
    pub trait RecursiveEntry {
        fn recursive<'a, D, R>(self, recursion: RecursionOptions) -> RecursiveQuery<'a, D, R, Self>
        where
            D: Dao + Recursive,
            R: Dao,
        {
            RecursiveQuery {
                recursion,
                direct_references: vec![],
                executor: self,
                _phantom: PhantomData,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Three recursion surfaces, sharing the same `WITH RECURSIVE` walker
// but feeding it different base CTEs, with cost models matching the
// parent surface:
//
// - top-level [`RecursiveEntry::recursive`] for plain `Dao` tables —
//   walks `SELECT * FROM table`. Cost O(table). Used for tables that
//   ARE the data (e.g. physical_function_runs).
// - [`Current::recursive`] for `CurrentDao` recursion at "now" — walks
//   `SELECT * FROM _current_view`. Cost O(partitions).
// - [`Historic::recursive`] for `VersionedDao` recursion at point in
//   time — walks `versioned_ranked_cte`. Cost O(table). Slow path,
//   opt-in by name for time-pinned reads.
// ---------------------------------------------------------------------------

/// Live recursion over plain `Dao` tables. The CTE input is
/// `SELECT * FROM table`. Reach via [`RecursiveEntry::recursive`].
pub struct RecursiveQuery<'a, D, R, E> {
    recursion: RecursionOptions,
    direct_references: Vec<&'a dyn SeaValue>,
    executor: E,
    _phantom: PhantomData<(D, R)>,
}

/// Recursion over `CurrentDao` `_current` views — one row per
/// partition, no ranking, no time travel. Reach via
/// [`Current::recursive`]. Carries `d_state` / `r_state` /
/// `direct_reference_state` for state filtering on the live snapshot.
pub struct CurrentRecursive<'a, D, R, E> {
    recursion: RecursionOptions,
    direct_references: Vec<&'a dyn SeaValue>,
    d_state: Condition,
    r_state: Condition,
    direct_reference_state: Option<Condition>,
    executor: E,
    _phantom: PhantomData<(D, R)>,
}

/// Recursion over `VersionedDao` tables at a point in time — uses
/// ROW_NUMBER-ranked CTEs and supports `d_state` / `r_state` /
/// `direct_reference_state`. Reach via [`Historic::recursive`].
pub struct HistoricRecursive<'a, T, D, R, E> {
    recursion: RecursionOptions,
    direct_references: Vec<&'a dyn SeaValue>,
    on: &'a T,
    d_state: Condition,
    r_state: Condition,
    direct_reference_state: Option<Condition>,
    executor: E,
    _phantom: PhantomData<(D, R)>,
}

// ---------------------------------------------------------------------------
// Top-level — non-versioned recursion.
// ---------------------------------------------------------------------------

impl<'a, D, R, E> RecursiveQuery<'a, D, R, E>
where
    D: Dao + Recursive,
    R: Dao,
    E: QueryExecutor,
{
    /// Single-source convenience: equivalent to
    /// `direct_references(vec![value])`.
    pub fn direct_reference(mut self, value: &'a dyn SeaValue) -> Self {
        self.direct_references = vec![value];
        self
    }

    /// Multi-source: the CTE anchors on `start_col IN (..)` and walks
    /// reachable rows from every starting point in a single query.
    pub fn direct_references(mut self, values: Vec<&'a dyn SeaValue>) -> Self {
        self.direct_references = values;
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
        R: DaoCore<Db = D::Db>,
    {
        let up_enabled = is_direction_enabled(self.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(self.recursion.max_downstream_depth());

        if !up_enabled && !down_enabled || self.direct_references.is_empty() {
            return Ok(vec![]);
        }

        let with = WithClause::new()
            .recursive(true)
            .cte(versions_cte_basic::<R>(Cte::ReferenceVersions))
            .cte(versions_cte_basic::<D>(Cte::RecursionVersions))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self.executor,
            with,
            &self.recursion,
            &self.direct_references,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// `current().recursive(...)` — recursion over _current views.
// ---------------------------------------------------------------------------

impl<E> Current<E> {
    /// Open a recursive walk over the `_current` views of `D` and `R`.
    /// One row per partition feeds the CTE — no ranking, no time travel.
    pub fn recursive<'a, D, R>(self, recursion: RecursionOptions) -> CurrentRecursive<'a, D, R, E>
    where
        D: CurrentDao + Recursive,
        R: CurrentDao,
    {
        CurrentRecursive {
            recursion,
            direct_references: vec![],
            d_state: Condition::all(),
            r_state: Condition::all(),
            direct_reference_state: None,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, D, R, E> CurrentRecursive<'a, D, R, E>
where
    D: CurrentDao + Recursive,
    R: CurrentDao,
    E: QueryExecutor,
{
    pub fn direct_reference(mut self, value: &'a dyn SeaValue) -> Self {
        self.direct_references = vec![value];
        self
    }

    pub fn direct_references(mut self, values: Vec<&'a dyn SeaValue>) -> Self {
        self.direct_references = values;
        self
    }

    /// State filter for the recursion DAO. Defaults to All.
    pub fn d_state(mut self, filter: StateFilter<D>) -> Self {
        self.d_state = filter.condition().clone();
        self
    }

    /// State filter for the reference DAO. Defaults to All.
    pub fn r_state(mut self, filter: StateFilter<R>) -> Self {
        self.r_state = filter.condition().clone();
        self
    }

    /// Extra state filter OR-ed with `r_state` only for direct-reference
    /// rows. `None` (the default) means no relaxation.
    pub fn direct_reference_state(mut self, filter: Option<StateFilter<R>>) -> Self {
        self.direct_reference_state = filter.map(|f| f.condition().clone());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
        R: DaoCore<Db = D::Db>,
    {
        let up_enabled = is_direction_enabled(self.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(self.recursion.max_downstream_depth());

        if !up_enabled && !down_enabled || self.direct_references.is_empty() {
            return Ok(vec![]);
        }

        // R-side state, optionally OR-ed with the entry-point relaxation.
        let r_state = if let Some(extra) = &self.direct_reference_state {
            let first_ref = self.direct_references.first().ok_or_else(|| {
                ta_objects::errors::SqlError::TypeNotFound(
                    "no direct references".into(),
                    Box::new(R::table().into_table_ref()),
                )
            })?;
            let starting_at = R::col_for_type(first_ref.as_type_id()).ok_or_else(|| {
                ta_objects::errors::SqlError::TypeNotFound(
                    format!("{:?}", first_ref),
                    Box::new(R::table().into_table_ref()),
                )
            })?;
            let direct_cond = Condition::all()
                .add(
                    Expr::col(starting_at)
                        .is_in(self.direct_references.iter().map(|r| r.as_sea_value())),
                )
                .add(extra.clone());
            Condition::any().add(self.r_state.clone()).add(direct_cond)
        } else {
            self.r_state.clone()
        };

        let with = WithClause::new()
            .recursive(true)
            .cte(versions_cte_current::<R>(r_state, Cte::ReferenceVersions))
            .cte(versions_cte_current::<D>(
                self.d_state,
                Cte::RecursionVersions,
            ))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self.executor,
            with,
            &self.recursion,
            &self.direct_references,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// `historic(&t).recursive(...)` — point-in-time recursion via ranked CTE.
// ---------------------------------------------------------------------------

impl<'a, T, E> Historic<'a, T, E> {
    /// Open a recursive walk pinned at the wrapper's `on` time. Builds
    /// ROW_NUMBER-ranked CTEs over `D` and `R`'s base tables; cost is
    /// O(table-size) per CTE. Supports `d_state` / `r_state` /
    /// `direct_reference_state`.
    pub fn recursive<D, R>(self, recursion: RecursionOptions) -> HistoricRecursive<'a, T, D, R, E>
    where
        D: VersionedDao<VersionOrder = T> + Recursive,
        R: VersionedDao<VersionOrder = T>,
    {
        HistoricRecursive {
            recursion,
            direct_references: vec![],
            on: self.on,
            d_state: Condition::all(),
            r_state: Condition::all(),
            direct_reference_state: None,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T, D, R, E> HistoricRecursive<'a, T, D, R, E>
where
    D: VersionedDao<VersionOrder = T> + Recursive,
    R: VersionedDao<VersionOrder = T>,
    T: SeaValue,
    E: QueryExecutor,
{
    pub fn direct_reference(mut self, value: &'a dyn SeaValue) -> Self {
        self.direct_references = vec![value];
        self
    }

    pub fn direct_references(mut self, values: Vec<&'a dyn SeaValue>) -> Self {
        self.direct_references = values;
        self
    }

    /// State filter for the recursion DAO. Defaults to All.
    pub fn d_state(mut self, filter: StateFilter<D>) -> Self {
        self.d_state = filter.condition().clone();
        self
    }

    /// State filter for the reference DAO. Defaults to All.
    pub fn r_state(mut self, filter: StateFilter<R>) -> Self {
        self.r_state = filter.condition().clone();
        self
    }

    /// Extra state filter OR-ed with `r_state` only for direct-reference
    /// rows. `None` (the default) means no relaxation. Pass
    /// `cond.then(|| filter)` to set conditionally without breaking the
    /// chain.
    pub fn direct_reference_state(mut self, filter: Option<StateFilter<R>>) -> Self {
        self.direct_reference_state = filter.map(|f| f.condition().clone());
        self
    }

    pub async fn fetch_all(self) -> Result<Vec<D>, SqlError>
    where
        E: Domained<Domain = D::Db>,
        R: DaoCore<Db = D::Db>,
    {
        let up_enabled = is_direction_enabled(self.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(self.recursion.max_downstream_depth());

        if !up_enabled && !down_enabled || self.direct_references.is_empty() {
            return Ok(vec![]);
        }

        let on: Option<&dyn SeaValue> = Some(self.on);
        // Reachability sub-CTE narrows the base CTE scans from O(table)
        // to O(reachable partitions) — see [`reachable_partitions_cte`].
        let with = WithClause::new()
            .recursive(true)
            .cte(reachable_partitions_cte::<D, R>(
                &self.direct_references,
                on,
                up_enabled,
                down_enabled,
            )?)
            .cte(reference_versions_cte::<D, R>(
                &self.direct_references,
                on,
                &self.r_state,
                self.direct_reference_state.as_ref(),
                true,
            )?)
            .cte(recursion_versions_cte::<D>(on, &self.d_state, true))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self.executor,
            with,
            &self.recursion,
            &self.direct_references,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::recursive::{
        Direction, direction_versions_cte, recursive_versions_cte, versioned_ranked_cte,
    };
    use crate::{HistoricEntry, LiveEntry};
    use chrono::{DateTime, Utc};
    use sea_query::{Asterisk, Query, WithClause};
    use std::sync::LazyLock;
    use ta_objects::Dao;
    use ta_objects::sql_condition::resolve_condition;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::dependency::{DependencyDB, DependencyDBWithNames};
    use td_objects::dxo::function::FunctionDB;
    use td_objects::dxo::trigger::{TriggerDB, TriggerDBWithNames};
    use td_objects::types::basic::{AtTime, CollectionId, Depth, FunctionId};

    #[ta_objects::typed(string)]
    #[derive(Eq, PartialEq)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestPartition;

    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[ta_objects::typed(string)]
    #[derive(Eq, PartialEq)]
    struct TestRecursion;

    // partitioning by id means there are no groups, each entry is independent
    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "test_table",
        versioned(pk = "id", version_by = "defined_on", partition_by = "id"),
        recursive(up = "current", down = "downstream"),
        states(
            Active = TestStatus::Active,
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
        versioned(pk = "id", version_by = "defined_on", partition_by = "reference_id"),
        states(
            Active = TestStatus::Active,
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

    /// Time later than any fixture or test insert — safe default for
    /// snapshot tests that don't care about the upper bound.
    static LATER: LazyLock<AtTime> = LazyLock::new(|| time(9));

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

    // ===================================================================
    // Unit tests — pure functions
    // ===================================================================

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

    // ===================================================================
    // Non-versioned fetch_all_recursive
    // ===================================================================

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        sql_table = "test_basic_table",
        recursive(up = "current", down = "downstream")
    )]
    struct BasicTestDao {
        id: TestId,
        current: TestRecursion,
        downstream: TestRecursion,
    }

    #[Dao]
    #[dao(sql_table = "test_basic_reference")]
    struct BasicReference {
        id: TestId,
        reference_id: TestRecursion,
    }

    #[td_test::test(db(crate::fixtures::Recursive))]
    async fn test_basic_fetch_all_recursive_downstream(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = db
            .recursive::<BasicTestDao, BasicReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .fetch_all()
            .await?;
        // Chain: R0(ref_0) -> N0(ref_0→ref_1) -> N1(ref_1→ref_2)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TestId::try_from("N1")?);
        assert_eq!(result[1].id, TestId::try_from("N0")?);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::Recursive))]
    async fn test_basic_fetch_all_recursive_upstream(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R2")?;
        let result: Vec<BasicTestDao> = db
            .recursive::<BasicTestDao, BasicReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
            .fetch_all()
            .await?;
        // Upstream from ref_2: N1(current=ref_1, downstream=ref_2) -> N0(current=ref_0, downstream=ref_1)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TestId::try_from("N1")?);
        assert_eq!(result[1].id, TestId::try_from("N0")?);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::Recursive))]
    async fn test_basic_fetch_all_recursive_both_disabled(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = db
            .recursive::<BasicTestDao, BasicReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
            .fetch_all()
            .await?;
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::Recursive))]
    async fn test_basic_fetch_all_recursive_depth_limit(db: TdPool) -> Result<(), TdError> {
        // Chain has 2 hops: R0(ref_0) → N0(ref_0→ref_1) → N1(ref_1→ref_2)
        // Depth limit of 1 should only return the immediate neighbor
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = db
            .recursive::<BasicTestDao, BasicReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::try_from(1).unwrap()),
            ))
            .direct_reference(&id)
            .fetch_all()
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("N0")?);
        Ok(())
    }

    // ===================================================================
    // Versioned — direction control
    // ===================================================================

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_both_directions_disabled(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_both_directions_unlimited(db: TdPool) {
        let id = TestId::try_from("RRR").unwrap();
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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

    // ===================================================================
    // Versioned — depth limits
    // ===================================================================

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_downstream_depth_limit_1(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::try_from(1).unwrap()),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                Some(Depth::try_from(2).unwrap()),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(2).unwrap()),
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                None,
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                Some(Depth::try_from(1).unwrap()),
            ))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::try_from(1).unwrap()),
                Some(Depth::try_from(2).unwrap()),
            ))
            .direct_reference(&id)
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

    // ===================================================================
    // Versioned — state & time filtering
    // ===================================================================

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_select_active_versions_at_time_fetch_all(db: TdPool) {
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .historic(&at)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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
        let early = AtTime::try_from(
            "2020-01-01T00:00:00.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap();
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = db
            .historic(&early)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .direct_reference(&id)
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
            .historic(&at)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .d_state(TestDao::active())
            .r_state(RecursionReference::active())
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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
        db.live().insert(&deleted_ref).await?;
        let target_ref = RecursionReference {
            id: TestId::try_from("TGT")?,
            reference_id: TestRecursion::try_from("ref_tgt")?,
            status: TestStatus::try_from("A")?,
            defined_on: time(5),
        };
        db.live().insert(&target_ref).await?;
        let dao = TestDao {
            id: TestId::try_from("OV1")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_del")?,
            downstream: TestRecursion::try_from("ref_tgt")?,
            defined_on: time(5),
        };
        db.live().insert(&dao).await?;

        let id = TestId::try_from("DEL")?;
        // Without direct_reference_state: DEL is deleted, Active filter excludes it.
        let empty: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .d_state(TestDao::active())
            .r_state(RecursionReference::active())
            .fetch_all()
            .await?;
        assert_eq!(empty.len(), 0);

        // With direct_reference_state(All): the entry point passes regardless of state.
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .d_state(TestDao::active())
            .r_state(RecursionReference::active())
            .direct_reference_state(Some(RecursionReference::all()))
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
            db.live().insert(&reference).await?;
        }
        let dao_a = TestDao {
            id: TestId::try_from("DSA")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_ds0")?,
            downstream: TestRecursion::try_from("ref_ds1")?,
            defined_on: time(5),
        };
        db.live().insert(&dao_a).await?;
        // This DAO is deleted — should be excluded by Active filter on D
        let dao_b = TestDao {
            id: TestId::try_from("DSB")?,
            status: TestStatus::try_from("D")?,
            current: TestRecursion::try_from("ref_ds1")?,
            downstream: TestRecursion::try_from("ref_ds2")?,
            defined_on: time(5),
        };
        db.live().insert(&dao_b).await?;

        let id = TestId::try_from("DS0")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .d_state(TestDao::active())
            .r_state(RecursionReference::active())
            .fetch_all()
            .await?;
        // DSA is active and found; DSB is deleted so the chain stops — ref_ds2 is never reached
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("DSA")?);
        Ok(())
    }

    // ===================================================================
    // Graph topology edge cases
    // ===================================================================

    #[td_test::test(db(crate::fixtures::RecursiveVersions))]
    async fn test_nonexistent_reference(db: TdPool) {
        let id = TestId::try_from("XXX").unwrap();
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .direct_reference(&id)
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
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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
        db.live().insert(&reference).await?;
        let dao = TestDao {
            id: TestId::try_from("IF0")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_iso")?,
            downstream: TestRecursion::try_from("ref_nowhere")?,
            defined_on: time(5),
        };
        db.live().insert(&dao).await?;

        let id = TestId::try_from("ISO")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .direct_reference(&id)
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
            db.live().insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("CF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_c{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_c{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            db.live().insert(&dao).await?;
        }

        let id = TestId::try_from("CC0")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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
            db.live().insert(&reference).await?;
        }
        for i in 0..3u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("UF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_uc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_uc{}", (i + 1) % 3))?,
                defined_on: time(5),
            };
            db.live().insert(&dao).await?;
        }

        let id = TestId::try_from("UC0")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                None,
                Some(Depth::ZERO),
            ))
            .direct_reference(&id)
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
            db.live().insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("BF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_bc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_bc{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            db.live().insert(&dao).await?;
        }

        let id = TestId::try_from("BC0")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(None, None))
            .direct_reference(&id)
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
            db.live().insert(&reference).await?;
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
                current: TestRecursion::try_from(*current)?,
                downstream: TestRecursion::try_from(*downstream)?,
                defined_on: time(5),
            };
            db.live().insert(&dao).await?;
        }

        let id = TestId::try_from("D00")?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
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

    // ===================================================================
    // Type compatibility & stress tests
    // ===================================================================

    #[td_test::test(db)]
    async fn test_select_active_versions_at_types(db: TdPool) -> Result<(), TdError> {
        use ta_objects::States;
        use td_database::domain::Apiserver;
        async fn test_query<const DS: u8, D, const RS: u8, R>(
            db: &TdPool,
            recursion_ref: &dyn SeaValue,
        ) -> Result<(), TdError>
        where
            D: VersionedDao<VersionOrder = AtTime>
                + Recursive
                + States<DS>
                + DaoCore<Db = Apiserver>,
            R: VersionedDao<VersionOrder = AtTime> + States<RS> + DaoCore<Db = Apiserver>,
        {
            let _result: Vec<D> = db
                .historic(&*LATER)
                .recursive::<D, R>(RecursionOptions::new(Some(Depth::ZERO), None))
                .direct_reference(recursion_ref)
                .d_state(StateFilter::<D>::from_states::<DS>())
                .r_state(StateFilter::<R>::from_states::<RS>())
                .fetch_all()
                .await?;
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
            db.live().insert(&reference).await?;
            let dao = TestDao {
                id: TestId::try_from(format!("ZZZ{i:04}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("x_ref_{i:04}"))?,
                downstream: TestRecursion::try_from(format!("x_ref_{:04}", i + 1))?,
                defined_on: time(5),
            };
            db.live().insert(&dao).await?;
            daos.push(dao);
        }

        let id = TestId::try_from(format!("YYY{:04}", 0))?;
        let result: Vec<TestDao> = db
            .historic(&*LATER)
            .recursive::<TestDao, RecursionReference>(RecursionOptions::new(
                Some(Depth::ZERO),
                None,
            ))
            .direct_reference(&id)
            .fetch_all()
            .await?;
        assert_eq!(result.len(), size);
        for i in 0..size {
            assert_eq!(result[i], daos[size - 1 - i]);
        }
        Ok(())
    }

    // ===================================================================
    // SQL generation tests
    // ===================================================================

    mod sql {
        use super::*;
        use crate::primitives::recursive::reachable_partitions_cte;
        use sea_query::SqliteQueryBuilder;

        #[test]
        fn test_reachable_partitions_cte_emits_recursive_walk() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reachable_partitions_cte::<TestDao, RecursionReference>(
                &[&id as &dyn SeaValue],
                None,
                true,
                true,
            )
            .unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Reachable)
                .to_owned()
                .with(WithClause::new().recursive(true).cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(sql.starts_with("WITH RECURSIVE"), "{sql}");
            assert!(sql.contains(r#""reachable""#), "names the CTE: {sql}");
            assert!(
                sql.contains(r#""partition_id""#),
                "exposes partition_id: {sql}"
            );
            // One UNION (anchor vs. recursive); both directions live inside
            // the recursive term via an OR-join + CASE projection — the
            // historical two-UNION shape ran into postgres' single
            // recursive-self-reference rule.
            assert!(
                sql.matches("UNION").count() == 1,
                "one UNION (anchor + recursive term covers both directions): {sql}"
            );
            assert!(
                sql.contains("CASE"),
                "directional CASE projection in recursive term: {sql}"
            );
            // No row-number ranking — this is the lightweight reachability walk.
            assert!(!sql.contains("ROW_NUMBER"), "lightweight walk: {sql}");
        }

        #[test]
        fn test_reachable_partitions_cte_only_upstream() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reachable_partitions_cte::<TestDao, RecursionReference>(
                &[&id as &dyn SeaValue],
                None,
                true,
                false,
            )
            .unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Reachable)
                .to_owned()
                .with(WithClause::new().recursive(true).cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(sql.matches("UNION").count() == 1, "one UNION only: {sql}");
        }

        #[test]
        fn test_reference_versions_cte_filtered_by_reachable() {
            let id = TestId::try_from("MMM").unwrap();
            let cte = reference_versions_cte::<TestDao, RecursionReference>(
                &[&id as &dyn SeaValue],
                None,
                &Condition::all(),
                None,
                true,
            )
            .unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::ReferenceVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            // The inner ranking subquery filters by Reachable.partition_id.
            assert!(
                sql.contains(r#""reachable""#) && sql.contains(r#""partition_id""#),
                "filters by reachable partitions: {sql}"
            );
            assert!(sql.contains("ROW_NUMBER"), "still ranks: {sql}");
        }

        #[test]
        fn test_versions_cte_basic() {
            let cte = versions_cte_basic::<BasicTestDao>(Cte::ReferenceVersions);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::ReferenceVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(sql.starts_with("WITH"), "should be a WITH query: {sql}");
            assert!(
                sql.contains(r#""test_basic_table""#),
                "should select from the DAO table: {sql}"
            );
            assert!(
                sql.contains(r#""reference_versions""#),
                "should name the CTE: {sql}"
            );
        }

        #[test]
        fn test_versioned_ranked_cte_no_defined_on() {
            let state_cond = resolve_condition::<TestDao>(TestStatus::Active).unwrap();
            let cte =
                versioned_ranked_cte::<TestDao>(None, state_cond, None, Cte::RecursionVersions);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursionVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(sql.contains("ROW_NUMBER"), "should use ROW_NUMBER: {sql}");
            assert!(sql.contains(r#""rn" = 1"#), "should filter rn = 1: {sql}");
            assert!(
                sql.contains(r#""status" = 'A'"#),
                "should filter by state: {sql}"
            );
            assert!(
                !sql.contains("<="),
                "should NOT have defined_on filter: {sql}"
            );
        }

        #[test]
        fn test_versioned_ranked_cte_with_defined_on() {
            let state_cond = resolve_condition::<TestDao>(TestStatus::Active).unwrap();
            let on = AtTime::try_from(
                "2025-04-02T08:19:50.543+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            )
            .unwrap();
            let cte = versioned_ranked_cte::<TestDao>(
                Some(&on),
                state_cond,
                None,
                Cte::RecursionVersions,
            );
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursionVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains("<="),
                "should have defined_on upper bound: {sql}"
            );
            assert!(sql.contains("ROW_NUMBER"), "should use ROW_NUMBER: {sql}");
        }

        #[test]
        fn test_direction_versions_cte_upstream() {
            let recursion = RecursionOptions::new(None, Some(Depth::ZERO));
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &[&direct_ref as &dyn SeaValue],
                Direction::Upstream,
                "INSTR",
            )
            .unwrap();
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
                .to_string(SqliteQueryBuilder);
            assert!(sql.contains("RECURSIVE"), "should be recursive: {sql}");
            assert!(sql.contains("UNION ALL"), "should have UNION ALL: {sql}");
            assert!(
                sql.contains(r#""upstream_versions""#),
                "should name CTE upstream_versions: {sql}"
            );
            assert!(
                sql.contains("'MMM'"),
                "should reference direct value: {sql}"
            );
            assert!(
                sql.contains("INSTR"),
                "should use INSTR for cycle detection: {sql}"
            );
            assert!(sql.contains("CONCAT"), "should use CONCAT for path: {sql}");
        }

        #[test]
        fn test_direction_versions_cte_downstream() {
            let recursion = RecursionOptions::new(Some(Depth::ZERO), None);
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &[&direct_ref as &dyn SeaValue],
                Direction::Downstream,
                "INSTR",
            )
            .unwrap();
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
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""downstream_versions""#),
                "should name CTE downstream_versions: {sql}"
            );
            assert!(sql.contains("UNION ALL"), "should have UNION ALL: {sql}");
        }

        #[test]
        fn test_direction_versions_cte_with_depth_limit() {
            let recursion =
                RecursionOptions::new(Some(Depth::ZERO), Some(Depth::try_from(3).unwrap()));
            let direct_ref = TestId::try_from("MMM").unwrap();
            let cte = direction_versions_cte::<BasicTestDao, BasicReference>(
                &recursion,
                &[&direct_ref as &dyn SeaValue],
                Direction::Downstream,
                "INSTR",
            )
            .unwrap();
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
                .to_string(SqliteQueryBuilder);
            assert!(sql.contains("< 3"), "should have depth limit < 3: {sql}");
        }

        #[test]
        fn test_recursive_versions_cte_both() {
            let cte = recursive_versions_cte(true, true);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursiveVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""upstream_versions""#),
                "should include upstream: {sql}"
            );
            assert!(
                sql.contains(r#""downstream_versions""#),
                "should include downstream: {sql}"
            );
            assert!(sql.contains("UNION ALL"), "should union both: {sql}");
        }

        #[test]
        fn test_recursive_versions_cte_upstream_only() {
            let cte = recursive_versions_cte(true, false);
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::RecursiveVersions)
                .to_owned()
                .with(WithClause::new().cte(cte).to_owned())
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""upstream_versions""#),
                "should include upstream: {sql}"
            );
            assert!(
                !sql.contains(r#""downstream_versions""#),
                "should NOT include downstream: {sql}"
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
                .to_string(SqliteQueryBuilder);
            assert!(
                !sql.contains(r#""upstream_versions""#),
                "should NOT include upstream: {sql}"
            );
            assert!(
                sql.contains(r#""downstream_versions""#),
                "should include downstream: {sql}"
            );
        }
    }
}
