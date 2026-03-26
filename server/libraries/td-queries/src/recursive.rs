//
// Copyright 2026 Tabs Data Inc.
//

use derive_builder::Builder;
use sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, IntoIden,
    IntoTableRef, JoinType, Order, OverStatement, Query, UnionType, WindowStatement, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{DaoCore, Recursive, States, VersionedDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::types::basic::Depth;
use td_objects::types::recursion::RecursionOptions;

// ---------------------------------------------------------------------------
// Types & enums
// ---------------------------------------------------------------------------

/// Names for the Common Table Expressions used in recursive queries.
///
/// Each variant maps to a CTE in the generated `WITH RECURSIVE` clause.
/// `Rn` is the `ROW_NUMBER` column alias used inside versioned ranking sub-queries.
#[derive(Iden, Clone, Copy)]
enum Cte {
    ReferenceVersions,
    RecursionVersions,
    UpstreamVersions,
    DownstreamVersions,
    RecursiveVersions,
    PathUp,
    DepthUp,
    PathDown,
    DepthDown,
    Rn,
}

/// Traversal direction for a recursive CTE.
#[derive(Clone, Copy)]
enum Direction {
    Upstream,
    Downstream,
}

// ---------------------------------------------------------------------------
// Table aliases
// ---------------------------------------------------------------------------

/// Alias `"rv"` — row variable in base-table scans and versioned ranked sub-queries.
fn row_var() -> Alias {
    Alias::new("rv")
}

/// Alias `"rcv"` — recursion-candidate variable in direction CTEs.
fn recursion_var() -> Alias {
    Alias::new("rcv")
}

/// Alias `"rfv"` — reference-table join variable in direction CTEs.
fn reference_var() -> Alias {
    Alias::new("rfv")
}

/// Alias `"rfv2"` — secondary reference variable for existence sub-queries.
fn reference_var2() -> Alias {
    Alias::new("rfv2")
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

/// Returns `true` when the direction should produce a CTE.
///
/// `None` means unlimited (enabled), `Some(0)` means disabled,
/// `Some(n)` where `n > 0` means enabled with a depth cap.
fn is_direction_enabled(max_depth: Option<Depth>) -> bool {
    !matches!(max_depth, Some(d) if *d == 0)
}

// ---------------------------------------------------------------------------
// Public API — option structs & trait
// ---------------------------------------------------------------------------

/// Shared options for non-versioned recursive queries.
///
/// Use [`OptionsBuilder`] (via `derive_builder`) to construct an instance,
/// then call [`Options::versioned`] to upgrade to [`VersionedOptions`].
#[derive(Builder)]
#[builder(pattern = "owned", build_fn(error = "TdError"))]
pub struct Options<'a> {
    recursion: RecursionOptions,
    direct_reference: &'a dyn SeaValue,
}

impl<'a> Options<'a> {
    /// Start building a [`VersionedOptions`] from these shared options.
    pub fn versioned<D: VersionedDao + Recursive, R: VersionedDao>(
        self,
    ) -> VersionedOptionsBuilder<'a, D, R> {
        VersionedOptionsBuilder::default().options(self)
    }
}

/// Extended options for versioned recursive queries.
///
/// Adds optional version-order upper bounds (`d_defined_on`, `r_defined_on`)
/// and an optional pre-resolved state override for the direct reference row.
///
/// Use [`Options::versioned`] to create a [`VersionedOptionsBuilder`].
#[derive(Builder)]
#[builder(pattern = "owned", build_fn(error = "TdError"))]
pub struct VersionedOptions<'a, D: VersionedDao + Recursive, R: VersionedDao> {
    options: Options<'a>,
    #[builder(default, setter(strip_option))]
    d_defined_on: Option<&'a D::VersionOrder>,
    #[builder(default, setter(strip_option))]
    r_defined_on: Option<&'a R::VersionOrder>,
    #[builder(default, setter(custom))]
    override_direct_reference_state: Option<Condition>,
}

impl<'a, D: VersionedDao + Recursive, R: VersionedDao> VersionedOptionsBuilder<'a, D, R> {
    /// Set an override state condition for the direct reference row.
    /// The condition is resolved eagerly against `R`.
    pub fn override_direct_reference_state(
        mut self,
        value: Option<impl SqlCondition>,
    ) -> Result<Self, SqlError> {
        self.override_direct_reference_state = match value {
            Some(cond) => Some(Some(resolve_condition::<R>(cond)?)),
            None => Some(None),
        };
        Ok(self)
    }
}

/// Extension trait that adds recursive-query methods to any [`SeaExecutor`].
pub trait RecursiveQueries {
    /// Fetch rows reachable from `direct_reference` via non-versioned recursion.
    fn fetch_all_recursive<D, R>(
        self,
        opts: Options<'_>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: DaoCore + Recursive,
        R: DaoCore;

    /// Fetch rows reachable from `direct_reference` via versioned recursion,
    /// applying ROW_NUMBER ranking and state filtering.
    fn fetch_all_versions_recursive<const DS: u8, D, const RS: u8, R>(
        self,
        opts: VersionedOptions<'_, D, R>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: VersionedDao + Recursive + States<DS>,
        R: VersionedDao + States<RS>;
}

// ---------------------------------------------------------------------------
// CTE builders — non-versioned base CTEs
// ---------------------------------------------------------------------------

/// Build a simple base CTE for non-versioned tables (`SELECT * FROM T`).
///
/// No ROW_NUMBER windowing and no state filtering — used by
/// [`RecursiveQueries::fetch_all_recursive`] for both the reference and
/// recursion tables.
fn versions_cte_basic<T: DaoCore>(cte_name: Cte) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((row_var(), Asterisk)))
        .from_as(T::table(), row_var())
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

// ---------------------------------------------------------------------------
// CTE builders — versioned base CTEs
// ---------------------------------------------------------------------------

/// Build a versioned base CTE using ROW_NUMBER ranking.
///
/// Ranks all rows in `T` per `T::partition_col()` (ordered by
/// `T::version_order_col() DESC`), then keeps only `rn = 1` rows that
/// satisfy `state_cond`. An optional `defined_on` upper-bounds the version
/// order so only rows at or before that point are considered.
///
/// This is the shared core of [`reference_versions_cte`] and
/// [`recursion_versions_cte`].
fn versioned_ranked_cte<T: VersionedDao>(
    defined_on: Option<&T::VersionOrder>,
    state_cond: Condition,
    cte_name: Cte,
) -> CommonTableExpression {
    let window = WindowStatement::new()
        .partition_by(T::partition_col())
        .order_by_expr(Expr::col(T::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(T::pk_col()), Order::Desc)
        .take();

    let mut ranked = Query::select()
        .expr(Expr::col((row_var(), Asterisk)))
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Cte::Rn,
        )
        .from_as(T::table(), row_var())
        .take();

    if let Some(on) = defined_on {
        ranked = ranked
            .and_where(Expr::col((row_var(), T::version_order_col())).lte(on.as_sea_value()))
            .take();
    }

    let query = Query::select()
        .expr(Expr::col(Asterisk))
        .from_subquery(ranked, Alias::new("_sub"))
        .cond_where(
            Condition::all()
                .add(Expr::col(Cte::Rn).eq(1))
                .add(state_cond),
        )
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

/// Build the `ReferenceVersions` CTE for versioned tables.
///
/// Constructs the state condition for the reference table `R`, optionally
/// widening it for the direct-reference row when
/// `override_direct_reference_state` is set, then delegates to
/// [`versioned_ranked_cte`].
fn reference_versions_cte<const DS: u8, D, const RS: u8, R>(
    opts: &VersionedOptions<'_, D, R>,
) -> Result<CommonTableExpression, SqlError>
where
    D: VersionedDao + Recursive + States<DS>,
    R: VersionedDao + States<RS>,
{
    let mut state_cond = resolve_condition::<R>(R::state())?;

    if let Some(ref override_state) = opts.override_direct_reference_state {
        let starting_at = R::col_for_type(opts.options.direct_reference.as_type_id()).ok_or(
            SqlError::TypeNotFound(
                format!("{:?}", opts.options.direct_reference),
                R::table().into_table_ref(),
            ),
        )?;
        let override_cond = Condition::all()
            .add(Expr::col(starting_at).eq(opts.options.direct_reference.as_sea_value()))
            .add(override_state.clone());
        state_cond = Condition::any().add(state_cond).add(override_cond);
    }

    Ok(versioned_ranked_cte::<R>(
        opts.r_defined_on.as_deref(),
        state_cond,
        Cte::ReferenceVersions,
    ))
}

/// Build the `RecursionVersions` CTE for versioned tables.
///
/// Applies the standard state condition for the recursion table `D` (no
/// override logic), then delegates to [`versioned_ranked_cte`].
fn recursion_versions_cte<const DS: u8, D, const RS: u8, R>(
    opts: &VersionedOptions<'_, D, R>,
) -> Result<CommonTableExpression, SqlError>
where
    D: VersionedDao + Recursive + States<DS>,
    R: VersionedDao + States<RS>,
{
    let state_cond = resolve_condition::<D>(D::state())?;

    Ok(versioned_ranked_cte::<D>(
        opts.d_defined_on.as_deref(),
        state_cond,
        Cte::RecursionVersions,
    ))
}

// ---------------------------------------------------------------------------
// CTE builders — direction & final union
// ---------------------------------------------------------------------------

/// Build a direction-specific recursive CTE (upstream or downstream).
///
/// Generates a `UNION ALL` of a base case and a recursive case:
///
/// - **Base case**: recursion-table rows whose start-column joins through the
///   reference table to `direct_reference`, with a matching follow-side
///   reference.
/// - **Recursive case**: recursion-table rows that chain from the previous
///   level via `follow_col → start_col`, with cycle detection (path-based
///   string search) and an optional depth limit.
fn direction_versions_cte<D, R>(
    recursion: &RecursionOptions,
    direct_reference: &dyn SeaValue,
    direction: Direction,
    str_search_fn: &str,
) -> Result<CommonTableExpression, SqlError>
where
    D: DaoCore + Recursive,
    R: DaoCore,
{
    // Resolve direction-specific parameters
    let (start_col, follow_col, path_iden, depth_iden, cte_name, self_alias, max_depth) =
        match direction {
            Direction::Upstream => (
                D::recurse_down(),
                D::recurse_up(),
                Cte::PathUp,
                Cte::DepthUp,
                Cte::UpstreamVersions,
                Alias::new("uv"),
                recursion.max_upstream_depth(),
            ),
            Direction::Downstream => (
                D::recurse_up(),
                D::recurse_down(),
                Cte::PathDown,
                Cte::DepthDown,
                Cte::DownstreamVersions,
                Alias::new("dv"),
                recursion.max_downstream_depth(),
            ),
        };

    let recursion_ref =
        R::col_for_type(std::any::TypeId::of::<D::Key>()).ok_or(SqlError::TypeNotFound(
            std::any::type_name::<D::Key>().to_string(),
            R::table().into_table_ref(),
        ))?;
    let starting_at =
        R::col_for_type(direct_reference.as_type_id()).ok_or(SqlError::TypeNotFound(
            format!("{:?}", direct_reference),
            R::table().into_table_ref(),
        ))?;

    // Base case — common chain (always present)
    let mut base = Query::select()
        .expr(Expr::col((recursion_var(), Asterisk)))
        .expr_as(Expr::val(1_i32), depth_iden)
        .from_as(Cte::RecursionVersions, recursion_var())
        .join_as(
            JoinType::InnerJoin,
            Cte::ReferenceVersions,
            reference_var(),
            Expr::col((reference_var(), recursion_ref)).equals((recursion_var(), start_col)),
        )
        .and_where(Expr::col((reference_var(), starting_at)).eq(direct_reference.as_sea_value()))
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, reference_var2())
                .and_where(
                    Expr::col((reference_var2(), recursion_ref))
                        .equals((recursion_var(), follow_col)),
                )
                .take(),
        ))
        .take();

    // Path column: delimiter-wrapped path ",value,"
    base = base
        .expr_as(
            Func::cust(Alias::new("CONCAT"))
                .arg(Expr::val(","))
                .arg(Func::cast_as(
                    Expr::col((recursion_var(), start_col)),
                    Alias::new("TEXT"),
                ))
                .arg(Expr::val(",")),
            path_iden,
        )
        .take();

    // Recursive case — common chain
    let fx = Alias::new("fx");
    let fy = Alias::new("fy");

    let mut recursive_part = Query::select()
        .expr(Expr::col((recursion_var(), Asterisk)))
        .expr_as(
            Expr::col((self_alias.clone(), depth_iden.into_iden())).add(Expr::val(1_i32)),
            depth_iden,
        )
        .from_as(Cte::RecursionVersions, recursion_var())
        .join_as(
            JoinType::InnerJoin,
            cte_name,
            self_alias.clone(),
            Expr::col((self_alias.clone(), follow_col)).equals((recursion_var(), start_col)),
        )
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, fx.clone())
                .and_where(
                    Expr::col((fx, recursion_ref)).equals((recursion_var(), D::recurse_down())),
                )
                .take(),
        ))
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, fy.clone())
                .and_where(
                    Expr::col((fy, recursion_ref)).equals((recursion_var(), D::recurse_up())),
                )
                .take(),
        ))
        .take();

    // Path append + cycle detection: fn(prev.path, CONCAT(',', element, ',')) = 0
    let (path_append_col, check_col) = match direction {
        Direction::Upstream => (
            (recursion_var(), follow_col),
            (recursion_var(), D::recurse_up()),
        ),
        Direction::Downstream => (
            (self_alias.clone(), follow_col),
            (recursion_var(), D::recurse_down()),
        ),
    };
    recursive_part = recursive_part
        .expr_as(
            Func::cust(Alias::new("CONCAT"))
                .arg(Expr::col((self_alias.clone(), path_iden)))
                .arg(Func::cast_as(
                    Expr::col(path_append_col),
                    Alias::new("TEXT"),
                ))
                .arg(Expr::val(",")),
            path_iden,
        )
        .and_where(
            Expr::from(
                Func::cust(Alias::new(str_search_fn))
                    .arg(Expr::col((self_alias.clone(), path_iden)))
                    .arg(
                        Func::cust(Alias::new("CONCAT"))
                            .arg(Expr::val(","))
                            .arg(Expr::col(check_col))
                            .arg(Expr::val(",")),
                    ),
            )
            .eq(0),
        )
        .take();

    // Depth limit only in recursive step, only when max_depth is Some(n) with n > 0
    if let Some(d) = max_depth {
        recursive_part = recursive_part
            .and_where(Expr::col((self_alias, depth_iden.into_iden())).lt(Expr::val(*d)))
            .take();
    }

    let union_query = base.union(UnionType::All, recursive_part).take();

    Ok(CommonTableExpression::new()
        .query(union_query)
        .table_name(cte_name)
        .to_owned())
}

/// Build the `RecursiveVersions` CTE that unions the enabled direction CTEs.
///
/// At least one of `has_upstream` / `has_downstream` must be `true`; the
/// caller guarantees this by checking [`is_direction_enabled`] beforehand.
fn recursive_versions_cte(has_upstream: bool, has_downstream: bool) -> CommonTableExpression {
    let query = match (has_upstream, has_downstream) {
        (true, true) => {
            let mut upstream = Query::select()
                .expr(Expr::col(Asterisk))
                .from(Cte::UpstreamVersions)
                .take();
            let downstream = Query::select()
                .expr(Expr::col(Asterisk))
                .from(Cte::DownstreamVersions)
                .take();
            upstream.union(UnionType::All, downstream).take()
        }
        (true, false) => Query::select()
            .expr(Expr::col(Asterisk))
            .from(Cte::UpstreamVersions)
            .take(),
        (false, true) => Query::select()
            .expr(Expr::col(Asterisk))
            .from(Cte::DownstreamVersions)
            .take(),
        (false, false) => unreachable!("at least one direction must be enabled"),
    };

    CommonTableExpression::new()
        .query(query)
        .table_name(Cte::RecursiveVersions)
        .to_owned()
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Append direction CTEs and the final union CTE to an existing `WITH`
/// clause, then execute the resulting `SELECT DISTINCT` query.
///
/// This is the shared tail of both [`RecursiveQueries::fetch_all_recursive`]
/// and [`RecursiveQueries::fetch_all_versions_recursive`]. The caller is
/// responsible for building the initial `with` clause with the appropriate
/// base CTEs (basic or versioned).
async fn build_and_execute_recursive<D, R, E>(
    executor: E,
    mut with: WithClause,
    recursion: &RecursionOptions,
    direct_reference: &dyn SeaValue,
    up_enabled: bool,
    down_enabled: bool,
) -> Result<Vec<D>, SqlError>
where
    D: DaoCore + Recursive,
    R: DaoCore,
    E: SeaExecutor + Send,
{
    let str_search_fn = match executor.kind() {
        #[cfg(feature = "postgres")]
        td_database::sea_query::kind::DatabaseBackend::Postgres => "STRPOS",
        #[allow(unreachable_patterns)]
        _ => "INSTR",
    };

    if up_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                direct_reference,
                Direction::Upstream,
                str_search_fn,
            )?)
            .to_owned();
    }
    if down_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                direct_reference,
                Direction::Downstream,
                str_search_fn,
            )?)
            .to_owned();
    }
    with = with
        .cte(recursive_versions_cte(up_enabled, down_enabled))
        .to_owned();

    let r = Query::select()
        .distinct()
        .columns(D::columns())
        .from(Cte::RecursiveVersions)
        .order_by_columns(D::order_by())
        .take()
        .with(with)
        .build_query_as()
        .fetch_all(executor)
        .await
        .map_err(SqlError::on_read::<D>)?;
    Ok(r)
}

impl<E> RecursiveQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_all_recursive<D, R>(self, opts: Options<'_>) -> Result<Vec<D>, SqlError>
    where
        D: DaoCore + Recursive,
        R: DaoCore,
    {
        let up_enabled = is_direction_enabled(opts.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(opts.recursion.max_downstream_depth());

        if !up_enabled && !down_enabled {
            return Ok(vec![]);
        }

        let with = WithClause::new()
            .recursive(true)
            .cte(versions_cte_basic::<R>(Cte::ReferenceVersions))
            .cte(versions_cte_basic::<D>(Cte::RecursionVersions))
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self,
            with,
            &opts.recursion,
            opts.direct_reference,
            up_enabled,
            down_enabled,
        )
        .await
    }

    async fn fetch_all_versions_recursive<const DS: u8, D, const RS: u8, R>(
        self,
        opts: VersionedOptions<'_, D, R>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao + Recursive + States<DS>,
        R: VersionedDao + States<RS>,
    {
        let up_enabled = is_direction_enabled(opts.options.recursion.max_upstream_depth());
        let down_enabled = is_direction_enabled(opts.options.recursion.max_downstream_depth());

        if !up_enabled && !down_enabled {
            return Ok(vec![]);
        }

        let with = WithClause::new()
            .recursive(true)
            .cte(reference_versions_cte::<DS, D, RS, R>(&opts)?)
            .cte(recursion_versions_cte::<DS, D, RS, R>(&opts)?)
            .to_owned();

        build_and_execute_recursive::<D, R, _>(
            self,
            with,
            &opts.options.recursion,
            opts.options.direct_reference,
            up_enabled,
            down_enabled,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BasicQueries;
    use chrono::{DateTime, Utc};
    use std::sync::LazyLock;
    use ta_objects::Dao;
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

    #[td_test::test(sqlx(fixture = crate::fixtures::Recursive))]
    #[tokio::test]
    async fn test_basic_fetch_all_recursive_downstream(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = (&db)
            .fetch_all_recursive::<BasicTestDao, BasicReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()?,
            )
            .await?;
        // Chain: R0(ref_0) -> N0(ref_0→ref_1) -> N1(ref_1→ref_2)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TestId::try_from("N1")?);
        assert_eq!(result[1].id, TestId::try_from("N0")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::Recursive))]
    #[tokio::test]
    async fn test_basic_fetch_all_recursive_upstream(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R2")?;
        let result: Vec<BasicTestDao> = (&db)
            .fetch_all_recursive::<BasicTestDao, BasicReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, Some(Depth::ZERO)))
                    .direct_reference(&id)
                    .build()?,
            )
            .await?;
        // Upstream from ref_2: N1(current=ref_1, downstream=ref_2) -> N0(current=ref_0, downstream=ref_1)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TestId::try_from("N1")?);
        assert_eq!(result[1].id, TestId::try_from("N0")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::Recursive))]
    #[tokio::test]
    async fn test_basic_fetch_all_recursive_both_disabled(db: TdPool) -> Result<(), TdError> {
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = (&db)
            .fetch_all_recursive::<BasicTestDao, BasicReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO)))
                    .direct_reference(&id)
                    .build()?,
            )
            .await?;
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::Recursive))]
    #[tokio::test]
    async fn test_basic_fetch_all_recursive_depth_limit(db: TdPool) -> Result<(), TdError> {
        // Chain has 2 hops: R0(ref_0) → N0(ref_0→ref_1) → N1(ref_1→ref_2)
        // Depth limit of 1 should only return the immediate neighbor
        let id = TestId::try_from("R0")?;
        let result: Vec<BasicTestDao> = (&db)
            .fetch_all_recursive::<BasicTestDao, BasicReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::ZERO),
                        Some(Depth::try_from(1).unwrap()),
                    ))
                    .direct_reference(&id)
                    .build()?,
            )
            .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("N0")?);
        Ok(())
    }

    // ===================================================================
    // Versioned — direction control
    // ===================================================================

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_both_directions_disabled(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO)))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_both_directions_unlimited(db: TdPool) {
        let id = TestId::try_from("RRR").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // Upstream: DDD(ref_1→ref_4) → AAA(ref_0→ref_1)
        // Downstream: EEE(ref_4→ref_5)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_none_fetch_all(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
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

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_none_fetch_all_upstream(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, Some(Depth::ZERO)))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // SSS: EEE -> DDD -> AAA
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_upstream_only_downstream_disabled(db: TdPool) {
        let id = TestId::try_from("RRR").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        None,
                        Some(Depth::ZERO),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // RRR → ref_4. Upstream: DDD (current=ref_1, downstream=ref_4) → AAA (current=ref_0, downstream=ref_1)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_downstream_only_upstream_disabled(db: TdPool) {
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::ZERO),
                        None,
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
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

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_downstream_depth_limit_1(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::ZERO),
                        Some(Depth::try_from(1).unwrap()),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // Depth 1: AAA only (base case from ref_0)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_downstream_depth_limit_2(db: TdPool) {
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::ZERO),
                        Some(Depth::try_from(2).unwrap()),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // Depth 1: AAA. Depth 2: BBB, CCC, DDD. Depth 3 blocked.
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[3]); // DDD
        assert_eq!(result[1], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[2], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[3], FIXTURE_TEST_DAOS[0]); // AAA
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_upstream_depth_limit_2(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::try_from(2).unwrap()),
                        Some(Depth::ZERO),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // Depth 1: EEE. Depth 2: DDD. Depth 3 blocked.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]); // EEE
        assert_eq!(result[1], FIXTURE_TEST_DAOS[3]); // DDD
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_none_last_in_stream(db: TdPool) {
        let id = TestId::try_from("SSS").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::try_from(1).unwrap()), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // SSS: EEE (direct upstream)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[4]);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_both_directions_with_depth_limits(db: TdPool) {
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::try_from(1).unwrap()),
                        Some(Depth::try_from(1).unwrap()),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
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

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_asymmetric_depth_limits(db: TdPool) {
        // NNN is at ref_1 in the middle. Use upstream=1, downstream=2
        let id = TestId::try_from("NNN").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(
                        Some(Depth::try_from(1).unwrap()),
                        Some(Depth::try_from(2).unwrap()),
                    ))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
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

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_at_time_fetch_all(db: TdPool) {
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .d_defined_on(&at)
                    .r_defined_on(&at)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // MMM: AAA -> BBB -> CCC (the rest didn't exist yet)
        assert_eq!(result.len(), 3);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[1]);
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_at_time_before_any_data(db: TdPool) {
        // Use a time before all fixture data was inserted
        let early = AtTime::try_from(
            "2020-01-01T00:00:00.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap();
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .d_defined_on(&early)
                    .r_defined_on(&early)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_break_downstream_by_status_ref(db: TdPool) {
        let at = time(2);
        let id = TestId::try_from("MMM").unwrap();
        // PPP is D at time 2, so it will stop the recursion
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::Active }, TestDao, { RecursionReference::Active }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .d_defined_on(&at)
                    .r_defined_on(&at)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // MMM: AAA -> CCC (as PPP is D, which should be pointing to BBB)
        assert_eq!(result.len(), 2);
        // (order by ID DESC)
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]);
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_select_active_versions_none_deleted(db: TdPool) {
        // OOO is active, but PPP has the same partition_id and is deleted
        let id = TestId::try_from("OOO").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_override_direct_reference_state(db: TdPool) -> Result<(), TdError> {
        // Create a deleted reference that is the only (rn=1) row for its partition.
        // Without override, Active state filter excludes it.
        // With override on the direct reference id, it gets included.
        let deleted_ref = RecursionReference {
            id: TestId::try_from("DEL")?,
            reference_id: TestRecursion::try_from("ref_del")?,
            status: TestStatus::try_from("D")?,
            defined_on: time(5),
        };
        (&db).insert(&deleted_ref).await?;
        // Create a ref for the downstream target
        let target_ref = RecursionReference {
            id: TestId::try_from("TGT")?,
            reference_id: TestRecursion::try_from("ref_tgt")?,
            status: TestStatus::try_from("A")?,
            defined_on: time(5),
        };
        (&db).insert(&target_ref).await?;
        // DAO linking ref_del → ref_tgt
        let dao = TestDao {
            id: TestId::try_from("OV1")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_del")?,
            downstream: TestRecursion::try_from("ref_tgt")?,
            defined_on: time(5),
        };
        (&db).insert(&dao).await?;

        // Without override: DEL is deleted, so Active filter excludes it → empty
        let id = TestId::try_from("DEL")?;
        let empty: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::Active }, TestDao, { RecursionReference::Active }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()?
                    .versioned()
                    .build()?,
            )
            .await?;
        assert_eq!(empty.len(), 0);

        // With override: allow the direct reference row to pass with All states
        let all_states = <RecursionReference as States<{ RecursionReference::All }>>::state();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::Active }, TestDao, { RecursionReference::Active }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()?
                    .versioned()
                    .override_direct_reference_state(Some(all_states))?
                    .build()?,
            )
            .await?;
        // DEL now passes state filter via override → OV1 (ref_del→ref_tgt) is the base case
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("OV1")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
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
            (&db).insert(&reference).await?;
        }
        let dao_a = TestDao {
            id: TestId::try_from("DSA")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_ds0")?,
            downstream: TestRecursion::try_from("ref_ds1")?,
            defined_on: time(5),
        };
        (&db).insert(&dao_a).await?;
        // This DAO is deleted — should be excluded by Active filter on D
        let dao_b = TestDao {
            id: TestId::try_from("DSB")?,
            status: TestStatus::try_from("D")?,
            current: TestRecursion::try_from("ref_ds1")?,
            downstream: TestRecursion::try_from("ref_ds2")?,
            defined_on: time(5),
        };
        (&db).insert(&dao_b).await?;

        let id = TestId::try_from("DS0")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::Active }, TestDao, { RecursionReference::Active }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()?
                    .versioned()
                    .build()?,
            )
            .await?;
        // DSA is active and found; DSB is deleted so the chain stops — ref_ds2 is never reached
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, TestId::try_from("DSA")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_d_defined_on_without_r_defined_on(db: TdPool) -> Result<(), TdError> {
        // d_defined_on=time(1) restricts DAOs: only AAA(t0), BBB(t0), CCC(t1) are visible.
        // DDD(t2) and EEE(t3) are excluded.
        // r_defined_on is unset → all references visible (including PPP at t2).
        // Using All states so PPP (status=D) still passes.
        let at = time(1);
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .d_defined_on(&at)
                    // r_defined_on intentionally omitted
                    .build()
                    .unwrap(),
            )
            .await?;
        // AAA(ref_0→ref_1), BBB(ref_1→ref_2), CCC(ref_1→ref_3) all visible.
        // DDD and EEE excluded by d_defined_on.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[2]); // CCC
        assert_eq!(result[1], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[2], FIXTURE_TEST_DAOS[0]); // AAA
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_r_defined_on_without_d_defined_on(db: TdPool) -> Result<(), TdError> {
        // All DAOs are visible (no d_defined_on filter), but references are
        // time-filtered to time(0), excluding PPP (time 2), QQQ (time 1),
        // RRR (time 3), SSS (time 3).
        let at = time(0);
        let id = TestId::try_from("MMM").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    // d_defined_on intentionally omitted
                    .r_defined_on(&at)
                    .build()
                    .unwrap(),
            )
            .await?;
        // At time(0) only references MMM(ref_0), NNN(ref_1), OOO(ref_2) exist.
        // Chain: AAA(ref_0→ref_1) → BBB(ref_1→ref_2). CCC(ref_1→ref_3) needs
        // QQQ(ref_3) which doesn't exist at time(0). DDD(ref_1→ref_4) needs
        // RRR(ref_4) which doesn't exist at time(0).
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], FIXTURE_TEST_DAOS[1]); // BBB
        assert_eq!(result[1], FIXTURE_TEST_DAOS[0]); // AAA
        Ok(())
    }

    // ===================================================================
    // Graph topology edge cases
    // ===================================================================

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_nonexistent_reference(db: TdPool) {
        let id = TestId::try_from("XXX").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // XXX doesn't exist in reference table
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_downstream_from_leaf_node(db: TdPool) {
        let id = TestId::try_from("QQQ").unwrap();
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        // ref_3 is a leaf: no dao has current=ref_3, so base case is empty
        assert_eq!(result.len(), 0);
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_single_node_no_recursion_neighbors(db: TdPool) -> Result<(), TdError> {
        // Create an isolated node with no edges leading anywhere
        let reference = RecursionReference {
            id: TestId::try_from("ISO")?,
            reference_id: TestRecursion::try_from("ref_iso")?,
            status: TestStatus::try_from("A")?,
            defined_on: time(5),
        };
        (&db).insert(&reference).await?;
        let dao = TestDao {
            id: TestId::try_from("IF0")?,
            status: TestStatus::try_from("A")?,
            current: TestRecursion::try_from("ref_iso")?,
            downstream: TestRecursion::try_from("ref_nowhere")?,
            defined_on: time(5),
        };
        (&db).insert(&dao).await?;

        let id = TestId::try_from("ISO")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await?;
        // ref_nowhere doesn't have a reference entry, so downstream base case fails EXISTS check
        // No upstream edges point to ref_iso either
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // Create a 4-node cycle: CF0→CF1→CF2→CF3→(back to ref_c0)
        for i in 0..4u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("CC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_c{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            (&db).insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("CF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_c{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_c{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            (&db).insert(&dao).await?;
        }

        let id = TestId::try_from("CC0")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await?;
        // CF0→CF1→CF2→CF3→(cycle back to ref_c0 blocked)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, TestId::try_from("CF2")?);
        assert_eq!(result[1].id, TestId::try_from("CF1")?);
        assert_eq!(result[2].id, TestId::try_from("CF0")?);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_upstream_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // Create 3-node cycle for upstream traversal
        for i in 0..3u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("UC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_uc{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            (&db).insert(&reference).await?;
        }
        for i in 0..3u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("UF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_uc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_uc{}", (i + 1) % 3))?,
                defined_on: time(5),
            };
            (&db).insert(&dao).await?;
        }

        let id = TestId::try_from("UC0")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, Some(Depth::ZERO)))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await?;
        // Upstream from ref_uc0: UF2(downstream=ref_uc0) → UF1(downstream=ref_uc2) → cycle blocked
        assert_eq!(result.len(), 2);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_bidirectional_cycle_detection(db: TdPool) -> Result<(), TdError> {
        // 4-node cycle, query with both directions enabled
        for i in 0..4u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("BC{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_bc{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            (&db).insert(&reference).await?;
        }
        for i in 0..4u32 {
            let dao = TestDao {
                id: TestId::try_from(format!("BF{i}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("ref_bc{i}"))?,
                downstream: TestRecursion::try_from(format!("ref_bc{}", (i + 1) % 4))?,
                defined_on: time(5),
            };
            (&db).insert(&dao).await?;
        }

        let id = TestId::try_from("BC0")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(None, None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await?;
        // Both directions should terminate without infinite loop
        // Downstream: BF0→BF1→BF2→(cycle back blocked)
        // Upstream: BF3(downstream=ref_bc0)→BF2(downstream=ref_bc3)→(cycle back blocked)
        assert!(result.len() <= 4);
        assert!(result.len() > 0);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
    async fn test_diamond_deduplication(db: TdPool) -> Result<(), TdError> {
        // Create diamond: ref_d0 → ref_d1, ref_d0 → ref_d2, ref_d1 → ref_d3, ref_d2 → ref_d3, ref_d3 → ref_d4
        for i in 0..5u32 {
            let reference = RecursionReference {
                id: TestId::try_from(format!("D0{i}"))?,
                reference_id: TestRecursion::try_from(format!("ref_d{i}"))?,
                status: TestStatus::try_from("A")?,
                defined_on: time(5),
            };
            (&db).insert(&reference).await?;
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
            (&db).insert(&dao).await?;
        }

        let id = TestId::try_from("D00")?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_select_active_versions_at_types(db: TdPool) -> Result<(), TdError> {
        async fn test_query<const DS: u8, D, const RS: u8, R>(
            db: &TdPool,
            recursion_ref: &dyn SeaValue,
        ) -> Result<(), TdError>
        where
            D: VersionedDao + Recursive + States<DS>,
            R: VersionedDao + States<RS>,
        {
            let _result: Vec<D> = db
                .fetch_all_versions_recursive::<DS, D, RS, R>(
                    OptionsBuilder::default()
                        .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                        .direct_reference(recursion_ref)
                        .build()
                        .unwrap()
                        .versioned()
                        .build()
                        .unwrap(),
                )
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

    #[td_test::test(sqlx(fixture = crate::fixtures::RecursiveVersions))]
    #[tokio::test]
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
            (&db).insert(&reference).await?;
            let dao = TestDao {
                id: TestId::try_from(format!("ZZZ{i:04}"))?,
                status: TestStatus::try_from("A")?,
                current: TestRecursion::try_from(format!("x_ref_{i:04}"))?,
                downstream: TestRecursion::try_from(format!("x_ref_{:04}", i + 1))?,
                defined_on: time(5),
            };
            (&db).insert(&dao).await?;
            daos.push(dao);
        }

        let id = TestId::try_from(format!("YYY{:04}", 0))?;
        let result: Vec<TestDao> = (&db)
            .fetch_all_versions_recursive::<{ TestDao::All }, TestDao, { RecursionReference::All }, RecursionReference>(
                OptionsBuilder::default()
                    .recursion(RecursionOptions::new(Some(Depth::ZERO), None))
                    .direct_reference(&id)
                    .build()
                    .unwrap()
                    .versioned()
                    .build()
                    .unwrap(),
            )
            .await?;
        assert_eq!(result.len(), size);
        for i in 0..size {
            assert_eq!(result[i], daos[size - 1 - i]);
        }
        Ok(())
    }
}
