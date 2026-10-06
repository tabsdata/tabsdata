//
// Copyright 2026 Tabsdata Inc.
//

//! CTE builders for recursive queries; the public surface is
//! [`crate::generic::recursive`].

use crate::QueryExecutor;
use crate::primitives::scd2::{open_interval_at, visible_at};
use sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, IntoIden,
    JoinType, Query, UnionType, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::{Dao, HasColumn, RecursiveDao, Scd2Dao, SelectDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::types::basic::Depth;
use td_objects::types::recursion::RecursionOptions;

/// The rows a walk starts from, with the column of `R` their type resolved to.
pub(crate) struct Start<'a, R: Dao> {
    col: R::Iden,
    values: Vec<&'a dyn SeaValue>,
}

impl<'a, R: Dao> Start<'a, R> {
    pub(crate) fn of<V: SeaValue>(values: Vec<&'a V>) -> Self
    where
        R: HasColumn<V>,
    {
        Start {
            col: <R as HasColumn<V>>::COL,
            values: values.into_iter().map(|v| v as &dyn SeaValue).collect(),
        }
    }

    pub(crate) fn matches(&self) -> Expr {
        Expr::col(self.col).is_in(self.values.iter().map(|v| v.as_sea_expr()))
    }

    pub(crate) fn matches_in(&self, table: impl IntoIden) -> Expr {
        Expr::col((table.into_iden(), self.col.into_iden()))
            .is_in(self.values.iter().map(|v| v.as_sea_expr()))
    }
}

/// CTE names in the generated `WITH RECURSIVE` clauses.
#[derive(Iden, Clone, Copy)]
pub(crate) enum Cte {
    ReferenceVersions,
    RecursionVersions,
    UpstreamVersions,
    DownstreamVersions,
    RecursiveVersions,
    PathUp,
    DepthUp,
    PathDown,
    DepthDown,
    /// R partition ids reachable through D's join graph, so the visible-at
    /// base CTEs never scan the full table.
    Reachable,
}

/// Every UNION branch projects its partition-id source under this name so the
/// recursive part can self-reference it.
#[derive(Iden, Clone, Copy)]
pub(crate) struct PartitionId;

#[derive(Clone, Copy)]
pub(crate) enum Direction {
    Upstream,
    Downstream,
}

/// Row variable in base-table and interval scans.
#[derive(Iden, Clone, Copy)]
struct Rv;

/// Recursion-candidate variable in direction CTEs.
#[derive(Iden, Clone, Copy)]
struct Rcv;

/// Reference-table join variable in direction CTEs.
#[derive(Iden, Clone, Copy)]
struct Rfv;

/// Secondary reference variable for existence sub-queries.
#[derive(Iden, Clone, Copy)]
struct Rfv2;

/// Self-alias for the recursive part of a direction CTE.
#[derive(Iden, Clone, Copy)]
pub(crate) enum DirAlias {
    Uv,
    Dv,
}

/// Existence sub-query reference aliases used inside the recursive part.
#[derive(Iden, Clone, Copy)]
struct Fx;

#[derive(Iden, Clone, Copy)]
struct Fy;

/// `None` means unlimited (enabled), `Some(0)` disabled, `Some(n > 0)`
/// enabled with a depth cap.
pub(crate) fn is_direction_enabled(max_depth: Option<Depth>) -> bool {
    !matches!(max_depth, Some(d) if *d == 0)
}

#[cfg(test)]
pub(crate) fn versions_cte_basic<T: Dao>(cte_name: Cte) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .from_as(T::TABLE, Rv)
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

/// Open intervals only (`valid_to = 'infinity'`): one row per partition,
/// served by the partial indexes.
pub(crate) fn versions_cte_current<T: Scd2Dao>(
    state_cond: Condition,
    cte_name: Cte,
) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .from_as(T::TABLE, Rv)
        .and_where(open_interval_at((Rv, T::VALID_TO)))
        .cond_where(state_cond)
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

/// Visible-at scan of `T`, restricted to `reachable` and `state_cond`.
pub(crate) fn versioned_visible_cte<T: Scd2Dao, V: SeaValue>(
    on: &V,
    state_cond: Condition,
    reachable: Condition,
    cte_name: Cte,
) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .from_as(T::TABLE, Rv)
        .cond_where(visible_at((Rv, T::VALID_FROM), (Rv, T::VALID_TO), on))
        .cond_where(reachable)
        .cond_where(state_cond)
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

/// `direct_reference_state`, when set, OR-s in a permissive branch for the
/// starting rows, so the entry point passes a broader filter than the walk.
pub(crate) fn reference_state_cond<R: Dao>(
    start: &Start<'_, R>,
    r_state: &Condition,
    direct_reference_state: Option<&Condition>,
) -> Condition {
    match direct_reference_state {
        Some(extra) => Condition::any()
            .add(r_state.clone())
            .add(Condition::all().add(start.matches()).add(extra.clone())),
        None => r_state.clone(),
    }
}

pub(crate) fn reference_versions_cte<D, R, V>(
    start: &Start<'_, R>,
    on: &V,
    r_state: &Condition,
    direct_reference_state: Option<&Condition>,
) -> CommonTableExpression
where
    D: RecursiveDao,
    R: Scd2Dao + HasColumn<D::Key>,
    V: SeaValue,
{
    versioned_visible_cte::<R, V>(
        on,
        reference_state_cond(start, r_state, direct_reference_state),
        reachable_filter_for_recursion_ref::<D, R>(),
        Cte::ReferenceVersions,
    )
}

pub(crate) fn recursion_versions_cte<D, V>(on: &V, d_state: &Condition) -> CommonTableExpression
where
    D: SelectDao + Scd2Dao + RecursiveDao,
    V: SeaValue,
{
    versioned_visible_cte::<D, V>(
        on,
        d_state.clone(),
        reachable_filter_for_recursion::<D>(),
        Cte::RecursionVersions,
    )
}

/// Matches on the column whose values were collected into `Reachable`, which
/// may differ from `R::PARTITION`.
fn reachable_filter_for_recursion_ref<D, R>() -> Condition
where
    D: RecursiveDao,
    R: Scd2Dao + HasColumn<D::Key>,
{
    let sub = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    Condition::all().add(Expr::col((Rv, <R as HasColumn<D::Key>>::COL)).in_subquery(sub))
}

/// `D.recurse_up IN (Reachable) OR D.recurse_down IN (Reachable)`.
fn reachable_filter_for_recursion<D: SelectDao + Scd2Dao + RecursiveDao>() -> Condition {
    let sub_up = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    let sub_down = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    Condition::any()
        .add(Expr::col((Rv, D::UP)).in_subquery(sub_up))
        .add(Expr::col((Rv, D::DOWN)).in_subquery(sub_down))
}

/// Walks the D ↔ R join graph from `start` and emits the reachable R partition
/// ids as `partition_id`. Raw base tables, no visibility or state filter, so
/// the result is a superset of what the filtered walk visits: as a partition
/// filter that stays correct while bounding the base CTE scans to
/// `O(reachable_partitions)`. `UNION` terminates cycles.
pub(crate) fn reachable_partitions_cte<D, R, V>(
    start: &Start<'_, R>,
    on: &V,
    up_enabled: bool,
    down_enabled: bool,
) -> CommonTableExpression
where
    D: SelectDao + Scd2Dao + RecursiveDao,
    R: Scd2Dao + HasColumn<D::Key>,
    V: SeaValue,
{
    let recursion_ref = <R as HasColumn<D::Key>>::COL;

    let base = Query::select()
        .expr_as(Expr::col((Rv, recursion_ref)), PartitionId)
        .from_as(R::TABLE, Rv)
        .and_where(start.matches_in(Rv))
        .and_where(Expr::col((Rv, R::VALID_FROM)).lte(on.as_sea_expr()))
        .take();

    // Postgres rejects recursive references in the anchor term and multiple
    // recursive references anywhere, so both directions share one JOIN to
    // `Reachable` with a directional OR, and a CASE picks the column to emit.
    let recurse_up = D::UP;
    let recurse_down = D::DOWN;
    let join_cond = match (up_enabled, down_enabled) {
        (true, true) => Condition::any()
            .add(Expr::col((Rcv, recurse_down)).equals((Fx, PartitionId)))
            .add(Expr::col((Rcv, recurse_up)).equals((Fx, PartitionId))),
        (true, false) => {
            Condition::all().add(Expr::col((Rcv, recurse_down)).equals((Fx, PartitionId)))
        }
        (false, true) => {
            Condition::all().add(Expr::col((Rcv, recurse_up)).equals((Fx, PartitionId)))
        }
        (false, false) => {
            return CommonTableExpression::new()
                .query(base)
                .table_name(Cte::Reachable)
                .to_owned();
        }
    };
    let emit_col: Expr = match (up_enabled, down_enabled) {
        (true, true) => Expr::case(
            Expr::col((Rcv, recurse_down)).eq(Expr::col((Fx, PartitionId))),
            Expr::col((Rcv, recurse_up)),
        )
        .finally(Expr::col((Rcv, recurse_down)))
        .into(),
        (true, false) => Expr::col((Rcv, recurse_up)),
        (false, true) => Expr::col((Rcv, recurse_down)),
        (false, false) => unreachable!(),
    };

    let recursive = Query::select()
        .expr_as(emit_col, PartitionId)
        .from_as(D::TABLE, Rcv)
        .join_as(JoinType::InnerJoin, Cte::Reachable, Fx, join_cond)
        .and_where(Expr::col((Rcv, D::VALID_FROM)).lte(on.as_sea_expr()))
        .take();

    let combined = {
        let mut b = base;
        b.union(UnionType::Distinct, recursive).take()
    };

    CommonTableExpression::new()
        .query(combined)
        .table_name(Cte::Reachable)
        .to_owned()
}

pub(crate) fn direction_versions_cte<D, R>(
    recursion: &RecursionOptions,
    start: &Start<'_, R>,
    direction: Direction,
) -> CommonTableExpression
where
    D: SelectDao + RecursiveDao,
    R: SelectDao + HasColumn<D::Key>,
{
    let (start_col, follow_col, path_iden, depth_iden, cte_name, self_alias, max_depth) =
        match direction {
            Direction::Upstream => (
                D::DOWN,
                D::UP,
                Cte::PathUp,
                Cte::DepthUp,
                Cte::UpstreamVersions,
                DirAlias::Uv,
                recursion.max_upstream_depth(),
            ),
            Direction::Downstream => (
                D::UP,
                D::DOWN,
                Cte::PathDown,
                Cte::DepthDown,
                Cte::DownstreamVersions,
                DirAlias::Dv,
                recursion.max_downstream_depth(),
            ),
        };

    let recursion_ref = <R as HasColumn<D::Key>>::COL;

    let mut base = Query::select()
        .expr(Expr::col((Rcv, Asterisk)))
        .expr_as(Expr::val(1_i32), depth_iden)
        .from_as(Cte::RecursionVersions, Rcv)
        .join_as(
            JoinType::InnerJoin,
            Cte::ReferenceVersions,
            Rfv,
            Expr::col((Rfv, recursion_ref)).equals((Rcv, start_col)),
        )
        .and_where(start.matches_in(Rfv))
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, Rfv2)
                .and_where(Expr::col((Rfv2, recursion_ref)).equals((Rcv, follow_col)))
                .take(),
        ))
        .take();

    base = base
        .expr_as(
            Func::cust(Alias::new("CONCAT"))
                .arg(Expr::val(","))
                .arg(Func::cast_as(
                    Expr::col((Rcv, start_col)),
                    Alias::new("TEXT"),
                ))
                .arg(Expr::val(",")),
            path_iden,
        )
        .take();

    let mut recursive_part = Query::select()
        .expr(Expr::col((Rcv, Asterisk)))
        .expr_as(
            Expr::col((self_alias, depth_iden.into_iden())).add(Expr::val(1_i32)),
            depth_iden,
        )
        .from_as(Cte::RecursionVersions, Rcv)
        .join_as(
            JoinType::InnerJoin,
            cte_name,
            self_alias,
            Expr::col((self_alias, follow_col)).equals((Rcv, start_col)),
        )
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, Fx)
                .and_where(Expr::col((Fx, recursion_ref)).equals((Rcv, D::DOWN)))
                .take(),
        ))
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, Fy)
                .and_where(Expr::col((Fy, recursion_ref)).equals((Rcv, D::UP)))
                .take(),
        ))
        .take();

    let (path_append_col, check_col) = match direction {
        Direction::Upstream => (Expr::col((Rcv, follow_col)), Expr::col((Rcv, D::UP))),
        Direction::Downstream => (
            Expr::col((self_alias, follow_col)),
            Expr::col((Rcv, D::DOWN)),
        ),
    };
    recursive_part = recursive_part
        .expr_as(
            Func::cust(Alias::new("CONCAT"))
                .arg(Expr::col((self_alias, path_iden)))
                .arg(Func::cast_as(path_append_col, Alias::new("TEXT")))
                .arg(Expr::val(",")),
            path_iden,
        )
        .and_where(
            Expr::from(
                Func::cust(Alias::new("STRPOS"))
                    .arg(Expr::col((self_alias, path_iden)))
                    .arg(
                        Func::cust(Alias::new("CONCAT"))
                            .arg(Expr::val(","))
                            .arg(check_col)
                            .arg(Expr::val(",")),
                    ),
            )
            .eq(0),
        )
        .take();

    if let Some(d) = max_depth {
        recursive_part = recursive_part
            .and_where(Expr::col((self_alias, depth_iden.into_iden())).lt(Expr::val(*d)))
            .take();
    }

    let union_query = base.union(UnionType::All, recursive_part).take();

    CommonTableExpression::new()
        .query(union_query)
        .table_name(cte_name)
        .to_owned()
}

pub(crate) fn recursive_versions_cte(
    has_upstream: bool,
    has_downstream: bool,
) -> CommonTableExpression {
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

pub(crate) async fn build_and_execute_recursive<D, R, E>(
    executor: E,
    mut with: WithClause,
    recursion: &RecursionOptions,
    start: &Start<'_, R>,
    up_enabled: bool,
    down_enabled: bool,
) -> Result<Vec<D>, SqlError>
where
    D: SelectDao + RecursiveDao,
    R: SelectDao + HasColumn<D::Key>,
    E: QueryExecutor,
{
    if up_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                start,
                Direction::Upstream,
            ))
            .to_owned();
    }
    if down_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                start,
                Direction::Downstream,
            ))
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
