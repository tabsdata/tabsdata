//
// Copyright 2026 Tabs Data Inc.
//

//! CTE builders for recursive queries.
//!
//! The trait + public option types live in
//! [`crate::generic::recursive`]; everything here is the SQL plumbing
//! the trait impl orchestrates. All `pub(crate)` so callers can't reach
//! in.

use crate::QueryExecutor;
use sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, IntoIden,
    IntoTableRef, JoinType, Order, OverStatement, Query, UnionType, WindowStatement, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::{CurrentDao, Dao, DaoCore, Recursive, VersionedDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::kind::DatabaseBackend;
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
    Rn,
    /// Reachable R partition IDs from `direct_references` via D's join
    /// graph — used to filter the ranked base CTEs in
    /// `historic.recursive` so they don't scan the full table.
    Reachable,
}

/// Single-column alias for the [`Cte::Reachable`] CTE — every UNION
/// branch projects its partition-id source as `partition_id` so the
/// recursive part can self-reference by name.
#[derive(Iden, Clone, Copy)]
pub(crate) struct PartitionId;

/// Traversal direction for a recursive CTE.
#[derive(Clone, Copy)]
pub(crate) enum Direction {
    Upstream,
    Downstream,
}

// ---------------------------------------------------------------------------
// Table aliases
// ---------------------------------------------------------------------------
//
// Zero-sized `Iden` impls — `Copy` lets them be passed by value as many
// times as needed without `String` allocation per use. `derive(Iden)`
// snake-cases the type name into the SQL identifier.

/// Row variable in base-table scans and versioned ranked sub-queries.
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

/// Direction-dependent self-alias for the recursive part of a direction CTE
/// (`uv` for upstream, `dv` for downstream).
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

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

/// Returns `true` when the direction should produce a CTE.
///
/// `None` means unlimited (enabled), `Some(0)` means disabled,
/// `Some(n)` where `n > 0` means enabled with a depth cap.
pub(crate) fn is_direction_enabled(max_depth: Option<Depth>) -> bool {
    !matches!(max_depth, Some(d) if *d == 0)
}

// ---------------------------------------------------------------------------
// CTE builders — non-versioned base CTEs
// ---------------------------------------------------------------------------

/// Build a simple base CTE for non-versioned tables (`SELECT * FROM T`).
pub(crate) fn versions_cte_basic<T: DaoCore>(cte_name: Cte) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .from_as(T::table(), Rv)
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

/// Build a base CTE that scans the `_current` view of a [`CurrentDao`]
/// instead of the base table — one row per partition, no ranking
/// needed. Use this when the recursion is at "now". `state_cond` is
/// AND-ed into the WHERE so callers can narrow to e.g. only Active rows.
pub(crate) fn versions_cte_current<T: CurrentDao>(
    state_cond: Condition,
    cte_name: Cte,
) -> CommonTableExpression {
    let query = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .from_as(Alias::new(T::current_table_name()), Rv)
        .cond_where(state_cond)
        .take();

    CommonTableExpression::new()
        .query(query)
        .table_name(cte_name)
        .to_owned()
}

// ---------------------------------------------------------------------------
// CTE builders — versioned base CTEs
// ---------------------------------------------------------------------------

/// Build a versioned base CTE using ROW_NUMBER ranking. `inner_filter`
/// is AND-ed into the ranking sub-query's WHERE — used to restrict the
/// scan to a reachable subset of the table (see [`reachable_partitions_cte`]).
pub(crate) fn versioned_ranked_cte<T: VersionedDao>(
    defined_on: Option<&dyn SeaValue>,
    state_cond: Condition,
    inner_filter: Option<Condition>,
    cte_name: Cte,
) -> CommonTableExpression {
    let window = WindowStatement::new()
        .partition_by(T::partition_col())
        .order_by_expr(Expr::col(T::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(T::pk_col()), Order::Desc)
        .take();

    let mut ranked = Query::select()
        .expr(Expr::col((Rv, Asterisk)))
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Cte::Rn,
        )
        .from_as(T::table(), Rv)
        .take();

    if let Some(on) = defined_on {
        ranked = ranked
            .and_where(Expr::col((Rv, T::version_order_col())).lte(on.as_sea_value()))
            .take();
    }

    if let Some(filter) = inner_filter {
        ranked = ranked.cond_where(filter).take();
    }

    let query = Query::select()
        .expr(Expr::col(Asterisk))
        .from_subquery(ranked, Alias::new("_sub"))
        .cond_where(
            Condition::all()
                .add(Expr::col(Cte::Rn).eq(1_i64))
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
/// `direct_reference_state`, when set, OR-s in a permissive branch
/// matching `direct_references` so the entry-point row passes a
/// broader filter than `r_state` allows for the rest of the walk.
pub(crate) fn reference_versions_cte<D, R>(
    direct_references: &[&dyn SeaValue],
    r_defined_on: Option<&dyn SeaValue>,
    r_state: &Condition,
    direct_reference_state: Option<&Condition>,
    reachable: bool,
) -> Result<CommonTableExpression, SqlError>
where
    D: Recursive,
    R: VersionedDao,
{
    let mut state_cond = r_state.clone();

    if let Some(extra) = direct_reference_state {
        let first_ref = direct_references.first().ok_or_else(|| {
            SqlError::TypeNotFound(
                "no direct references".into(),
                Box::new(R::table().into_table_ref()),
            )
        })?;
        let starting_at = R::col_for_type(first_ref.as_type_id()).ok_or(SqlError::TypeNotFound(
            format!("{:?}", first_ref),
            Box::new(R::table().into_table_ref()),
        ))?;
        let direct_cond = Condition::all()
            .add(Expr::col(starting_at).is_in(direct_references.iter().map(|r| r.as_sea_value())))
            .add(extra.clone());
        state_cond = Condition::any().add(state_cond).add(direct_cond);
    }

    let inner_filter = if reachable {
        Some(reachable_filter_for_recursion_ref::<D, R>()?)
    } else {
        None
    };

    Ok(versioned_ranked_cte::<R>(
        r_defined_on,
        state_cond,
        inner_filter,
        Cte::ReferenceVersions,
    ))
}

/// Build the `RecursionVersions` CTE for versioned tables.
pub(crate) fn recursion_versions_cte<D>(
    d_defined_on: Option<&dyn SeaValue>,
    d_state: &Condition,
    reachable: bool,
) -> CommonTableExpression
where
    D: VersionedDao + Recursive,
{
    let inner_filter = reachable.then(reachable_filter_for_recursion::<D>);
    versioned_ranked_cte::<D>(
        d_defined_on,
        d_state.clone(),
        inner_filter,
        Cte::RecursionVersions,
    )
}

/// `R.recursion_ref IN (SELECT partition_id FROM Reachable)` — used by
/// the reference base CTE when reachability filtering is on. Matches
/// on the column whose values were collected into `Reachable`, which
/// may differ from `R::partition_col()` (e.g. for `TableDB`, partition
/// is `table_id` but `recursion_ref` is `function_id`).
fn reachable_filter_for_recursion_ref<D, R>() -> Result<Condition, SqlError>
where
    D: Recursive,
    R: VersionedDao,
{
    let recursion_ref = R::col_for_type(std::any::TypeId::of::<D::Key>()).ok_or_else(|| {
        SqlError::TypeNotFound(
            std::any::type_name::<D::Key>().to_string(),
            Box::new(R::table().into_table_ref()),
        )
    })?;
    let sub = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    Ok(Condition::all().add(Expr::col((Rv, recursion_ref)).in_subquery(sub)))
}

/// `D.recurse_up IN (Reachable) OR D.recurse_down IN (Reachable)` —
/// used by the recursion base CTE when reachability filtering is on.
fn reachable_filter_for_recursion<D: VersionedDao + Recursive>() -> Condition {
    let sub_up = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    let sub_down = Query::select()
        .column(PartitionId)
        .from(Cte::Reachable)
        .take();
    Condition::any()
        .add(Expr::col((Rv, D::recurse_up())).in_subquery(sub_up))
        .add(Expr::col((Rv, D::recurse_down())).in_subquery(sub_down))
}

// ---------------------------------------------------------------------------
// CTE builder — reachability sub-query (R partition IDs reachable from
// `direct_references` via D's join graph).
// ---------------------------------------------------------------------------

/// Recursive CTE that walks the D ↔ R join graph from `direct_references`
/// and emits the set of R partition IDs reachable in the requested
/// directions. Output column: `partition_id`.
///
/// The walk uses raw base tables (no row-number ranking, no state
/// filter) and only joins on D's recursion columns — lightweight
/// compared to the full ranked base CTEs. `UNION` (DISTINCT)
/// terminates cycles. `version_order <= on` keeps the walk historical
/// when set.
///
/// The output is a SUPERSET of the partitions the proper recursive
/// walker (with state filter) will visit — feeding it as a partition
/// filter to [`reference_versions_cte`] / [`recursion_versions_cte`]
/// preserves correctness while bounding the base CTE scan to
/// `O(reachable_partitions)` instead of `O(table_size)`.
pub(crate) fn reachable_partitions_cte<D, R>(
    direct_references: &[&dyn SeaValue],
    on: Option<&dyn SeaValue>,
    up_enabled: bool,
    down_enabled: bool,
) -> Result<CommonTableExpression, SqlError>
where
    D: VersionedDao + Recursive,
    R: VersionedDao,
{
    debug_assert!(
        !direct_references.is_empty(),
        "direct_references must be non-empty; callers gate on this"
    );

    let first_ref = direct_references[0];
    let starting_at = R::col_for_type(first_ref.as_type_id()).ok_or(SqlError::TypeNotFound(
        format!("{:?}", first_ref),
        Box::new(R::table().into_table_ref()),
    ))?;
    let recursion_ref =
        R::col_for_type(std::any::TypeId::of::<D::Key>()).ok_or(SqlError::TypeNotFound(
            std::any::type_name::<D::Key>().to_string(),
            Box::new(R::table().into_table_ref()),
        ))?;

    // Base case: R rows whose `starting_at` matches direct_references —
    // emit `recursion_ref` as `partition_id`.
    let mut base = Query::select()
        .expr_as(Expr::col((Rv, recursion_ref)), PartitionId)
        .from_as(R::table(), Rv)
        .and_where(
            Expr::col((Rv, starting_at)).is_in(direct_references.iter().map(|r| r.as_sea_value())),
        )
        .take();
    if let Some(t) = on {
        base = base
            .and_where(Expr::col((Rv, R::version_order_col())).lte(t.as_sea_value()))
            .take();
    }

    // Build the recursive term as a single SELECT with one self-reference to
    // `Reachable`. Postgres rejects both (a) recursive references in the
    // non-recursive (anchor) term and (b) multiple recursive references
    // anywhere — `base UNION up UNION down` falls into (a), while combining
    // `up + down` with UNION ALL produces two `JOIN Reachable` and falls into
    // (b). Sqlite is loose about both. The portable form is one JOIN to
    // `Reachable` with a directional OR in the ON-clause, and a CASE in the
    // projection that picks which column to emit based on which direction
    // matched.
    let recurse_up = D::recurse_up();
    let recurse_down = D::recurse_down();
    let join_cond = match (up_enabled, down_enabled) {
        (true, true) => {
            // Match either direction.
            Condition::any()
                .add(Expr::col((Rcv, recurse_down)).equals((Fx, PartitionId)))
                .add(Expr::col((Rcv, recurse_up)).equals((Fx, PartitionId)))
        }
        (true, false) => {
            Condition::all().add(Expr::col((Rcv, recurse_down)).equals((Fx, PartitionId)))
        }
        (false, true) => {
            Condition::all().add(Expr::col((Rcv, recurse_up)).equals((Fx, PartitionId)))
        }
        (false, false) => {
            // No recursion — anchor only.
            return Ok(CommonTableExpression::new()
                .query(base)
                .table_name(Cte::Reachable)
                .to_owned());
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

    let mut recursive = Query::select()
        .expr_as(emit_col, PartitionId)
        .from_as(D::table(), Rcv)
        .join_as(JoinType::InnerJoin, Cte::Reachable, Fx, join_cond)
        .take();
    if let Some(t) = on {
        recursive = recursive
            .and_where(Expr::col((Rcv, D::version_order_col())).lte(t.as_sea_value()))
            .take();
    }

    let combined = {
        let mut b = base;
        b.union(UnionType::Distinct, recursive).take()
    };

    Ok(CommonTableExpression::new()
        .query(combined)
        .table_name(Cte::Reachable)
        .to_owned())
}

// ---------------------------------------------------------------------------
// CTE builders — direction & final union
// ---------------------------------------------------------------------------

/// Build a direction-specific recursive CTE (upstream or downstream).
pub(crate) fn direction_versions_cte<D, R>(
    recursion: &RecursionOptions,
    direct_references: &[&dyn SeaValue],
    direction: Direction,
    str_search_fn: &str,
) -> Result<CommonTableExpression, SqlError>
where
    D: Dao + Recursive,
    R: Dao,
{
    debug_assert!(
        !direct_references.is_empty(),
        "direct_references must be non-empty; callers gate on this"
    );
    // Resolve direction-specific parameters
    let (start_col, follow_col, path_iden, depth_iden, cte_name, self_alias, max_depth) =
        match direction {
            Direction::Upstream => (
                D::recurse_down(),
                D::recurse_up(),
                Cte::PathUp,
                Cte::DepthUp,
                Cte::UpstreamVersions,
                DirAlias::Uv,
                recursion.max_upstream_depth(),
            ),
            Direction::Downstream => (
                D::recurse_up(),
                D::recurse_down(),
                Cte::PathDown,
                Cte::DepthDown,
                Cte::DownstreamVersions,
                DirAlias::Dv,
                recursion.max_downstream_depth(),
            ),
        };

    let recursion_ref =
        R::col_for_type(std::any::TypeId::of::<D::Key>()).ok_or(SqlError::TypeNotFound(
            std::any::type_name::<D::Key>().to_string(),
            Box::new(R::table().into_table_ref()),
        ))?;
    let first_ref = direct_references[0];
    let starting_at = R::col_for_type(first_ref.as_type_id()).ok_or(SqlError::TypeNotFound(
        format!("{:?}", first_ref),
        Box::new(R::table().into_table_ref()),
    ))?;

    // Base case — common chain (always present)
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
        .and_where(
            Expr::col((Rfv, starting_at)).is_in(direct_references.iter().map(|r| r.as_sea_value())),
        )
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
                .and_where(Expr::col((Fx, recursion_ref)).equals((Rcv, D::recurse_down())))
                .take(),
        ))
        .and_where(Expr::exists(
            Query::select()
                .expr(Expr::val(1_i32))
                .from_as(Cte::ReferenceVersions, Fy)
                .and_where(Expr::col((Fy, recursion_ref)).equals((Rcv, D::recurse_up())))
                .take(),
        ))
        .take();

    let (path_append_col, check_col) = match direction {
        Direction::Upstream => (
            Expr::col((Rcv, follow_col)),
            Expr::col((Rcv, D::recurse_up())),
        ),
        Direction::Downstream => (
            Expr::col((self_alias, follow_col)),
            Expr::col((Rcv, D::recurse_down())),
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
                Func::cust(Alias::new(str_search_fn))
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

    Ok(CommonTableExpression::new()
        .query(union_query)
        .table_name(cte_name)
        .to_owned())
}

/// Build the `RecursiveVersions` CTE that unions the enabled direction CTEs.
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

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Append direction CTEs and the final union CTE to an existing `WITH`
/// clause, then execute the resulting `SELECT DISTINCT` query.
pub(crate) async fn build_and_execute_recursive<D, R, E>(
    executor: E,
    mut with: WithClause,
    recursion: &RecursionOptions,
    direct_references: &[&dyn SeaValue],
    up_enabled: bool,
    down_enabled: bool,
) -> Result<Vec<D>, SqlError>
where
    D: Dao + Recursive,
    R: Dao,
    E: QueryExecutor,
{
    #[allow(
        clippy::match_same_arms,
        clippy::match_single_binding,
        clippy::single_match
    )]
    let str_search_fn = match executor.kind() {
        #[cfg(sqlite)]
        DatabaseBackend::Sqlite => "INSTR",
        #[cfg(postgres)]
        DatabaseBackend::Postgres => "STRPOS",
        #[cfg(mysql)]
        DatabaseBackend::MySql => "INSTR",
    };

    if up_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                direct_references,
                Direction::Upstream,
                str_search_fn,
            )?)
            .to_owned();
    }
    if down_enabled {
        with = with
            .cte(direction_versions_cte::<D, R>(
                recursion,
                direct_references,
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
