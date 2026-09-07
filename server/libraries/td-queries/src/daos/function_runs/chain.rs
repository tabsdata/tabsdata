//
// Copyright 2026 Tabsdata Inc.
//

//! The logical chain `logical_function_run_inputs → logical_table_data_versions
//! → materialization → producer LFR`, shared by the readiness, blocked and
//! commit queries. A NULL producer (a version minted over pre-2.0 data) counts
//! as terminal.

use crate::primitives::scd2::{open_interval_at, visible_at};
use sea_query::{
    Condition, Expr, ExprTrait, Iden, IntoIden, JoinType, Query, SelectStatement, SimpleExpr,
};
use ta_objects::SeaValue;
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBIden;
use td_objects::types::basic::{AtTime, LogicalFunctionRunStatus};

/// Alias of the LEFT-joined producer LFR; NULL when the version has none.
#[derive(Iden, Clone, Copy)]
pub(crate) struct ProducerLfr;

/// Alias of the LEFT-joined materialization of an input version; NULL when
/// there is none. See [`unmaterialized`].
#[derive(Iden, Clone, Copy)]
pub(crate) struct CurrentPtdv;

/// Which materialization of an input version the chain sees.
#[derive(Clone, Copy)]
pub(crate) enum Visibility<'a> {
    Open,
    At(&'a AtTime),
}

/// `ptdv.logical_table_data_version_id = ltdv.id` plus the visibility
/// predicate over `ptdv`'s interval columns.
pub(crate) fn materialization_of_ltdv<T: IntoIden>(
    ptdv: T,
    visibility: Visibility<'_>,
) -> Condition {
    let ptdv = ptdv.into_iden();
    let linked = Expr::col((
        ptdv.clone(),
        PhysicalTableDataVersionDBIden::LogicalTableDataVersionId,
    ))
    .equals((
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::Id,
    ));
    let valid_to = (ptdv.clone(), PhysicalTableDataVersionDBIden::ValidTo);
    let visible = match visibility {
        Visibility::Open => Condition::all().add(open_interval_at(valid_to)),
        Visibility::At(at) => visible_at(
            (ptdv, PhysicalTableDataVersionDBIden::ValidFrom),
            valid_to,
            at,
        ),
    };
    Condition::all().add(linked).add(visible)
}

fn producer_id() -> Expr {
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
}

/// The input version has no materialization the chain can see. Readiness and
/// commit block on it; the initial status must not — at plan-build time no
/// in-plan output is materialized yet.
pub(crate) fn unmaterialized() -> SimpleExpr {
    Expr::col((CurrentPtdv, PhysicalTableDataVersionDBIden::Id)).is_null()
}

fn producer_status() -> Expr {
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status))
}

fn status_values(statuses: &[LogicalFunctionRunStatus]) -> Vec<sea_query::Value> {
    statuses.iter().map(SeaValue::as_sea_value).collect()
}

/// Producer present and in `statuses`; a NULL producer never qualifies.
pub(crate) fn producer_status_in_strict(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    producer_id()
        .is_not_null()
        .and(producer_status().is_in(status_values(statuses)))
}

/// Producer present and not in `statuses`; a NULL producer never qualifies.
pub(crate) fn producer_status_not_in(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    producer_id()
        .is_not_null()
        .and(producer_status().is_not_in(status_values(statuses)))
}

/// Producer in `statuses`, or absent.
pub(crate) fn producer_status_in_or_lazy(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    producer_id()
        .is_null()
        .or(producer_status().is_in(status_values(statuses)))
}

/// The chain as a bare `FROM … JOIN …` with the self-loop excluded (a
/// function's trigger-self input row references its own output version).
/// The caller adds the projection, the outer correlation and the blocking
/// predicate over [`CurrentPtdv`] / [`ProducerLfr`].
///
/// The materialization hop is a fenced lateral (`LIMIT 1 OFFSET 0`), never
/// `JOIN ON id = (subquery)`: RLS quals blind the planner to ~1-row
/// estimates, and a subquery equality demotes the probe into a join filter
/// over inputs × versions. LEFT, so an unmaterialized input stays on the
/// chain for the caller to judge instead of vanishing from it.
pub(crate) fn build_chain(visibility: Visibility<'_>) -> SelectStatement {
    chain(Some(visibility))
}

/// [`build_chain`] without the materialization hop: the producer's status is
/// all the caller judges, and that is not versioned, so no `at` applies.
pub(crate) fn producer_chain() -> SelectStatement {
    chain(None)
}

fn chain(materialization: Option<Visibility<'_>>) -> SelectStatement {
    let producer = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
    );
    let mut stmt = Query::select();
    stmt.from(LogicalFunctionRunInputDBIden::Table).join(
        JoinType::InnerJoin,
        LogicalTableDataVersionDBIden::Table,
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::Id,
        ))
        .equals((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        )),
    );
    if let Some(visibility) = materialization {
        let current_ptdv = Query::select()
            .column(PhysicalTableDataVersionDBIden::Id)
            .from(PhysicalTableDataVersionDBIden::Table)
            .cond_where(materialization_of_ltdv(
                PhysicalTableDataVersionDBIden::Table,
                visibility,
            ))
            .limit(1)
            .offset(0)
            .take();
        stmt.join_lateral(
            JoinType::LeftJoin,
            current_ptdv,
            CurrentPtdv,
            Expr::val(true),
        );
    }
    stmt.join_as(
        JoinType::LeftJoin,
        LogicalFunctionRunDBIden::Table,
        ProducerLfr,
        Expr::col(producer).equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
    )
    .and_where(
        Expr::col(producer)
            .is_null()
            .or(Expr::col(producer).ne(Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            )))),
    )
    .take()
}
