//
// Copyright 2026 Tabs Data Inc.
//

//! Shared SQL scaffolding for the logical-layer readiness chain.
//!
//! The chain walks from a downstream consumer's
//! [`LogicalFunctionRunInputDB`] to the upstream LTDV and on to the
//! producer LFR (for canonical status):
//!
//! ```text
//! FFR_input → LTDV → producer_LFR (status)
//!                   + latest pointer event
//! ```
//!
//! `LTDV` itself has no status column — flow-control status lives on
//! the producer LFR and is read via the `LEFT JOIN producer_lfr` here.
//! NULL `producer_lfr` (lazy-mint over a pre-2.0 concrete TDV) is
//! treated as terminal: the data was produced before the logical layer
//! existed, so it's always "done" from a readiness standpoint.
//!
//! Self-loop exclusion (NULL-safe): a function's trigger-self FFR_input
//! rows reference its own output LTDV; gating on those would deadlock
//! the publisher against itself. Lazy-mints over pre-existing concrete
//! TDVs have NULL `producer_logical_function_run_id` and *are*
//! legitimate upstreams.

use sea_query::{
    Expr, ExprTrait, Iden, JoinType, Order, Query, SelectStatement, SimpleExpr, SubQueryStatement,
};
use ta_objects::SeaValue;
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
    LogicalTableDataVersionPointerDBIden,
};
use td_objects::types::basic::{AtTime, FunctionRunStatus};

/// Joined-with-AS alias for the latest-pointer event row.
#[derive(Iden, Clone, Copy)]
pub(crate) struct Lp;

/// Joined-with-AS alias for the producer LFR (canonical status source
/// for an LTDV). LEFT JOIN — NULL when the LTDV is a lazy-mint over a
/// pre-2.0 concrete TDV (no logical-layer producer row exists).
#[derive(Iden, Clone, Copy)]
pub(crate) struct ProducerLfr;

/// True when the producer LFR's status is one of `statuses`, or when the
/// producer is NULL (lazy-mint over a pre-2.0 concrete TDV is treated as
/// terminal for status purposes). Requires that the caller has joined
/// the producer LFR aliased as [`ProducerLfr`].
pub(crate) fn producer_status_in_or_lazy(statuses: &[FunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_null()
        .or(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_in(values))
}

/// True when the producer LFR is present (not a lazy-mint) AND its
/// status is one of `statuses`. NULL producer (lazy-mint) does not
/// qualify — use when "lazy = terminal/healthy" should NOT trigger the
/// predicate (e.g. cascade "is upstream failed?" checks).
pub(crate) fn producer_status_in_strict(statuses: &[FunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_not_null()
        .and(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_in(values))
}

/// True when the producer LFR is present AND its status is NOT one of
/// `statuses`. NULL producer (lazy-mint) does not qualify — readiness
/// treats lazy-mints as terminal/done. Use as the readiness-blocking
/// predicate (status not in {Done, Committed}).
pub(crate) fn producer_status_not_in(statuses: &[FunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_not_null()
        .and(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_not_in(values))
}

/// Shape parameters. `at_bound = Some(at)` bounds the latest-pointer
/// subquery by `set_on <= at` (readiness, blocked).
pub(crate) struct ChainParams<'a> {
    pub(crate) at_bound: Option<&'a AtTime>,
}

/// Build a `SelectStatement` joining `logical_function_run_inputs →
/// logical_table_data_versions → latest pointer event` and the
/// NULL-safe self-loop exclusion in the WHERE. Caller adds projection,
/// outer scope, and any additional filters on `LTDV.status` or
/// `Lp.status`.
pub(crate) fn build_chain(params: ChainParams<'_>) -> SelectStatement {
    let ChainParams { at_bound } = params;

    // Latest pointer's id per LTDV by `(set_on DESC, id DESC)`.
    // Returning the id (rather than `MAX(set_on)`) lets the outer JOIN
    // match on a unique row even when multiple events share `set_on`.
    let mut latest_id_subq_select = Query::select()
        .column(LogicalTableDataVersionPointerDBIden::Id)
        .from(LogicalTableDataVersionPointerDBIden::Table)
        .and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
            ))
            .equals((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
        )
        .to_owned();
    if let Some(at) = at_bound {
        latest_id_subq_select.and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::SetOn,
            ))
            .lte(at.as_sea_value()),
        );
    }
    latest_id_subq_select
        .order_by(LogicalTableDataVersionPointerDBIden::SetOn, Order::Desc)
        .order_by(LogicalTableDataVersionPointerDBIden::Id, Order::Desc)
        .limit(1);
    let latest_id_subq = Expr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(latest_id_subq_select)),
    );

    let mut stmt = Query::select()
        .from(LogicalFunctionRunInputDBIden::Table)
        .to_owned();

    stmt.join(
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
    )
    .join_as(
        JoinType::InnerJoin,
        LogicalTableDataVersionPointerDBIden::Table,
        Lp,
        Expr::col((Lp, LogicalTableDataVersionPointerDBIden::Id)).eq(latest_id_subq),
    )
    // Producer LFR — canonical status source. LEFT JOIN because
    // lazy-mints over pre-2.0 concrete TDVs have NULL producer.
    .join_as(
        JoinType::LeftJoin,
        LogicalFunctionRunDBIden::Table,
        ProducerLfr,
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
    )
    // Self-loop exclusion (NULL-safe — lazy-mints have NULL producer).
    .and_where(
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .is_null()
        .or(Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .ne(Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        )))),
    );

    stmt
}
