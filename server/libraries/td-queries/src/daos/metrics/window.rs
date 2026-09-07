//
// Copyright 2026 Tabsdata Inc.
//

//! The window driver and the fenced probes hanging off it. The RLS qual
//! (`current_setting`) is planner-unestimable, so every statement drives off
//! the `physical_function_runs (ended_on)` index and reaches everything else
//! through `OFFSET 0` laterals, staying Θ(window) regardless of history.

use crate::daos::metrics::Sq;
use crate::daos::metrics::filters::apply_active_entity_filter;
use crate::primitives::scd2::open_interval_at;
use chrono::{DateTime, Utc};
use sea_query::{
    Expr, ExprTrait, IntoColumnRef, IntoIden, JoinType, Query, SelectStatement, SimpleExpr,
};
use ta_objects::errors::SqlError;
use ta_objects::{Scd2Dao, SeaValue};
use td_error::{TdError, td_error};
use td_objects::dxo::logical::{
    FunctionRunDBWithNamesIden, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden, TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunDBIden};
use td_objects::dxo::query_filters::{TimeWindow, WindowUnit};
use td_objects::types::basic::LogicalFunctionRunStatus;

#[td_error]
enum WindowBoundsError {
    #[error("window bound {ms} ms is not a representable timestamp")]
    Unrepresentable { ms: i64 } = 0,
}

/// The window as TIMESTAMPTZ bounds on `ended_on`. Anchor-aligned windows
/// can start mid-bucket, so the filter is on the raw timestamp and only the
/// `GROUP BY` uses the bucket-floored column.
pub(crate) struct WindowBounds {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl TryFrom<&TimeWindow> for WindowBounds {
    type Error = SqlError;

    fn try_from(window: &TimeWindow) -> Result<Self, SqlError> {
        window
            .validate()
            .map_err(|e| SqlError::DtoConversionError(TdError::new(e)))?;
        let bound = |ms: i64| {
            DateTime::<Utc>::from_timestamp_millis(ms).ok_or_else(|| {
                SqlError::DtoConversionError(TdError::new(WindowBoundsError::Unrepresentable {
                    ms,
                }))
            })
        };
        Ok(WindowBounds {
            start: bound(window.start_ms)?,
            end: bound(window.end_ms)?,
        })
    }
}

/// FROM the windowed current attempts, then one fenced hop to the run by
/// primary key. [`fr_probe`] keeps the view's alias so downstream is
/// shape-agnostic.
pub(crate) fn from_windowed_runs(stmt: &mut SelectStatement, bounds: &WindowBounds) {
    let ended_on = (
        PhysicalFunctionRunDBIden::Table,
        PhysicalFunctionRunDBIden::EndedOn,
    );
    stmt.from(PhysicalFunctionRunDBIden::Table)
        .and_where(Expr::col(ended_on).gte(bounds.start))
        .and_where(Expr::col(ended_on).lt(bounds.end))
        .and_where(open_interval_at((
            PhysicalFunctionRunDBIden::Table,
            <PhysicalFunctionRunDB as Scd2Dao>::VALID_TO,
        )))
        .join_lateral(
            JoinType::InnerJoin,
            fr_probe(),
            FunctionRunDBWithNamesIden::Table,
            Expr::val(true),
        );
    apply_active_entity_filter(
        stmt,
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::ProjectId,
        ),
    );
    apply_ended_status_filter(stmt);
}

/// Ids only; the aggregators attach names after aggregation. Keying on the
/// primary key reaches the row by index instead of re-expanding the view per
/// row under RLS's ~1-row estimates.
fn fr_probe() -> SelectStatement {
    let mut stmt = Query::select();
    for col in [
        LogicalFunctionRunDBIden::Id,
        LogicalFunctionRunDBIden::CollectionId,
        LogicalFunctionRunDBIden::GroupId,
        LogicalFunctionRunDBIden::ProjectId,
        LogicalFunctionRunDBIden::Status,
        LogicalFunctionRunDBIden::TransactionId,
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(LogicalFunctionRunDBIden::Table, Sq::P)
        .and_where(Expr::col((Sq::P, LogicalFunctionRunDBIden::Id)).equals((
            PhysicalFunctionRunDBIden::Table,
            PhysicalFunctionRunDBIden::LogicalFunctionRunId,
        )))
        .offset(0)
        .take()
}

/// Fenced probe of the LTDV view keyed by `inner_col = outer`; ids and
/// magnitudes only.
fn tdv_probe<I, O>(inner_col: I, outer: O) -> SelectStatement
where
    I: IntoIden + Copy,
    O: IntoColumnRef,
{
    let mut stmt = Query::select();
    for col in [
        LogicalTableDataVersionDBIden::Id.into_iden(),
        LogicalTableDataVersionDBIden::TableId.into_iden(),
        LogicalTableDataVersionDBIden::ProjectId.into_iden(),
        LogicalTableDataVersionDBIden::CollectionId.into_iden(),
        LogicalTableDataVersionDBIden::GroupId.into_iden(),
        LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId.into_iden(),
        LogicalTableDataVersionDBIden::FunctionParamPos.into_iden(),
        LogicalTableDataVersionDBIden::Private.into_iden(),
        TableDataVersionDBWithNamesIden::RowCount.into_iden(),
        TableDataVersionDBWithNamesIden::DataBytes.into_iden(),
        TableDataVersionDBWithNamesIden::HasData.into_iden(),
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(TableDataVersionDBWithNamesIden::Table, Sq::P)
        .and_where(Expr::col((Sq::P, inner_col)).equals(outer))
        .offset(0)
        .take()
}

pub(crate) fn join_source_tdv(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        tdv_probe(
            LogicalTableDataVersionDBIden::Id,
            (
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            ),
        ),
        TableDataVersionDBWithNamesIden::Table,
        Expr::val(true),
    );
}

pub(crate) fn join_produced_tdv(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        tdv_probe(
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            (
                FunctionRunDBWithNamesIden::Table,
                LogicalFunctionRunDBIden::Id,
            ),
        ),
        TableDataVersionDBWithNamesIden::Table,
        Expr::val(true),
    );
}

/// Fenced probe of the run's inputs; unfenced, wide windows cross-join the
/// whole inputs table against the window runs.
fn lfri_probe() -> SelectStatement {
    let mut stmt = Query::select();
    for col in [
        LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        LogicalFunctionRunInputDBIden::RequirementDependencyPos,
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(LogicalFunctionRunInputDBIden::Table, Sq::P)
        .and_where(
            Expr::col((Sq::P, LogicalFunctionRunInputDBIden::LogicalFunctionRunId)).equals((
                FunctionRunDBWithNamesIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        )
        .offset(0)
        .take()
}

pub(crate) fn join_consumed_lfri(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        lfri_probe(),
        LogicalFunctionRunInputDBIden::Table,
        Expr::val(true),
    );
}

/// A user-dependency input row: it names a version and its dependency
/// position is not a `td.fn_state__*` plumbing slot.
pub(crate) fn lfri_qualified() -> SimpleExpr {
    Expr::col((
        LogicalFunctionRunInputDBIden::Table,
        LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
    ))
    .is_not_null()
    .and(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementDependencyPos,
        ))
        .gte(0),
    )
}

/// Redundant with the window in steady state; closes the corner where the
/// current attempt is gone while the run's sticky status is still ENDED.
fn apply_ended_status_filter(stmt: &mut SelectStatement) {
    stmt.and_where(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::Status,
        ))
        .is_in(
            LogicalFunctionRunStatus::ENDED
                .iter()
                .map(SeaValue::as_sea_value),
        ),
    );
}

/// The bucket is read off the window driver, not the resolved run.
pub(crate) fn bucket_column_pfr(unit: WindowUnit) -> PhysicalFunctionRunDBIden {
    match unit {
        WindowUnit::Hour => PhysicalFunctionRunDBIden::EndedOnHourMs,
        WindowUnit::Day => PhysicalFunctionRunDBIden::EndedOnDayMs,
        WindowUnit::Week => PhysicalFunctionRunDBIden::EndedOnWeekMs,
        WindowUnit::Month | WindowUnit::Quarter | WindowUnit::Year => {
            PhysicalFunctionRunDBIden::EndedOnMonthMs
        }
    }
}

pub(crate) fn bucket_expr(unit: WindowUnit) -> SimpleExpr {
    Expr::col((PhysicalFunctionRunDBIden::Table, bucket_column_pfr(unit)))
}
