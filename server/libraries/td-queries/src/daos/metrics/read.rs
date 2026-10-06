//
// Copyright 2026 Tabsdata Inc.
//

//! The `/metrics` read: one statement, grouped down to the table so the
//! caller can count distinct tables across statuses.

use chrono::{DateTime, Utc};
use sea_query::{Alias, Expr, ExprTrait, Func, Iden, IntoIden, Query, SelectStatement, SimpleExpr};
use ta_objects::SeaValue;
use td_error::{TdError, td_error};
use td_objects::dxo::metrics::{Dimension, MetricsRequest};
use td_objects::dxo::metrics_facts::MetricsFactDBIden;
use td_objects::dxo::query_filters::DimensionFilter;
use td_objects::types::basic::{CurrentAttempt, FactSide, Private};
use td_objects::types::visible_projects::VisibleProjects;

#[td_error]
enum WindowBoundError {
    #[error("window bound {ms} ms is not a representable timestamp")]
    Unrepresentable { ms: i64 } = 0,
}

#[derive(Iden, Clone, Copy)]
enum Aggregate {
    Side,
    EdgeKind,
    FromCollectionId,
    FromGroupId,
    CollectionId,
    GroupId,
    ProjectId,
    RunStatus,
    TableId,
    FnRuns,
    Versions,
    VersionsWithData,
    VersionsUnconsumed,
    RowCount,
    DataBytes,
}

const KEYS: [(Aggregate, MetricsFactDBIden); 9] = [
    (Aggregate::Side, MetricsFactDBIden::Side),
    (Aggregate::EdgeKind, MetricsFactDBIden::EdgeKind),
    (
        Aggregate::FromCollectionId,
        MetricsFactDBIden::FromCollectionId,
    ),
    (Aggregate::FromGroupId, MetricsFactDBIden::FromGroupId),
    (Aggregate::CollectionId, MetricsFactDBIden::CollectionId),
    (Aggregate::GroupId, MetricsFactDBIden::GroupId),
    (Aggregate::ProjectId, MetricsFactDBIden::ProjectId),
    (Aggregate::RunStatus, MetricsFactDBIden::RunStatus),
    (Aggregate::TableId, MetricsFactDBIden::TableId),
];

fn col<T: IntoIden, C: IntoIden>(table: T, column: C) -> SimpleExpr {
    Expr::col((table.into_iden(), column.into_iden()))
}

fn count_where(condition: SimpleExpr) -> SimpleExpr {
    Func::count(Expr::case(condition, 1).finally(Expr::value(sea_query::Value::Int(None)))).into()
}

fn sum(expr: SimpleExpr) -> SimpleExpr {
    let sum: SimpleExpr = Func::sum(expr).into();
    let cast: SimpleExpr = Func::cast_as(sum, Alias::new("BIGINT")).into();
    Func::coalesce([cast, Expr::val(0i64)]).into()
}

pub(crate) fn apply_dim_filter<Id: SeaValue>(
    stmt: &mut SelectStatement,
    filter: &DimensionFilter<Id>,
    column: SimpleExpr,
) {
    if !filter.include.is_empty() {
        stmt.and_where(
            column
                .clone()
                .is_in(filter.include.iter().map(SeaValue::as_sea_expr)),
        );
    }
    if !filter.exclude.is_empty() {
        stmt.and_where(column.is_not_in(filter.exclude.iter().map(SeaValue::as_sea_expr)));
    }
}

/// Either endpoint passes `include`, both clear `exclude`; a NULL endpoint
/// neither passes nor fails.
fn apply_either_endpoint_filter<Id: SeaValue>(
    stmt: &mut SelectStatement,
    filter: &DimensionFilter<Id>,
    source: SimpleExpr,
    target: SimpleExpr,
) {
    if !filter.include.is_empty() {
        let values: Vec<_> = filter.include.iter().map(SeaValue::as_sea_expr).collect();
        stmt.and_where(
            source
                .clone()
                .is_in(values.clone())
                .or(target.clone().is_in(values)),
        );
    }
    if !filter.exclude.is_empty() {
        let values: Vec<_> = filter.exclude.iter().map(SeaValue::as_sea_expr).collect();
        stmt.and_where(
            source
                .clone()
                .is_null()
                .or(source.is_not_in(values.clone())),
        )
        .and_where(target.clone().is_null().or(target.is_not_in(values)));
    }
}

fn at(ms: i64) -> Result<DateTime<Utc>, TdError> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| WindowBoundError::Unrepresentable { ms }.into())
}

pub(crate) fn per_table_query(
    req: &MetricsRequest,
    visible_projects: &VisibleProjects,
) -> Result<SelectStatement, TdError> {
    use MetricsFactDBIden as F;
    let f = F::Table;
    let attempt = || col(f, F::Side).eq(FactSide::Attempt.as_sea_expr());
    let edge_first = match req.dim {
        Dimension::Collection => col(f, F::EdgeFirst),
        Dimension::Group => col(f, F::EdgeFirstGroup),
    };

    let window_end = at(*req.window.end_ms)?;

    let mut stmt = Query::select();
    for (alias, column) in KEYS {
        stmt.expr_as(col(f, column), alias);
    }
    stmt.expr_as(count_where(attempt().or(edge_first)), Aggregate::FnRuns)
        .expr_as(count_where(attempt().not()), Aggregate::Versions)
        .expr_as(
            count_where(
                col(f, F::Side)
                    .eq(FactSide::Written.as_sea_expr())
                    .and(col(f, F::HasData)),
            ),
            Aggregate::VersionsWithData,
        )
        .expr_as(
            count_where(
                col(f, F::Side).eq(FactSide::Written.as_sea_expr()).and(
                    col(f, F::FirstConsumedOn)
                        .is_null()
                        .or(col(f, F::FirstConsumedOn).gte(window_end)),
                ),
            ),
            Aggregate::VersionsUnconsumed,
        )
        .expr_as(sum(col(f, F::RowCount)), Aggregate::RowCount)
        .expr_as(sum(col(f, F::DataBytes)), Aggregate::DataBytes)
        .from(f)
        .and_where(col(f, F::Ts).gte(at(*req.window.start_ms)?))
        .and_where(col(f, F::Ts).lt(window_end))
        .and_where(col(f, F::RunStatus).is_in(req.statuses.iter().map(SeaValue::as_sea_expr)))
        .cond_where(visible_projects.narrows(col(f, F::ProjectId)));
    if !*req.include_replaced {
        stmt.and_where(col(f, F::Current).eq(CurrentAttempt::TRUE.as_sea_expr()));
    }
    if !*req.include_private {
        stmt.and_where(col(f, F::Private).eq(Private::FALSE.as_sea_expr()));
    }
    apply_either_endpoint_filter(
        &mut stmt,
        &req.collections,
        col(f, F::FromCollectionId),
        col(f, F::CollectionId),
    );
    apply_either_endpoint_filter(
        &mut stmt,
        &req.groups,
        col(f, F::FromGroupId),
        col(f, F::GroupId),
    );
    for (_, column) in KEYS {
        stmt.group_by_col((f, column));
    }
    Ok(stmt)
}
