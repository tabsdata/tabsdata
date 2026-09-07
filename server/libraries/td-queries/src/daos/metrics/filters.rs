//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{Expr, ExprTrait, IntoColumnRef, IntoIden, Query, SelectStatement};
use ta_objects::SeaValue;
use td_objects::dxo::collection::ActiveCollectionDBIden;
use td_objects::dxo::logical::{
    FunctionRunDBWithNamesIden, LogicalFunctionRunDBIden, LogicalTableDataVersionDBIden,
    TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::MetricsRequest;
use td_objects::dxo::project::ActiveProjectDBIden;
use td_objects::dxo::query_filters::DimensionFilter;
use td_objects::types::basic::{LogicalFunctionRunStatus, Private};

/// `function_param_pos >= 0 AND private = FALSE` on an LTDV-shaped table.
pub(crate) fn apply_user_table_filter<T: IntoIden>(stmt: &mut SelectStatement, table: T) {
    let table = table.into_iden();
    stmt.and_where(
        Expr::col((
            table.clone(),
            LogicalTableDataVersionDBIden::FunctionParamPos,
        ))
        .gte(0),
    )
    .and_where(
        Expr::col((table, LogicalTableDataVersionDBIden::Private))
            .eq(Private::FALSE.as_sea_value()),
    );
}

/// Both must still resolve in their `*_active` view; `collections_active`
/// only gates the collection's own deletion, hence the separate project
/// probe. Ids are globally unique, so each probe keys on id alone.
pub(crate) fn apply_active_entity_filter<C, P>(
    stmt: &mut SelectStatement,
    collection_id: C,
    project_id: P,
) where
    C: IntoColumnRef,
    P: IntoColumnRef,
{
    stmt.and_where(Expr::exists(
        Query::select()
            .expr(Expr::val(1))
            .from(ActiveCollectionDBIden::Table)
            .and_where(
                Expr::col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Id))
                    .equals(collection_id),
            )
            .take(),
    ))
    .and_where(Expr::exists(
        Query::select()
            .expr(Expr::val(1))
            .from(ActiveProjectDBIden::Table)
            .and_where(
                Expr::col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Id)).equals(project_id),
            )
            .take(),
    ));
}

pub(crate) fn filter_source_tdv_active(stmt: &mut SelectStatement) {
    apply_active_entity_filter(
        stmt,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::ProjectId,
        ),
    );
}

pub(crate) fn apply_dim_filter<Id, C>(
    stmt: &mut SelectStatement,
    filter: &DimensionFilter<Id>,
    col: C,
) where
    Id: SeaValue,
    C: IntoColumnRef + Clone,
{
    if !filter.include.is_empty() {
        stmt.and_where(
            Expr::col(col.clone()).is_in(filter.include.iter().map(SeaValue::as_sea_value)),
        );
    }
    if !filter.exclude.is_empty() {
        stmt.and_where(Expr::col(col).is_not_in(filter.exclude.iter().map(SeaValue::as_sea_value)));
    }
}

/// An edge survives if either endpoint passes `include` and both clear
/// `exclude`.
pub(crate) fn apply_dim_filter_either_endpoint<Id, C1, C2>(
    stmt: &mut SelectStatement,
    filter: &DimensionFilter<Id>,
    source_col: C1,
    target_col: C2,
) where
    Id: SeaValue,
    C1: IntoColumnRef + Clone,
    C2: IntoColumnRef + Clone,
{
    if !filter.include.is_empty() {
        let vals: Vec<_> = filter.include.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(
            Expr::col(source_col.clone())
                .is_in(vals.clone())
                .or(Expr::col(target_col.clone()).is_in(vals)),
        );
    }
    if !filter.exclude.is_empty() {
        let vals: Vec<_> = filter.exclude.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(Expr::col(source_col).is_not_in(vals.clone()))
            .and_where(Expr::col(target_col).is_not_in(vals));
    }
}

pub(crate) fn apply_status_filter<C: IntoColumnRef>(
    stmt: &mut SelectStatement,
    statuses: &[LogicalFunctionRunStatus],
    col: C,
) {
    if !statuses.is_empty() {
        stmt.and_where(Expr::col(col).is_in(statuses.iter().map(SeaValue::as_sea_value)));
    }
}

pub(crate) fn apply_run_status_filter(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_status_filter(
        stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::Status,
        ),
    );
}

/// Collection and group filters on the run's own dims.
pub(crate) fn apply_run_dim_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_dim_filter(
        stmt,
        &req.collections,
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ),
    );
    apply_dim_filter(
        stmt,
        &req.groups,
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::GroupId,
        ),
    );
}

/// Collection and group filters on the table version's own dims.
pub(crate) fn apply_tdv_dim_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_dim_filter(
        stmt,
        &req.collections,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
    );
    apply_dim_filter(
        stmt,
        &req.groups,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::GroupId,
        ),
    );
}
