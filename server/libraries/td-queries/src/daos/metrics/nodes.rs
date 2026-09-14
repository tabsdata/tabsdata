//
// Copyright 2026 Tabsdata Inc.
//

use crate::daos::metrics::filters::{
    apply_run_dim_filters, apply_run_status_filter, apply_user_table_filter,
    filter_source_tdv_active,
};
use crate::daos::metrics::window::{
    WindowBounds, bucket_column_pfr, bucket_expr, from_windowed_runs, join_produced_tdv,
    join_source_tdv, lfri_qualified,
};
use crate::daos::metrics::{
    Col, Sq, bigint_zero, coalesce_sum, fr_col, kind_lit, null_str, tdv_col,
};
use sea_query::{Expr, ExprTrait, Func, JoinType, Order, Query, SelectStatement};
use ta_objects::SeaValue;
use td_objects::dxo::collection::{ActiveCollectionDBIden, ResolvedCollectionDBIden};
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::{
    FunctionRunDBWithNamesIden, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden, TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::{Dimension, MetricsRequest, NodeKind};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
use td_objects::dxo::project::{ActiveProjectDBIden, ProjectDBIden};
use td_objects::types::basic::HasData;
use td_objects::types::visible_projects::VisibleProjects;

/// Aggregate by ids, then attach dim and project names once. `dim=group`
/// folds collections together (NULL collection id and name).
pub(crate) fn node_aggregates_query(
    req: &MetricsRequest,
    bounds: &WindowBounds,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let by_collection = matches!(req.dim, Dimension::Collection);

    let mut inner = Query::select();
    from_windowed_runs(&mut inner, bounds);
    inner.expr_as(bucket_expr(req.unit), Col::Bucket);
    if by_collection {
        inner.expr_as(
            fr_col(LogicalFunctionRunDBIden::CollectionId),
            Col::CollectionId,
        );
    }
    inner
        .expr_as(fr_col(LogicalFunctionRunDBIden::GroupId), Col::GroupId)
        .expr_as(fr_col(LogicalFunctionRunDBIden::ProjectId), Col::ProjectId)
        .expr_as(fr_col(LogicalFunctionRunDBIden::Status), Col::Status)
        .expr_as(Func::count(Expr::val(1)), Col::FnRuns)
        .expr_as(
            Func::count_distinct(fr_col(LogicalFunctionRunDBIden::TransactionId)),
            Col::Transactions,
        );
    for (sq, from, to) in [
        (Sq::Outs, Col::TablesOut, Col::TableVersionsOut),
        (Sq::Outs, Col::RowsOut, Col::RowCountOut),
        (Sq::Outs, Col::BytesOut, Col::DataBytesOut),
        (Sq::Ins, Col::TablesIn, Col::TableVersionsIn),
        (Sq::Ins, Col::RowsIn, Col::RowCountIn),
        (Sq::Ins, Col::BytesIn, Col::DataBytesIn),
    ] {
        inner.expr_as(coalesce_sum((sq, from)), to);
    }
    inner
        .join_lateral(
            JoinType::LeftJoin,
            per_lfr_input_aggregates_lateral(),
            Sq::Ins,
            Expr::val(true),
        )
        .join_lateral(
            JoinType::LeftJoin,
            per_lfr_output_aggregates_lateral(),
            Sq::Outs,
            Expr::val(true),
        );
    apply_run_status_filter(&mut inner, req);
    apply_run_dim_filters(&mut inner, req);
    inner.group_by_col((
        PhysicalFunctionRunDBIden::Table,
        bucket_column_pfr(req.unit),
    ));
    if by_collection {
        inner.group_by_col((
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ));
    }
    for col in [
        LogicalFunctionRunDBIden::GroupId,
        LogicalFunctionRunDBIden::ProjectId,
        LogicalFunctionRunDBIden::Status,
    ] {
        inner.group_by_col((FunctionRunDBWithNamesIden::Table, col));
    }

    let mut outer = Query::select();
    outer
        .expr(Expr::col((Sq::A, Col::Bucket)))
        .expr_as(kind_lit(NodeKind::Dim), Col::Kind);
    if by_collection {
        outer.expr(Expr::col((Sq::A, Col::CollectionId))).expr_as(
            Expr::col((Sq::C, ActiveCollectionDBIden::Name)),
            Col::CollectionName,
        );
    } else {
        outer
            .expr_as(null_str(), Col::CollectionId)
            .expr_as(null_str(), Col::CollectionName);
    }
    outer
        .expr(Expr::col((Sq::A, Col::GroupId)))
        .expr_as(Expr::col((Sq::G, GroupDBIden::Name)), Col::GroupName)
        .expr(Expr::col((Sq::A, Col::ProjectId)))
        .expr_as(
            Expr::col((Sq::Pr, ActiveProjectDBIden::Name)),
            Col::ProjectName,
        );
    for c in [
        Col::Status,
        Col::FnRuns,
        Col::Transactions,
        Col::TableVersionsOut,
        Col::RowCountOut,
        Col::DataBytesOut,
        Col::TableVersionsIn,
        Col::RowCountIn,
        Col::DataBytesIn,
    ] {
        outer.expr(Expr::col((Sq::A, c)));
    }
    // Not summable here; the service layer fills it from node_output_tables_query.
    outer.expr_as(bigint_zero(), Col::LogicalTablesOut);
    outer.from_subquery(inner, Sq::A);
    if by_collection {
        outer.join_as(
            JoinType::LeftJoin,
            ResolvedCollectionDBIden::Table,
            Sq::C,
            Expr::col((Sq::C, ActiveCollectionDBIden::Id)).equals((Sq::A, Col::CollectionId)),
        );
    }
    outer
        .join_as(
            JoinType::LeftJoin,
            GroupDBIden::Table,
            Sq::G,
            Expr::col((Sq::G, GroupDBIden::Id)).equals((Sq::A, Col::GroupId)),
        )
        .join_as(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            Sq::Pr,
            Expr::col((Sq::Pr, ActiveProjectDBIden::Id)).equals((Sq::A, Col::ProjectId)),
        )
        .cond_where(visible_projects.narrows(Expr::col((Sq::A, Col::ProjectId))))
        .order_by((Sq::A, Col::Bucket), Order::Desc);
    outer
}

/// Whole-select aggregate correlated on the outer run: always exactly one
/// row, NULLs absorbed by the outer COALESCE.
fn per_lfr_input_aggregates_lateral() -> SelectStatement {
    let mut stmt = Query::select();
    stmt.expr_as(
        Func::count_distinct(Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        ))),
        Col::TablesIn,
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::RowCount,
        )),
        Col::RowsIn,
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::DataBytes,
        )),
        Col::BytesIn,
    )
    .from(LogicalFunctionRunInputDBIden::Table);
    join_source_tdv(&mut stmt);
    stmt.and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .equals((
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::Id,
        )),
    )
    .and_where(lfri_qualified());
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    filter_source_tdv_active(&mut stmt);
    stmt
}

/// Single view entry through the producer index, so no inner fence.
fn per_lfr_output_aggregates_lateral() -> SelectStatement {
    let mut stmt = Query::select();
    stmt.expr_as(Func::count(Expr::val(1)), Col::TablesOut)
        .expr_as(
            coalesce_sum((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::RowCount,
            )),
            Col::RowsOut,
        )
        .expr_as(
            coalesce_sum((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::DataBytes,
            )),
            Col::BytesOut,
        )
        .from(TableDataVersionDBWithNamesIden::Table)
        .and_where(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .equals((
                FunctionRunDBWithNamesIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::HasData,
            ))
            .eq(HasData::TRUE.as_sea_value()),
        );
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    stmt
}

/// Deliberately without the `has_data` filter of the output aggregates: a
/// table with only no-data versions still counts as an output table.
pub(crate) fn node_output_tables_query(
    req: &MetricsRequest,
    bounds: &WindowBounds,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, bounds);
    join_produced_tdv(&mut stmt);
    let collection_id = match req.dim {
        Dimension::Collection => fr_col(LogicalFunctionRunDBIden::CollectionId),
        Dimension::Group => null_str(),
    };
    stmt.distinct()
        .expr_as(bucket_expr(req.unit), Col::Bucket)
        .expr_as(fr_col(LogicalFunctionRunDBIden::ProjectId), Col::ProjectId)
        .expr_as(collection_id, Col::CollectionId)
        .expr_as(fr_col(LogicalFunctionRunDBIden::GroupId), Col::GroupId)
        .expr_as(fr_col(LogicalFunctionRunDBIden::Status), Col::Status)
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::TableId),
            Col::TableId,
        );
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    apply_run_status_filter(&mut stmt, req);
    apply_run_dim_filters(&mut stmt, req);
    stmt.cond_where(visible_projects.narrows(fr_col(LogicalFunctionRunDBIden::ProjectId)));
    stmt
}
