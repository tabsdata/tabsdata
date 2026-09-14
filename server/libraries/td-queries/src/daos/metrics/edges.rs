//
// Copyright 2026 Tabsdata Inc.
//

//! The edge union: real edges (consumed version → run), synthetic sources
//! (publisher runs with no qualifying input), egress (runs with inputs but no
//! visible outputs) and unused (output versions nobody consumed).

use crate::daos::metrics::filters::{
    apply_dim_filter_either_endpoint, apply_run_dim_filters, apply_run_status_filter,
    apply_tdv_dim_filters, apply_user_table_filter, filter_source_tdv_active,
};
use crate::daos::metrics::window::{
    WindowBounds, bucket_expr, from_windowed_runs, join_consumed_lfri, join_produced_tdv,
    join_source_tdv, lfri_qualified,
};
use crate::daos::metrics::{
    Col, Sq, bigint_zero, coalesce_sum, fr_col, kind_lit, null_str, tdv_col,
};
use sea_query::{
    Expr, ExprTrait, Func, IntoIden, JoinType, NullOrdering, Order, Query, SelectStatement,
    SimpleExpr, UnionType,
};
use td_objects::dxo::collection::{ActiveCollectionDBIden, ResolvedCollectionDBIden};
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::{
    FunctionRunDBWithNamesIden, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden, TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::{Dimension, MetricsRequest, NodeKind};
use td_objects::dxo::project::{ActiveProjectDBIden, ProjectDBIden};
use td_objects::types::visible_projects::VisibleProjects;

fn run_dim(dim: Dimension) -> LogicalFunctionRunDBIden {
    match dim {
        Dimension::Collection => LogicalFunctionRunDBIden::CollectionId,
        Dimension::Group => LogicalFunctionRunDBIden::GroupId,
    }
}

fn tdv_dim(dim: Dimension) -> LogicalTableDataVersionDBIden {
    match dim {
        Dimension::Collection => LogicalTableDataVersionDBIden::CollectionId,
        Dimension::Group => LogicalTableDataVersionDBIden::GroupId,
    }
}

/// Id-only projection shared by every branch.
fn edge_branch_select(
    stmt: &mut SelectStatement,
    req: &MetricsRequest,
    source_kind: NodeKind,
    source: SimpleExpr,
    target_kind: NodeKind,
    target: SimpleExpr,
) {
    stmt.expr_as(bucket_expr(req.unit), Col::Bucket)
        .expr_as(kind_lit(source_kind), Col::SourceKind)
        .expr_as(source, Col::Source)
        .expr_as(kind_lit(target_kind), Col::TargetKind)
        .expr_as(target, Col::Target)
        .expr_as(fr_col(LogicalFunctionRunDBIden::Status), Col::Status)
        .expr_as(fr_col(LogicalFunctionRunDBIden::Id), Col::LfrId)
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::Id),
            Col::SourceLtdvId,
        )
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::TableId),
            Col::TableId,
        )
        .expr_as(
            tdv_col(TableDataVersionDBWithNamesIden::RowCount),
            Col::SourceRowCount,
        )
        .expr_as(
            tdv_col(TableDataVersionDBWithNamesIden::DataBytes),
            Col::SourceDataBytes,
        )
        .expr_as(fr_col(LogicalFunctionRunDBIden::ProjectId), Col::ProjectId);
}

fn apply_edge_user_status_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_user_table_filter(stmt, TableDataVersionDBWithNamesIden::Table);
    apply_run_status_filter(stmt, req);
}

fn edge_real_branch(req: &MetricsRequest, bounds: &WindowBounds) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, bounds);
    stmt.distinct();
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        tdv_col(tdv_dim(req.dim)),
        NodeKind::Dim,
        fr_col(run_dim(req.dim)),
    );
    join_consumed_lfri(&mut stmt);
    join_source_tdv(&mut stmt);
    stmt.and_where(lfri_qualified());
    filter_source_tdv_active(&mut stmt);
    apply_edge_user_status_filters(&mut stmt, req);
    apply_dim_filter_either_endpoint(
        &mut stmt,
        &req.collections,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ),
    );
    apply_dim_filter_either_endpoint(
        &mut stmt,
        &req.groups,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::GroupId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::GroupId,
        ),
    );
    stmt
}

fn edge_synth_branch(req: &MetricsRequest, bounds: &WindowBounds) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, bounds);
    join_produced_tdv(&mut stmt);
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::ExternalSource,
        null_str(),
        NodeKind::Dim,
        fr_col(run_dim(req.dim)),
    );
    stmt.join(
        JoinType::LeftJoin,
        LogicalFunctionRunInputDBIden::Table,
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .equals((
            FunctionRunDBWithNamesIden::Table,
            LogicalFunctionRunDBIden::Id,
        ))
        .and(lfri_qualified()),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .is_null(),
    );
    apply_edge_user_status_filters(&mut stmt, req);
    apply_run_dim_filters(&mut stmt, req);
    stmt
}

fn edge_egress_branch(req: &MetricsRequest, bounds: &WindowBounds) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, bounds);
    stmt.distinct();
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        fr_col(run_dim(req.dim)),
        NodeKind::ExternalDestination,
        null_str(),
    );
    join_consumed_lfri(&mut stmt);
    join_source_tdv(&mut stmt);
    stmt.and_where(lfri_qualified());
    filter_source_tdv_active(&mut stmt);
    let mut produces_visible_output = Query::select();
    produces_visible_output
        .expr(Expr::val(1))
        .from(LogicalTableDataVersionDBIden::Table)
        .and_where(
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .equals((
                FunctionRunDBWithNamesIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        );
    apply_user_table_filter(
        &mut produces_visible_output,
        LogicalTableDataVersionDBIden::Table,
    );
    stmt.and_where(Expr::exists(produces_visible_output).not());
    apply_edge_user_status_filters(&mut stmt, req);
    apply_run_dim_filters(&mut stmt, req);
    stmt
}

fn edge_unused_branch(req: &MetricsRequest, bounds: &WindowBounds) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, bounds);
    join_produced_tdv(&mut stmt);
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        tdv_col(tdv_dim(req.dim)),
        NodeKind::Unused,
        null_str(),
    );
    let consumed = Query::select()
        .expr(Expr::val(1))
        .from(LogicalFunctionRunInputDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            ))
            .equals((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
        )
        .and_where(lfri_qualified())
        .take();
    stmt.and_where(Expr::exists(consumed).not());
    apply_edge_user_status_filters(&mut stmt, req);
    apply_tdv_dim_filters(&mut stmt, req);
    stmt
}

fn edge_inner_union(req: &MetricsRequest, bounds: &WindowBounds) -> SelectStatement {
    let mut inner = edge_real_branch(req, bounds);
    inner.union(UnionType::All, edge_synth_branch(req, bounds));
    inner.union(UnionType::All, edge_egress_branch(req, bounds));
    inner.union(UnionType::All, edge_unused_branch(req, bounds));
    inner
}

/// Aggregate the id-only branches, then attach names once: per-window-row
/// name joins measured ~4× slower.
pub(crate) fn edge_aggregates_query(
    req: &MetricsRequest,
    bounds: &WindowBounds,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let (dim_table, dim_id, dim_name) = match req.dim {
        Dimension::Collection => (
            ResolvedCollectionDBIden::Table.into_iden(),
            ActiveCollectionDBIden::Id.into_iden(),
            ActiveCollectionDBIden::Name.into_iden(),
        ),
        Dimension::Group => (
            GroupDBIden::Table.into_iden(),
            GroupDBIden::Id.into_iden(),
            GroupDBIden::Name.into_iden(),
        ),
    };
    let dim_name_join =
        |alias: Sq, id_col: Col| Expr::col((alias, dim_id.clone())).equals((Sq::A, id_col));

    let keys = [
        Col::Bucket,
        Col::SourceKind,
        Col::Source,
        Col::TargetKind,
        Col::Target,
        Col::Status,
        Col::ProjectId,
    ];
    let mut agg = Query::select();
    for c in keys {
        agg.expr(Expr::col((Sq::E, c)));
    }
    agg.expr_as(
        Func::count_distinct(Expr::col((Sq::E, Col::LfrId))),
        Col::FnRuns,
    )
    .expr_as(
        Func::count_distinct(Expr::col((Sq::E, Col::SourceLtdvId))),
        Col::Tables,
    )
    .expr_as(coalesce_sum((Sq::E, Col::SourceRowCount)), Col::RowCount)
    .expr_as(coalesce_sum((Sq::E, Col::SourceDataBytes)), Col::DataBytes)
    .from_subquery(edge_inner_union(req, bounds), Sq::E);
    for c in keys {
        agg.group_by_col((Sq::E, c));
    }

    let mut outer = Query::select();
    outer
        .expr(Expr::col((Sq::A, Col::Bucket)))
        .expr(Expr::col((Sq::A, Col::SourceKind)))
        .expr(Expr::col((Sq::A, Col::Source)))
        .expr_as(Expr::col((Sq::SrcC, dim_name.clone())), Col::SourceName)
        .expr(Expr::col((Sq::A, Col::TargetKind)))
        .expr(Expr::col((Sq::A, Col::Target)))
        .expr_as(Expr::col((Sq::TgtC, dim_name)), Col::TargetName)
        .expr(Expr::col((Sq::A, Col::Status)))
        .expr(Expr::col((Sq::A, Col::ProjectId)))
        .expr_as(
            Expr::col((Sq::Proj, ActiveProjectDBIden::Name)),
            Col::ProjectName,
        )
        .expr(Expr::col((Sq::A, Col::FnRuns)))
        .expr(Expr::col((Sq::A, Col::Tables)))
        // Not summable here; the service layer fills it from edge_tables_query.
        .expr_as(bigint_zero(), Col::LogicalTables)
        .expr(Expr::col((Sq::A, Col::RowCount)))
        .expr(Expr::col((Sq::A, Col::DataBytes)))
        .from_subquery(agg, Sq::A)
        .join_as(
            JoinType::LeftJoin,
            dim_table.clone(),
            Sq::SrcC,
            dim_name_join(Sq::SrcC, Col::Source),
        )
        .join_as(
            JoinType::LeftJoin,
            dim_table,
            Sq::TgtC,
            dim_name_join(Sq::TgtC, Col::Target),
        )
        .join_as(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            Sq::Proj,
            Expr::col((Sq::Proj, ActiveProjectDBIden::Id)).equals((Sq::A, Col::ProjectId)),
        )
        .cond_where(visible_projects.narrows(Expr::col((Sq::A, Col::ProjectId))))
        .order_by((Sq::A, Col::Bucket), Order::Desc)
        .order_by_with_nulls((Sq::A, Col::Source), Order::Asc, NullOrdering::Last)
        .order_by_with_nulls((Sq::A, Col::Target), Order::Asc, NullOrdering::Last)
        .order_by((Sq::A, Col::Status), Order::Asc);
    outer
}

pub(crate) fn edge_tables_query(
    req: &MetricsRequest,
    bounds: &WindowBounds,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let mut outer = Query::select();
    outer.distinct();
    for c in [
        Col::Bucket,
        Col::ProjectId,
        Col::SourceKind,
        Col::Source,
        Col::TargetKind,
        Col::Target,
        Col::Status,
        Col::TableId,
    ] {
        outer.expr(Expr::col((Sq::E, c)));
    }
    outer
        .from_subquery(edge_inner_union(req, bounds), Sq::E)
        .cond_where(visible_projects.narrows(Expr::col((Sq::E, Col::ProjectId))));
    outer
}
