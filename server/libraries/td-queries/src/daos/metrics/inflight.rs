//
// Copyright 2026 Tabsdata Inc.
//

use crate::daos::metrics::read::apply_dim_filter;
use sea_query::{Expr, ExprTrait, Func, Iden, JoinType, Query, SelectStatement};
use ta_objects::SeaValue;
use td_objects::dxo::collection::ResolvedCollectionDBIden;
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::LogicalFunctionRunDBIden;
use td_objects::dxo::metrics::InflightRequest;
use td_objects::dxo::project::ActiveProjectDBIden;
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::visible_projects::VisibleProjects;

#[derive(Iden, Clone, Copy)]
enum Col {
    CollectionName,
    GroupName,
    ProjectName,
    Count,
}

/// A soft-deleted collection keeps its pre-deletion name, as in `/metrics`.
pub(crate) fn inflight_query(
    req: &InflightRequest,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let lfr = |c: LogicalFunctionRunDBIden| (LogicalFunctionRunDBIden::Table, c);
    let mut stmt = Query::select();
    stmt.column(lfr(LogicalFunctionRunDBIden::CollectionId))
        .expr_as(
            Expr::col((
                ResolvedCollectionDBIden::Table,
                ResolvedCollectionDBIden::Name,
            )),
            Col::CollectionName,
        )
        .column(lfr(LogicalFunctionRunDBIden::GroupId))
        .expr_as(
            Expr::col((GroupDBIden::Table, GroupDBIden::Name)),
            Col::GroupName,
        )
        .column(lfr(LogicalFunctionRunDBIden::ProjectId))
        .expr_as(
            Expr::col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Name)),
            Col::ProjectName,
        )
        .column(lfr(LogicalFunctionRunDBIden::Status))
        .expr_as(Func::count(Expr::val(1)), Col::Count)
        .from(LogicalFunctionRunDBIden::Table)
        .join(
            JoinType::InnerJoin,
            ResolvedCollectionDBIden::Table,
            Expr::col(lfr(LogicalFunctionRunDBIden::CollectionId)).equals((
                ResolvedCollectionDBIden::Table,
                ResolvedCollectionDBIden::Id,
            )),
        )
        .join(
            JoinType::LeftJoin,
            GroupDBIden::Table,
            Expr::col(lfr(LogicalFunctionRunDBIden::GroupId))
                .equals((GroupDBIden::Table, GroupDBIden::Id)),
        )
        .join(
            JoinType::InnerJoin,
            ActiveProjectDBIden::Table,
            Expr::col(lfr(LogicalFunctionRunDBIden::ProjectId))
                .equals((ActiveProjectDBIden::Table, ActiveProjectDBIden::Id)),
        )
        .and_where(
            Expr::col(lfr(LogicalFunctionRunDBIden::Status)).is_in(
                LogicalFunctionRunStatus::IN_FLIGHT
                    .iter()
                    .map(SeaValue::as_sea_expr),
            ),
        )
        .cond_where(visible_projects.narrows(Expr::col(lfr(LogicalFunctionRunDBIden::ProjectId))))
        .group_by_col(lfr(LogicalFunctionRunDBIden::CollectionId))
        .group_by_col((
            ResolvedCollectionDBIden::Table,
            ResolvedCollectionDBIden::Name,
        ))
        .group_by_col(lfr(LogicalFunctionRunDBIden::GroupId))
        .group_by_col((GroupDBIden::Table, GroupDBIden::Name))
        .group_by_col(lfr(LogicalFunctionRunDBIden::ProjectId))
        .group_by_col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Name))
        .group_by_col(lfr(LogicalFunctionRunDBIden::Status));

    apply_dim_filter(
        &mut stmt,
        &req.collections,
        Expr::col(lfr(LogicalFunctionRunDBIden::CollectionId)),
    );
    apply_dim_filter(
        &mut stmt,
        &req.groups,
        Expr::col(lfr(LogicalFunctionRunDBIden::GroupId)),
    );
    if !req.statuses.is_empty() {
        stmt.and_where(
            Expr::col(lfr(LogicalFunctionRunDBIden::Status))
                .is_in(req.statuses.iter().map(SeaValue::as_sea_expr)),
        );
    }
    stmt
}
