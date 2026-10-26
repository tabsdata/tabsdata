//
// Copyright 2026 Tabsdata Inc.
//

use crate::daos::metrics::read::apply_dim_filter;
use crate::primitives::scd2::{label_of, last_version_of};
use sea_query::{Expr, ExprTrait, Func, Iden, JoinType, Query, SelectStatement};
use ta_objects::SeaValue;
use td_objects::dxo::collection::{CollectionDB, CollectionDBIden};
use td_objects::dxo::group::{GroupDB, GroupDBIden};
use td_objects::dxo::logical::LogicalFunctionRunDBIden;
use td_objects::dxo::metrics::InflightRequest;
use td_objects::dxo::project::{ProjectDB, ProjectDBIden};
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::visible_projects::VisibleProjects;

#[derive(Iden, Clone, Copy)]
enum Col {
    CollectionName,
    GroupName,
    ProjectName,
    Count,
}

/// A deleted collection, group or project is labelled by its id, as in
/// `/metrics`.
pub(crate) fn inflight_query(
    req: &InflightRequest,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let lfr = |c: LogicalFunctionRunDBIden| (LogicalFunctionRunDBIden::Table, c);
    let mut stmt = Query::select();
    stmt.column(lfr(LogicalFunctionRunDBIden::CollectionId))
        .expr_as(
            label_of::<CollectionDB>(
                (CollectionDBIden::Table, CollectionDBIden::Name),
                lfr(LogicalFunctionRunDBIden::CollectionId),
            ),
            Col::CollectionName,
        )
        .column(lfr(LogicalFunctionRunDBIden::GroupId))
        .expr_as(
            label_of::<GroupDB>(
                (GroupDBIden::Table, GroupDBIden::Name),
                lfr(LogicalFunctionRunDBIden::GroupId),
            ),
            Col::GroupName,
        )
        .column(lfr(LogicalFunctionRunDBIden::ProjectId))
        .expr_as(
            label_of::<ProjectDB>(
                (ProjectDBIden::Table, ProjectDBIden::Name),
                lfr(LogicalFunctionRunDBIden::ProjectId),
            ),
            Col::ProjectName,
        )
        .column(lfr(LogicalFunctionRunDBIden::Status))
        .expr_as(Func::count(Expr::val(1)), Col::Count)
        .from(LogicalFunctionRunDBIden::Table)
        .join(
            JoinType::LeftJoin,
            CollectionDBIden::Table,
            last_version_of::<CollectionDB>(lfr(LogicalFunctionRunDBIden::CollectionId)),
        )
        .join(
            JoinType::LeftJoin,
            GroupDBIden::Table,
            last_version_of::<GroupDB>(lfr(LogicalFunctionRunDBIden::GroupId)),
        )
        .join(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            last_version_of::<ProjectDB>(lfr(LogicalFunctionRunDBIden::ProjectId)),
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
        .group_by_col((CollectionDBIden::Table, CollectionDBIden::Name))
        .group_by_col((CollectionDBIden::Table, CollectionDBIden::ValidTo))
        .group_by_col(lfr(LogicalFunctionRunDBIden::GroupId))
        .group_by_col((GroupDBIden::Table, GroupDBIden::Name))
        .group_by_col((GroupDBIden::Table, GroupDBIden::ValidTo))
        .group_by_col(lfr(LogicalFunctionRunDBIden::ProjectId))
        .group_by_col((ProjectDBIden::Table, ProjectDBIden::Name))
        .group_by_col((ProjectDBIden::Table, ProjectDBIden::ValidTo))
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
