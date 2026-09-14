//
// Copyright 2026 Tabsdata Inc.
//

use crate::daos::metrics::Col;
use crate::daos::metrics::filters::{apply_dim_filter, apply_status_filter};
use sea_query::{Expr, ExprTrait, Func, JoinType, Query, SelectStatement};
use ta_objects::SeaValue;
use td_objects::dxo::collection::ActiveCollectionDBIden;
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::LogicalFunctionRunDBIden;
use td_objects::dxo::metrics::InflightRequest;
use td_objects::dxo::project::ActiveProjectDBIden;
use td_objects::types::basic::LogicalFunctionRunStatus;
use td_objects::types::visible_projects::VisibleProjects;

/// The bare LFR table with name joins; the `*__with_names` view drags in
/// joins this endpoint doesn't need. The inner joins on the `*_active` views
/// drop runs of soft-deleted collections and projects.
pub(crate) fn inflight_query(
    req: &InflightRequest,
    visible_projects: &VisibleProjects,
) -> SelectStatement {
    let lfr = |c: LogicalFunctionRunDBIden| (LogicalFunctionRunDBIden::Table, c);
    let mut stmt = Query::select();
    stmt.column(lfr(LogicalFunctionRunDBIden::CollectionId))
        .expr_as(
            Expr::col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Name)),
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
            ActiveCollectionDBIden::Table,
            Expr::col(lfr(LogicalFunctionRunDBIden::CollectionId))
                .equals((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Id)),
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
                    .map(SeaValue::as_sea_value),
            ),
        )
        .cond_where(visible_projects.narrows(Expr::col(lfr(LogicalFunctionRunDBIden::ProjectId))))
        .group_by_col(lfr(LogicalFunctionRunDBIden::CollectionId))
        .group_by_col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Name))
        .group_by_col(lfr(LogicalFunctionRunDBIden::GroupId))
        .group_by_col((GroupDBIden::Table, GroupDBIden::Name))
        .group_by_col(lfr(LogicalFunctionRunDBIden::ProjectId))
        .group_by_col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Name))
        .group_by_col(lfr(LogicalFunctionRunDBIden::Status));

    apply_dim_filter(
        &mut stmt,
        &req.collections,
        lfr(LogicalFunctionRunDBIden::CollectionId),
    );
    apply_dim_filter(
        &mut stmt,
        &req.groups,
        lfr(LogicalFunctionRunDBIden::GroupId),
    );
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        lfr(LogicalFunctionRunDBIden::Status),
    );
    stmt
}
