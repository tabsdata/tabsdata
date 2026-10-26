//
// Copyright 2026 Tabsdata Inc.
//

//! Deleted projects whose cleanup is still owed: runs left unfinished, or
//! triggers waiting for their stop.

use crate::QueryExecutor;
use crate::daos::function_runs::deleted_functions::UNFINISHED;
use crate::primitives::scd2::open_interval_at;
use sea_query::{Alias, Expr, ExprTrait, Query, SelectStatement};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::LogicalFunctionRunDBIden;
use td_objects::dxo::project::{ProjectDB, ProjectDBIden};
use td_objects::dxo::worker::WorkerDBIden;
use td_objects::types::basic::{ProjectId, WorkerStatus, WorkerType};

pub struct Projects<E> {
    pub(crate) executor: E,
}

impl<E> Projects<E>
where
    E: QueryExecutor,
{
    /// Every deleted project with an unfinished run or a trigger waiting for
    /// its stop. Reads across projects: on a project-scoped connection it sees
    /// at most that project.
    pub async fn draining(self) -> Result<Vec<ProjectId>, SqlError> {
        draining_query()
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<ProjectDB>)
    }
}

pub(crate) fn draining_query() -> SelectStatement {
    let project = (ProjectDBIden::Table, ProjectDBIden::ProjectId);
    let unfinished_run = Query::select()
        .expr(Expr::val(1))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ProjectId,
            ))
            .equals(project),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Status,
            ))
            .is_in(UNFINISHED.iter().map(SeaValue::as_sea_expr)),
        )
        .take();
    let stopping_trigger = Query::select()
        .expr(Expr::val(1))
        .from(WorkerDBIden::Table)
        .and_where(Expr::col((WorkerDBIden::Table, WorkerDBIden::WorkerEnabled)).eq(true))
        .and_where(
            Expr::col((WorkerDBIden::Table, WorkerDBIden::WorkerType)).is_in(
                [WorkerType::CronTrigger, WorkerType::StageTrigger]
                    .iter()
                    .map(SeaValue::as_sea_expr),
            ),
        )
        .and_where(
            Expr::col((WorkerDBIden::Table, WorkerDBIden::Status))
                .eq(WorkerStatus::StopScheduled.as_sea_expr()),
        )
        .and_where(Expr::col((WorkerDBIden::Table, WorkerDBIden::ProjectId)).equals(project))
        .take();
    let live = Alias::new("live");
    let open_version = Query::select()
        .expr(Expr::val(1))
        .from_as(ProjectDBIden::Table, live.clone())
        .and_where(Expr::col((live.clone(), ProjectDBIden::ProjectId)).equals(project))
        .and_where(open_interval_at((live, ProjectDBIden::ValidTo)))
        .take();
    Query::select()
        .distinct()
        .column(project)
        .from(ProjectDBIden::Table)
        .and_where(Expr::exists(open_version).not())
        .and_where(Expr::exists(unfinished_run).or(Expr::exists(stopping_trigger)))
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_draining_query() {
        let sql = draining_query().to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "projects"."project_id" FROM "projects" WHERE (NOT EXISTS(SELECT 1 FROM "projects" AS "live" WHERE "live"."project_id" = "projects"."project_id" AND "live"."valid_to" = ('infinity'::timestamptz))) AND (EXISTS(SELECT 1 FROM "logical_function_runs" WHERE "logical_function_runs"."project_id" = "projects"."project_id" AND "logical_function_runs"."status" IN ('S', 'RR', 'RS', 'R', 'D', 'E', 'F', 'H')) OR EXISTS(SELECT 1 FROM "workers" WHERE "workers"."worker_enabled" = TRUE AND "workers"."worker_type" IN ('CT', 'ST') AND "workers"."status" = 'SPS' AND "workers"."project_id" = "projects"."project_id"))"#
        );
    }
}
