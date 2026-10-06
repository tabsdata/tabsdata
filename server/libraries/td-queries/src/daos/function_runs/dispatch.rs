//
// Copyright 2026 Tabsdata Inc.
//

//! Dispatch-time enrichment: the denormalized row the worker-yaml builder
//! reads for each just-minted attempt.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use sea_query::{Alias, Asterisk, Expr, ExprTrait, Func, JoinType, Order, Query, SelectStatement};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::bundle::BundleDBIden;
use td_objects::dxo::collection::{ActiveCollectionDBIden, ResolvedCollectionDBIden};
use td_objects::dxo::connection::ConnectionDBIden;
use td_objects::dxo::execution::ExecutionDBIden;
use td_objects::dxo::function::{FunctionDBIden, FunctionPatchDBIden};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunDBIden, PhysicalFunctionRunDBWithNamesIden,
    PhysicalFunctionRunToExecute,
};
use td_objects::dxo::project::{ActiveProjectDBIden, ProjectDBIden};
use td_objects::dxo::user::UserDBIden;
use td_objects::types::basic::PhysicalFunctionRunId;

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// The [`PhysicalFunctionRunToExecute`] rows for `fr_ids`, no readiness
    /// predicate.
    pub async fn for_dispatch(
        self,
        fr_ids: &[PhysicalFunctionRunId],
    ) -> Result<Vec<PhysicalFunctionRunToExecute>, SqlError> {
        if fr_ids.is_empty() {
            return Ok(Vec::new());
        }
        fetch_runs_for_dispatch_query(fr_ids)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)
    }
}

fn effective_bundle_id() -> Expr {
    Expr::expr(Func::coalesce([
        Expr::col((FunctionPatchDBIden::Table, FunctionPatchDBIden::BundleId)),
        Expr::col((FunctionDBIden::Table, FunctionDBIden::BundleId)),
    ]))
}

/// `function_version_id` always holds the head fvid, so head-side attributes
/// (name, storage version, stage prefix, decorator) read straight off
/// `functions`; `bundle_id` / `defined_on` are patch-overridable and COALESCE
/// patch-first. The collection resolves through `collections_resolved` so a
/// run can outlive a soft-deleted collection, and the bundle prefix falls back
/// to the collection's effective prefix when the bundle row is absent.
fn function_run_to_execute_select() -> SelectStatement {
    Query::select()
        .expr(Expr::col((PhysicalFunctionRunDBIden::Table, Asterisk)))
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::Name)),
            PhysicalFunctionRunDBWithNamesIden::Name,
        )
        .expr_as(
            Expr::col((
                ResolvedCollectionDBIden::Table,
                ActiveCollectionDBIden::Name,
            )),
            PhysicalFunctionRunDBWithNamesIden::Collection,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::Name)),
            PhysicalFunctionRunDBWithNamesIden::Execution,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::FunctionVersionId)),
            PhysicalFunctionRunDBWithNamesIden::ExecutionFunctionVersionId,
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((UserDBIden::Table, UserDBIden::Name)),
                Expr::col((
                    PhysicalFunctionRunDBIden::Table,
                    PhysicalFunctionRunDBIden::TriggeredById,
                )),
            ])),
            PhysicalFunctionRunDBWithNamesIden::TriggeredBy,
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::StorageVersion)),
            FunctionDBIden::StorageVersion,
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::StageId)),
            FunctionDBIden::StageId,
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::StageBasePath)),
            FunctionDBIden::StageBasePath,
        )
        .expr_as(
            Expr::col((
                ResolvedCollectionDBIden::Table,
                ActiveCollectionDBIden::ConnectionId,
            )),
            Alias::new("connection_id"),
        )
        .expr_as(
            Expr::col((ConnectionDBIden::Table, ConnectionDBIden::Kind)),
            Alias::new("connection"),
        )
        .expr_as(
            Expr::col((ProjectDBIden::Table, ActiveProjectDBIden::Name)),
            Alias::new("project"),
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((BundleDBIden::Table, BundleDBIden::BasePath)),
                Expr::col((
                    ResolvedCollectionDBIden::Table,
                    ActiveCollectionDBIden::EffectiveBasePath,
                )),
            ])),
            Alias::new("bundle_base_path"),
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::Decorator)),
            FunctionDBIden::Decorator,
        )
        .expr_as(effective_bundle_id(), FunctionDBIden::BundleId)
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((FunctionPatchDBIden::Table, FunctionPatchDBIden::ValidFrom)),
                Expr::col((FunctionDBIden::Table, FunctionDBIden::ValidFrom)),
            ])),
            Alias::new("defined_on"),
        )
        .from(PhysicalFunctionRunDBIden::Table)
        .join(
            JoinType::LeftJoin,
            FunctionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionId,
            ))
            .equals((FunctionDBIden::Table, FunctionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            FunctionPatchDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionPatchId,
            ))
            .equals((FunctionPatchDBIden::Table, FunctionPatchDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            ResolvedCollectionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::CollectionId,
            ))
            .equals((ResolvedCollectionDBIden::Table, ActiveCollectionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            ConnectionDBIden::Table,
            Expr::col((
                ResolvedCollectionDBIden::Table,
                ActiveCollectionDBIden::ConnectionId,
            ))
            .equals((ConnectionDBIden::Table, ConnectionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ProjectId,
            ))
            .equals((ProjectDBIden::Table, ActiveProjectDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            BundleDBIden::Table,
            Expr::col((BundleDBIden::Table, BundleDBIden::Id)).eq(effective_bundle_id()),
        )
        .join(
            JoinType::LeftJoin,
            ExecutionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ExecutionId,
            ))
            .equals((ExecutionDBIden::Table, ExecutionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            UserDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::TriggeredById,
            ))
            .equals((UserDBIden::Table, UserDBIden::Id)),
        )
        .order_by(
            (
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ValidFrom,
            ),
            Order::Desc,
        )
        .take()
}

pub(crate) fn fetch_runs_for_dispatch_query(fr_ids: &[PhysicalFunctionRunId]) -> SelectStatement {
    let mut stmt = function_run_to_execute_select();
    stmt.and_where(
        Expr::col((
            PhysicalFunctionRunDBIden::Table,
            PhysicalFunctionRunDBIden::Id,
        ))
        .is_in(fr_ids.iter().map(SeaValue::as_sea_expr)),
    );
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_fetch_runs_for_dispatch_query() {
        let ids = vec![PhysicalFunctionRunId::try_from("pfr20260101-000000001").unwrap()];
        let sql = fetch_runs_for_dispatch_query(&ids).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_function_runs".*, "functions"."name" AS "name", "collections_resolved"."name" AS "collection", "executions"."name" AS "execution", "executions"."function_version_id" AS "execution_function_version_id", COALESCE("users"."name", "physical_function_runs"."triggered_by_id") AS "triggered_by", "functions"."storage_version" AS "storage_version", "functions"."stage_id" AS "stage_id", "functions"."stage_base_path" AS "stage_base_path", "collections_resolved"."connection_id" AS "connection_id", "connections"."kind" AS "connection", "projects_resolved"."name" AS "project", COALESCE("bundles"."base_path", "collections_resolved"."effective_base_path") AS "bundle_base_path", "functions"."decorator" AS "decorator", COALESCE("function_patches"."bundle_id", "functions"."bundle_id") AS "bundle_id", COALESCE("function_patches"."valid_from", "functions"."valid_from") AS "defined_on" FROM "physical_function_runs" LEFT JOIN "functions" ON "physical_function_runs"."function_version_id" = "functions"."id" LEFT JOIN "function_patches" ON "physical_function_runs"."function_version_patch_id" = "function_patches"."id" LEFT JOIN "collections_resolved" ON "physical_function_runs"."collection_id" = "collections_resolved"."id" LEFT JOIN "connections" ON "collections_resolved"."connection_id" = "connections"."id" LEFT JOIN "projects_resolved" ON "physical_function_runs"."project_id" = "projects_resolved"."id" LEFT JOIN "bundles" ON "bundles"."id" = COALESCE("function_patches"."bundle_id", "functions"."bundle_id") LEFT JOIN "executions" ON "physical_function_runs"."execution_id" = "executions"."id" LEFT JOIN "users" ON "physical_function_runs"."triggered_by_id" = "users"."id" WHERE "physical_function_runs"."id" IN (202601010000000001) ORDER BY "physical_function_runs"."valid_from" DESC"#
        );
    }
}
