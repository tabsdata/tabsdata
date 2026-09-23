//
// Copyright 2026 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::daos::function_runs::chain::{CurrentPtdv, Visibility, materialization_of_ltdv};
use sea_query::{Expr, ExprTrait, JoinType, Query, SelectStatement};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::{Dao, FromDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::execution::{Execution, ExecutionDBIden};
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBIden;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{FunctionVersionId, PhysicalTableDataVersionId};

type DaoOf = <Execution as FromDao>::Dao;
type Iden = <DaoOf as Dao>::Iden;

/// `executions`-table list queries whose filter walks other tables; the
/// pagination is applied in SQL.
pub struct Executions<E> {
    pub(crate) executor: E,
}

impl<E> Executions<E>
where
    E: QueryExecutor,
{
    /// Executions that consumed `tdv_id` as an input.
    pub async fn list_by_table_data_version(
        self,
        tdv_id: &Option<PhysicalTableDataVersionId>,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        self.list(params.apply_to(list_by_tdv_query(tdv_id))).await
    }

    /// Executions that ran `fv_id`, as a head version or as a patch.
    pub async fn list_by_function_version(
        self,
        fv_id: &FunctionVersionId,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        self.list(params.apply_to(list_by_fv_query(fv_id))).await
    }

    async fn list(self, mut query: SelectStatement) -> Result<Vec<Execution>, SqlError> {
        let daos: Vec<DaoOf> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<DaoOf>)?;
        daos.into_iter()
            .map(|dao| Execution::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}

fn executions_where(filter: sea_query::SimpleExpr) -> SelectStatement {
    Query::select()
        .columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .from(Iden::Table)
        .and_where(filter)
        .take()
}

fn execution_id_matches<T: sea_query::IntoIden, C: sea_query::IntoIden>(
    table: T,
    col: C,
) -> sea_query::SimpleExpr {
    Expr::col((table, col)).equals((Iden::Table, ExecutionDBIden::Id))
}

/// An EXISTS over `inputs → LTDV → open materialization` keeps the
/// executions columns unaliased for the list params' ORDER BY.
pub(crate) fn list_by_tdv_query(tdv_id: &Option<PhysicalTableDataVersionId>) -> SelectStatement {
    let consumed = Query::select()
        .expr(Expr::value(1))
        .from(LogicalFunctionRunInputDBIden::Table)
        .join(
            JoinType::InnerJoin,
            LogicalTableDataVersionDBIden::Table,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            )),
        )
        .join_as(
            JoinType::InnerJoin,
            PhysicalTableDataVersionDBIden::Table,
            CurrentPtdv,
            materialization_of_ltdv(CurrentPtdv, Visibility::Open),
        )
        .and_where(execution_id_matches(
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::ExecutionId,
        ))
        .and_where(
            Expr::col((CurrentPtdv, PhysicalTableDataVersionDBIden::Id)).eq(tdv_id.as_sea_value()),
        )
        .take();
    executions_where(Expr::exists(consumed))
}

/// LFRs only carry head fvids, so a patch fvid matches through the
/// attempts' `function_version_patch_id`.
pub(crate) fn list_by_fv_query(fv_id: &FunctionVersionId) -> SelectStatement {
    let as_head = Query::select()
        .expr(Expr::value(1))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(execution_id_matches(
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::ExecutionId,
        ))
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::FunctionVersionId,
            ))
            .eq(fv_id.as_sea_value()),
        )
        .take();
    let as_patch = Query::select()
        .expr(Expr::value(1))
        .from(PhysicalFunctionRunDBIden::Table)
        .and_where(execution_id_matches(
            PhysicalFunctionRunDBIden::Table,
            PhysicalFunctionRunDBIden::ExecutionId,
        ))
        .and_where(
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionPatchId,
            ))
            .eq(fv_id.as_sea_value()),
        )
        .take();
    executions_where(Expr::exists(as_head).or(Expr::exists(as_patch)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_list_by_tdv_query() {
        let tdv_id = Some(PhysicalTableDataVersionId::try_from("pdv20260101-0000000001").unwrap());
        let sql = list_by_tdv_query(&tdv_id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "executions__with_names"."id", "executions__with_names"."name", "executions__with_names"."collection_id", "executions__with_names"."function_id", "executions__with_names"."function_version_id", "executions__with_names"."triggered_on", "executions__with_names"."triggered_by_id", "executions__with_names"."trigger_type", "executions__with_names"."external_uid", "executions__with_names"."recursion_options", "executions__with_names"."project_id", "executions__with_names"."status", "executions__with_names"."started_on", "executions__with_names"."ended_on", "executions__with_names"."collection", "executions__with_names"."collection_deleted", "executions__with_names"."function", "executions__with_names"."triggered_by", "executions__with_names"."project", "executions__with_names"."function_run_status_count" FROM "executions__with_names" WHERE EXISTS(SELECT 1 FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "physical_table_data_versions" AS "current_ptdv" ON "current_ptdv"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND "current_ptdv"."valid_to" = ('infinity'::timestamptz) WHERE "logical_function_run_inputs"."execution_id" = "executions__with_names"."id" AND "current_ptdv"."id" = 202601010000000001)"#
        );
    }

    #[test]
    fn test_list_by_tdv_query_none() {
        let sql = list_by_tdv_query(&None).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "executions__with_names"."id", "executions__with_names"."name", "executions__with_names"."collection_id", "executions__with_names"."function_id", "executions__with_names"."function_version_id", "executions__with_names"."triggered_on", "executions__with_names"."triggered_by_id", "executions__with_names"."trigger_type", "executions__with_names"."external_uid", "executions__with_names"."recursion_options", "executions__with_names"."project_id", "executions__with_names"."status", "executions__with_names"."started_on", "executions__with_names"."ended_on", "executions__with_names"."collection", "executions__with_names"."collection_deleted", "executions__with_names"."function", "executions__with_names"."triggered_by", "executions__with_names"."project", "executions__with_names"."function_run_status_count" FROM "executions__with_names" WHERE EXISTS(SELECT 1 FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "physical_table_data_versions" AS "current_ptdv" ON "current_ptdv"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND "current_ptdv"."valid_to" = ('infinity'::timestamptz) WHERE "logical_function_run_inputs"."execution_id" = "executions__with_names"."id" AND "current_ptdv"."id" = NULL)"#
        );
    }

    #[test]
    fn test_list_by_fv_query() {
        let fv_id = FunctionVersionId::try_from("fv20260101-0000000001").unwrap();
        let sql = list_by_fv_query(&fv_id).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "executions__with_names"."id", "executions__with_names"."name", "executions__with_names"."collection_id", "executions__with_names"."function_id", "executions__with_names"."function_version_id", "executions__with_names"."triggered_on", "executions__with_names"."triggered_by_id", "executions__with_names"."trigger_type", "executions__with_names"."external_uid", "executions__with_names"."recursion_options", "executions__with_names"."project_id", "executions__with_names"."status", "executions__with_names"."started_on", "executions__with_names"."ended_on", "executions__with_names"."collection", "executions__with_names"."collection_deleted", "executions__with_names"."function", "executions__with_names"."triggered_by", "executions__with_names"."project", "executions__with_names"."function_run_status_count" FROM "executions__with_names" WHERE EXISTS(SELECT 1 FROM "logical_function_runs" WHERE "logical_function_runs"."execution_id" = "executions__with_names"."id" AND "logical_function_runs"."function_version_id" = 202601010000000001) OR EXISTS(SELECT 1 FROM "physical_function_runs" WHERE "physical_function_runs"."execution_id" = "executions__with_names"."id" AND "physical_function_runs"."function_version_patch_id" = 202601010000000001)"#
        );
    }
}
