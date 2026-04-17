//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Alias, Expr, ExprTrait, JoinType, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::function_requirement::FunctionRequirementDBIden;
use td_objects::dxo::function_run::FunctionRunDBIden;
use td_objects::types::basic::{ExecutionId, FunctionRunId, FunctionRunStatus};

/// Optimized queries for `function_requirements`.
pub trait FunctionRequirementQueries {
    /// Find the function_run_ids in this execution whose upstream dependency
    /// has a Failed or OnHold status.
    ///
    /// Joins `function_requirements` directly to `function_runs` via
    /// `requirement_function_run_id`.
    fn fetch_blocked_function_run_ids(
        self,
        execution_id: &ExecutionId,
    ) -> impl Future<Output = Result<Vec<FunctionRunId>, SqlError>>;
}

pub(crate) fn blocked_run_ids_query(execution_id: &ExecutionId) -> sea_query::SelectStatement {
    let upstream = Alias::new("upstream");

    Query::select()
        .distinct()
        .expr(Expr::col((
            FunctionRequirementDBIden::Table,
            FunctionRequirementDBIden::FunctionRunId,
        )))
        .from(FunctionRequirementDBIden::Table)
        .join_as(
            JoinType::InnerJoin,
            FunctionRunDBIden::Table,
            upstream.clone(),
            Expr::col((
                FunctionRequirementDBIden::Table,
                FunctionRequirementDBIden::RequirementFunctionRunId,
            ))
            .equals((upstream.clone(), FunctionRunDBIden::Id)),
        )
        .and_where(
            Expr::col((
                FunctionRequirementDBIden::Table,
                FunctionRequirementDBIden::ExecutionId,
            ))
            .eq(execution_id.as_sea_value()),
        )
        .and_where(Expr::col((upstream, FunctionRunDBIden::Status)).is_in([
            FunctionRunStatus::Failed.as_sea_value(),
            FunctionRunStatus::OnHold.as_sea_value(),
        ]))
        .take()
}

impl<E> FunctionRequirementQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_blocked_function_run_ids(
        self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<FunctionRunId>, SqlError> {
        let ids: Vec<FunctionRunId> = blocked_run_ids_query(execution_id)
            .build_query_scalar()
            .fetch_all(self)
            .await
            .map_err(|e| SqlError::on_entity("Function Requirement", e))?;

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_blocked_run_ids_query_sql() {
        let eid = ExecutionId::try_from("EP20260101.0000000001").unwrap();
        let sql = blocked_run_ids_query(&eid).to_string(SqliteQueryBuilder);
        assert!(sql.contains("SELECT DISTINCT"), "should be distinct: {sql}");
        assert!(
            sql.contains("INNER JOIN"),
            "should join function_runs: {sql}"
        );
        assert!(
            sql.contains(r#""function_requirements""#),
            "should query from function_requirements: {sql}"
        );
        assert!(
            sql.contains(r#""execution_id""#),
            "should filter by execution_id: {sql}"
        );
        assert!(
            sql.contains("'F'") && sql.contains("'H'"),
            "should filter by Failed and OnHold status: {sql}"
        );
    }
}
