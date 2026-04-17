//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, CommonTableExpression, Expr, ExprTrait, Func, JoinType, Order, Query,
    WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::collection::CollectionDBIden;
use td_objects::dxo::execution::ExecutionDBIden;
use td_objects::dxo::function::FunctionDBIden;
use td_objects::dxo::function_requirement::FunctionRequirementDBIden;
use td_objects::dxo::function_run::{
    FunctionRunDBIden, FunctionRunDBWithNamesIden, FunctionRunToExecute,
};
use td_objects::dxo::user::UserDBIden;
use td_objects::types::basic::{ExecutionId, FunctionRunId, FunctionRunStatus};

/// Optimized queries for `function_runs` that avoid full-table scans.
pub trait FunctionRunQueries {
    /// Scoped version of the `function_runs__to_commit` view.
    ///
    /// The view scans ALL `function_runs` rows to GROUP BY transaction_id and
    /// check whether every run in each transaction is Done. This method limits
    /// the scan to a specific set of `execution_ids`, making it O(k) instead
    /// of O(n) where k is the number of runs in the given executions.
    ///
    /// Returns the function runs that are ready to be committed (all runs in
    /// their transaction are Done, and no pending requirements exist).
    fn fetch_function_runs_to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> impl Future<Output = Result<Vec<FunctionRunId>, SqlError>>;

    /// Replacement for the `function_runs__to_execute` view query.
    ///
    /// The view uses a CTE that scans ALL `function_requirements` through a
    /// 3-table LEFT JOIN chain (`table_data_versions__with_function`) just to
    /// compute requirement status. This method uses NOT EXISTS with a direct
    /// INNER JOIN from `function_requirements.requirement_function_run_id` to
    /// `function_runs.status`, which short-circuits per run and avoids the
    /// full table scan.
    fn fetch_function_runs_to_execute(
        self,
    ) -> impl Future<Output = Result<Vec<FunctionRunToExecute>, SqlError>>;
}

pub(crate) fn fetch_ids_to_commit_query(execution_ids: &[ExecutionId]) -> sea_query::WithQuery {
    let exec_values: Vec<_> = execution_ids.iter().map(|t| t.as_sea_value()).collect();

    // CTE1: transactions_with_done_runs
    let cte1_select = Query::select()
        .column(FunctionRunDBIden::TransactionId)
        .from(FunctionRunDBIden::Table)
        .and_where(Expr::col(FunctionRunDBIden::ExecutionId).is_in(exec_values.clone()))
        .group_by_col(FunctionRunDBIden::TransactionId)
        .and_having(
            Expr::expr(Func::sum(
                Expr::case(
                    Expr::col(FunctionRunDBIden::Status).ne(FunctionRunStatus::Done.as_sea_value()),
                    1,
                )
                .finally(0),
            ))
            .eq(0),
        )
        .to_owned();

    let cte1 = CommonTableExpression::new()
        .query(cte1_select)
        .table_name(Alias::new("transactions_with_done_runs"))
        .to_owned();

    // CTE2: transactions_with_pending_reqs
    let upstream = Alias::new("upstream");
    let cte2_select = Query::select()
        .distinct()
        .column((
            FunctionRequirementDBIden::Table,
            FunctionRequirementDBIden::TransactionId,
        ))
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
            .is_in(exec_values),
        )
        .and_where(Expr::col((upstream, FunctionRunDBIden::Status)).is_not_in([
            FunctionRunStatus::Done.as_sea_value(),
            FunctionRunStatus::Committed.as_sea_value(),
        ]))
        .to_owned();

    let cte2 = CommonTableExpression::new()
        .query(cte2_select)
        .table_name(Alias::new("transactions_with_pending_reqs"))
        .to_owned();

    let with_clause = WithClause::new().cte(cte1).cte(cte2).to_owned();

    // Main query
    let done_cte = Alias::new("transactions_with_done_runs");
    let pending_cte = Alias::new("transactions_with_pending_reqs");

    Query::select()
        .expr(Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::Id)))
        .from(FunctionRunDBIden::Table)
        .join_as(
            JoinType::Join,
            done_cte.clone(),
            done_cte.clone(),
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TransactionId))
                .equals((done_cte, FunctionRunDBIden::TransactionId)),
        )
        .join_as(
            JoinType::LeftJoin,
            pending_cte.clone(),
            pending_cte.clone(),
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TransactionId))
                .equals((pending_cte.clone(), FunctionRunDBIden::TransactionId)),
        )
        .and_where(Expr::col((pending_cte, FunctionRunDBIden::TransactionId)).is_null())
        .to_owned()
        .with(with_clause)
}

pub(crate) fn fetch_runs_to_execute_query() -> sea_query::SelectStatement {
    let upstream = Alias::new("upstream");

    let pending_req = Query::select()
        .column((
            FunctionRequirementDBIden::Table,
            FunctionRequirementDBIden::Id,
        ))
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
                FunctionRequirementDBIden::FunctionRunId,
            ))
            .equals((FunctionRunDBIden::Table, FunctionRunDBIden::Id)),
        )
        .and_where(Expr::col((upstream, FunctionRunDBIden::Status)).is_not_in([
            FunctionRunStatus::Done.as_sea_value(),
            FunctionRunStatus::Committed.as_sea_value(),
        ]))
        .to_owned();

    Query::select()
        .expr(Expr::col((FunctionRunDBIden::Table, Asterisk)))
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::Name)),
            FunctionRunDBWithNamesIden::Name,
        )
        .expr_as(
            Expr::col((CollectionDBIden::Table, CollectionDBIden::Name)),
            FunctionRunDBWithNamesIden::Collection,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::Name)),
            FunctionRunDBWithNamesIden::Execution,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::FunctionVersionId)),
            FunctionRunDBWithNamesIden::ExecutionFunctionVersionId,
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((UserDBIden::Table, UserDBIden::Name)),
                Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TriggeredById)),
            ])),
            FunctionRunDBWithNamesIden::TriggeredBy,
        )
        .expr(Expr::col((
            FunctionDBIden::Table,
            FunctionDBIden::DataLocation,
        )))
        .expr(Expr::col((
            FunctionDBIden::Table,
            FunctionDBIden::StorageVersion,
        )))
        .expr(Expr::col((FunctionDBIden::Table, FunctionDBIden::BundleId)))
        .expr(Expr::col((
            FunctionDBIden::Table,
            FunctionDBIden::DefinedOn,
        )))
        .from(FunctionRunDBIden::Table)
        .join(
            JoinType::Join,
            FunctionDBIden::Table,
            Expr::col((
                FunctionRunDBIden::Table,
                FunctionRunDBIden::FunctionVersionId,
            ))
            .equals((FunctionDBIden::Table, FunctionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            CollectionDBIden::Table,
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::CollectionId))
                .equals((CollectionDBIden::Table, CollectionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            ExecutionDBIden::Table,
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::ExecutionId))
                .equals((ExecutionDBIden::Table, ExecutionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            UserDBIden::Table,
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TriggeredById))
                .equals((UserDBIden::Table, UserDBIden::Id)),
        )
        .and_where(
            Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::Status)).is_in([
                FunctionRunStatus::Scheduled.as_sea_value(),
                FunctionRunStatus::ReScheduled.as_sea_value(),
            ]),
        )
        .and_where(Expr::exists(pending_req).not())
        .order_by(
            (FunctionRunDBIden::Table, FunctionRunDBIden::TriggeredOn),
            Order::Desc,
        )
        .take()
}

impl<E> FunctionRunQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_function_runs_to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> Result<Vec<FunctionRunId>, SqlError> {
        if execution_ids.is_empty() {
            return Ok(vec![]);
        }

        let result = fetch_ids_to_commit_query(execution_ids)
            .build_query_scalar()
            .fetch_all(self)
            .await
            .map_err(|e| SqlError::on_entity("Function Run", e))?;

        Ok(result)
    }

    async fn fetch_function_runs_to_execute(self) -> Result<Vec<FunctionRunToExecute>, SqlError> {
        let result: Vec<FunctionRunToExecute> = fetch_runs_to_execute_query()
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(|e| SqlError::on_entity("Function Run", e))?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_fetch_to_commit_query_sql() {
        let ids = vec![
            ExecutionId::try_from("EP20260101.0000000001").unwrap(),
            ExecutionId::try_from("EP20260101.0000000002").unwrap(),
        ];
        let sql = fetch_ids_to_commit_query(&ids).to_string(SqliteQueryBuilder);
        assert!(sql.starts_with("WITH"), "should be a WITH query: {sql}");
        assert!(
            sql.contains("transactions_with_done_runs"),
            "should have CTE1: {sql}"
        );
        assert!(
            sql.contains("transactions_with_pending_reqs"),
            "should have CTE2: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "CTE1 should group: {sql}");
        assert!(sql.contains("HAVING"), "CTE1 should have HAVING: {sql}");
        assert!(sql.contains("LEFT JOIN"), "should LEFT JOIN pending: {sql}");
        assert!(
            sql.contains("IS NULL"),
            "should check pending IS NULL: {sql}"
        );
    }

    #[test]
    fn test_fetch_runs_to_execute_query_sql() {
        let sql = fetch_runs_to_execute_query().to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "function_runs""#),
            "should query function_runs: {sql}"
        );
        assert!(
            sql.contains(r#"JOIN "functions""#),
            "should join functions: {sql}"
        );
        assert!(
            sql.contains("LEFT JOIN") && sql.contains("collection"),
            "should left join collections: {sql}"
        );
        assert!(
            sql.contains(r#"LEFT JOIN "executions""#),
            "should left join executions: {sql}"
        );
        assert!(
            sql.contains(r#"LEFT JOIN "users""#),
            "should left join users: {sql}"
        );
        assert!(
            sql.contains("NOT EXISTS"),
            "should have NOT EXISTS for pending reqs: {sql}"
        );
        assert!(
            sql.contains("COALESCE"),
            "should have COALESCE for triggered_by: {sql}"
        );
        assert!(sql.contains("ORDER BY"), "should order results: {sql}");
    }
}
