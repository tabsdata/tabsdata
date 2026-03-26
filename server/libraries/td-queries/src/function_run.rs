//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, CommonTableExpression, Expr, ExprTrait, Func, JoinType, Query, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunDBIden};
use td_objects::types::basic::ExecutionId;

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
    fn fetch_to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> impl Future<Output = Result<Vec<FunctionRunDB>, SqlError>>;
}

impl<E> FunctionRunQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> Result<Vec<FunctionRunDB>, SqlError> {
        if execution_ids.is_empty() {
            return Ok(vec![]);
        }

        let exec_values: Vec<_> = execution_ids.iter().map(|t| t.as_sea_value()).collect();

        // CTE1: transactions_with_done_runs
        // Find transactions where ALL function runs have status = 'D' (Done).
        // Scoped to the given execution_ids.
        let cte1_select = Query::select()
            .column(FunctionRunDBIden::TransactionId)
            .from(FunctionRunDBIden::Table)
            .and_where(Expr::col(FunctionRunDBIden::ExecutionId).is_in(exec_values.clone()))
            .group_by_col(FunctionRunDBIden::TransactionId)
            .and_having(
                Expr::expr(Func::sum(
                    Expr::case(Expr::col(FunctionRunDBIden::Status).ne("D"), 1).finally(0),
                ))
                .eq(0),
            )
            .to_owned();

        let cte1 = CommonTableExpression::new()
            .query(cte1_select)
            .table_name(Alias::new("transactions_with_done_runs"))
            .to_owned();

        // CTE2: transactions_with_pending_reqs
        // Find transactions that still have non-Done/non-Committed requirements.
        // Scoped to the given execution_ids.
        let cte2_select = Query::select()
            .distinct()
            .column(Alias::new("transaction_id"))
            .from(Alias::new("function_requirements__with_status"))
            .and_where(Expr::col(Alias::new("status")).is_not_in(["D", "C"]))
            .and_where(Expr::col(Alias::new("execution_id")).is_in(exec_values))
            .to_owned();

        let cte2 = CommonTableExpression::new()
            .query(cte2_select)
            .table_name(Alias::new("transactions_with_pending_reqs"))
            .to_owned();

        let with_clause = WithClause::new().cte(cte1).cte(cte2).to_owned();

        // Main query: select function_runs from done transactions without pending reqs
        let done_cte = Alias::new("transactions_with_done_runs");
        let pending_cte = Alias::new("transactions_with_pending_reqs");

        let select = Query::select()
            .expr(Expr::col((FunctionRunDBIden::Table, Asterisk)))
            .from(FunctionRunDBIden::Table)
            .join_as(
                JoinType::Join,
                done_cte.clone(),
                done_cte.clone(),
                Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TransactionId))
                    .equals((done_cte, Alias::new("transaction_id"))),
            )
            .join_as(
                JoinType::LeftJoin,
                pending_cte.clone(),
                pending_cte.clone(),
                Expr::col((FunctionRunDBIden::Table, FunctionRunDBIden::TransactionId))
                    .equals((pending_cte.clone(), Alias::new("transaction_id"))),
            )
            .and_where(Expr::col((pending_cte, Alias::new("transaction_id"))).is_null())
            .to_owned();

        let result = select
            .with(with_clause)
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(|e| SqlError::on_entity("Function Run", e))?;

        Ok(result)
    }
}
