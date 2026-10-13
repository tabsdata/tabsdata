//
// Copyright 2026 Tabsdata Inc.
//

//! What the scheduler settles on every tick: transactions left holding Done
//! runs. A transaction publishes all its tables or none, so Done runs beside a
//! Canceled one are discarded, and an all-Done transaction is committed.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use sea_query::{Asterisk, Expr, ExprTrait, Query, SelectStatement};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunDBIden};
use td_objects::types::basic::{ExecutionId, LogicalFunctionRunStatus};

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// Done runs whose transaction holds a Canceled run.
    pub async fn done_beside_canceled(self) -> Result<Vec<LogicalFunctionRunDB>, SqlError> {
        done_beside_canceled_query()
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// The executions holding a transaction whose runs are all Done: where
    /// [`Self::to_commit`] looks. Empty in steady state, as the Done callback
    /// commits what it completes.
    pub async fn awaiting_commit(self) -> Result<Vec<ExecutionId>, SqlError> {
        awaiting_commit_query()
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }
}

#[derive(sea_query::Iden, Clone, Copy)]
struct Sibling;

fn done() -> Expr {
    Expr::col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::Status,
    ))
    .eq(LogicalFunctionRunStatus::Done.as_sea_expr())
}

pub(crate) fn done_beside_canceled_query() -> SelectStatement {
    let canceled_sibling = Query::select()
        .expr(Expr::val(1))
        .from_as(LogicalFunctionRunDBIden::Table, Sibling)
        .and_where(
            Expr::col((Sibling, LogicalFunctionRunDBIden::TransactionId)).equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .and_where(
            Expr::col((Sibling, LogicalFunctionRunDBIden::Status))
                .eq(LogicalFunctionRunStatus::Canceled.as_sea_expr()),
        )
        .take();
    Query::select()
        .expr(Expr::col((LogicalFunctionRunDBIden::Table, Asterisk)))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(done())
        .and_where(Expr::exists(canceled_sibling))
        .take()
}

pub(crate) fn awaiting_commit_query() -> SelectStatement {
    let unfinished_sibling = Query::select()
        .expr(Expr::val(1))
        .from_as(LogicalFunctionRunDBIden::Table, Sibling)
        .and_where(
            Expr::col((Sibling, LogicalFunctionRunDBIden::TransactionId)).equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .and_where(
            Expr::col((Sibling, LogicalFunctionRunDBIden::Status))
                .ne(LogicalFunctionRunStatus::Done.as_sea_expr()),
        )
        .take();
    Query::select()
        .distinct()
        .column((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::ExecutionId,
        ))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(done())
        .and_where(Expr::exists(unfinished_sibling).not())
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_done_beside_canceled_query() {
        let sql = done_beside_canceled_query().to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "logical_function_runs".* FROM "logical_function_runs" WHERE "logical_function_runs"."status" = 'D' AND EXISTS(SELECT 1 FROM "logical_function_runs" AS "sibling" WHERE "sibling"."transaction_id" = "logical_function_runs"."transaction_id" AND "sibling"."status" = 'X')"#
        );
    }

    #[test]
    fn test_awaiting_commit_query() {
        let sql = awaiting_commit_query().to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "logical_function_runs"."execution_id" FROM "logical_function_runs" WHERE "logical_function_runs"."status" = 'D' AND (NOT EXISTS(SELECT 1 FROM "logical_function_runs" AS "sibling" WHERE "sibling"."transaction_id" = "logical_function_runs"."transaction_id" AND "sibling"."status" <> 'D'))"#
        );
    }
}
