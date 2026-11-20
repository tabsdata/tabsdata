//
// Copyright 2026 Tabsdata Inc.
//

//! The commit dependencies between transactions: a transaction commits only
//! once every other transaction it reads from has committed, and not while it
//! has a Failed or OnHold run. Each query is one step upstream, between
//! uncommitted transactions only; the caller walks the steps.

use crate::Serves;
use crate::daos::function_runs::LogicalRuns;
use crate::daos::function_runs::chain::ProducerLfr;
use crate::daos::function_runs::gates::{GateProducer, Gated, gates};
use crate::primitives::chunked::fetch_chunked;
use sea_query::{Expr, ExprTrait, Iden, JoinType, Query, SelectStatement, SimpleExpr};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden,
};
use td_objects::types::basic::{LogicalFunctionRunId, LogicalFunctionRunStatus, TransactionId};

impl<E> LogicalRuns<E>
where
    E: Serves<LogicalFunctionRunDB>,
{
    /// `(run, awaited)` for every uncommitted transaction a subscriber among
    /// `runs` waits for.
    pub async fn awaited_transactions(
        self,
        runs: &[LogicalFunctionRunId],
    ) -> Result<Vec<(LogicalFunctionRunId, TransactionId)>, SqlError> {
        fetch_chunked::<LogicalFunctionRunDB, _, _, _>(
            self.executor,
            runs,
            awaited_transactions_query,
        )
        .await
    }

    /// `(transaction, read)` for every other transaction whose uncommitted
    /// runs produced what a run of `transactions` reads.
    pub async fn transactions_read_by(
        self,
        transactions: &[TransactionId],
    ) -> Result<Vec<(TransactionId, TransactionId)>, SqlError> {
        fetch_chunked::<LogicalFunctionRunDB, _, _, _>(
            self.executor,
            transactions,
            transactions_read_by_query,
        )
        .await
    }

    /// `(transaction, run)` for every Failed or OnHold run of `transactions`.
    pub async fn stuck_runs(
        self,
        transactions: &[TransactionId],
    ) -> Result<Vec<(TransactionId, LogicalFunctionRunId)>, SqlError> {
        fetch_chunked::<LogicalFunctionRunDB, _, _, _>(
            self.executor,
            transactions,
            stuck_runs_query,
        )
        .await
    }

    /// Every Scheduled or ReScheduled subscriber that waits for another,
    /// uncommitted transaction.
    pub async fn waiting_subscribers(self) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        waiting_subscribers_query()
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }
}

fn uncommitted<T: Iden + 'static>(run: T) -> SimpleExpr {
    Expr::col((run, LogicalFunctionRunDBIden::Status))
        .ne(LogicalFunctionRunStatus::Committed.as_sea_expr())
}

pub(crate) fn awaited_transactions_query(runs: &[LogicalFunctionRunId]) -> SelectStatement {
    let mut stmt = gates();
    stmt.distinct()
        .column((Gated, LogicalFunctionRunDBIden::Id))
        .column((GateProducer, LogicalFunctionRunDBIden::TransactionId))
        .and_where(
            Expr::col((Gated, LogicalFunctionRunDBIden::Id))
                .is_in(runs.iter().map(SeaValue::as_sea_expr)),
        );
    stmt
}

pub(crate) fn transactions_read_by_query(transactions: &[TransactionId]) -> SelectStatement {
    let reader = (
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::TransactionId,
    );
    Query::select()
        .distinct()
        .column(reader)
        .column((ProducerLfr, LogicalFunctionRunDBIden::TransactionId))
        .from(LogicalFunctionRunDBIden::Table)
        .join(
            JoinType::InnerJoin,
            LogicalFunctionRunInputDBIden::Table,
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        )
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
            LogicalFunctionRunDBIden::Table,
            ProducerLfr,
            Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id)).equals((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            )),
        )
        .and_where(Expr::col(reader).is_in(transactions.iter().map(SeaValue::as_sea_expr)))
        .and_where(uncommitted(ProducerLfr))
        .and_where(
            Expr::col((ProducerLfr, LogicalFunctionRunDBIden::TransactionId)).ne(Expr::col(reader)),
        )
        .take()
}

pub(crate) fn stuck_runs_query(transactions: &[TransactionId]) -> SelectStatement {
    Query::select()
        .column(LogicalFunctionRunDBIden::TransactionId)
        .column(LogicalFunctionRunDBIden::Id)
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col(LogicalFunctionRunDBIden::TransactionId)
                .is_in(transactions.iter().map(SeaValue::as_sea_expr)),
        )
        .and_where(Expr::col(LogicalFunctionRunDBIden::Status).is_in([
            LogicalFunctionRunStatus::Failed.as_sea_expr(),
            LogicalFunctionRunStatus::OnHold.as_sea_expr(),
        ]))
        .take()
}

pub(crate) fn waiting_subscribers_query() -> SelectStatement {
    let mut stmt = gates();
    stmt.distinct()
        .column((Gated, LogicalFunctionRunDBIden::Id))
        .and_where(Expr::col((Gated, LogicalFunctionRunDBIden::Status)).is_in([
            LogicalFunctionRunStatus::Scheduled.as_sea_expr(),
            LogicalFunctionRunStatus::ReScheduled.as_sea_expr(),
        ]));
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    fn transactions() -> Vec<TransactionId> {
        vec![TransactionId::try_from("tx20260101-0000000001").unwrap()]
    }

    #[test]
    fn test_awaited_transactions_query() {
        let runs = [LogicalFunctionRunId::try_from("fr20260101-0000000001").unwrap()];
        let sql = awaited_transactions_query(&runs).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "gated"."id", "gate_producer"."transaction_id" FROM "logical_function_runs" AS "gated" INNER JOIN "functions" AS "gated_function" ON "gated_function"."id" = "gated"."function_version_id" AND "gated_function"."decorator" = 'S' INNER JOIN "logical_function_run_inputs" ON "logical_function_run_inputs"."logical_function_run_id" = "gated"."id" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "logical_function_runs" AS "gate_producer" ON "gate_producer"."id" = "logical_table_data_versions"."producer_logical_function_run_id" AND "gate_producer"."transaction_id" <> "gated"."transaction_id" AND "gate_producer"."status" <> 'C' WHERE "gated"."id" IN (202601010000000001)"#
        );
    }

    #[test]
    fn test_transactions_read_by_query() {
        let sql = transactions_read_by_query(&transactions()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "logical_function_runs"."transaction_id", "producer_lfr"."transaction_id" FROM "logical_function_runs" INNER JOIN "logical_function_run_inputs" ON "logical_function_run_inputs"."logical_function_run_id" = "logical_function_runs"."id" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "logical_function_runs" AS "producer_lfr" ON "producer_lfr"."id" = "logical_table_data_versions"."producer_logical_function_run_id" WHERE "logical_function_runs"."transaction_id" IN (202601010000000001) AND "producer_lfr"."status" <> 'C' AND "producer_lfr"."transaction_id" <> "logical_function_runs"."transaction_id""#
        );
    }

    #[test]
    fn test_stuck_runs_query() {
        let sql = stuck_runs_query(&transactions()).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "transaction_id", "id" FROM "logical_function_runs" WHERE "transaction_id" IN (202601010000000001) AND "status" IN ('F', 'H')"#
        );
    }

    #[test]
    fn test_waiting_subscribers_query() {
        let sql = waiting_subscribers_query().to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "gated"."id" FROM "logical_function_runs" AS "gated" INNER JOIN "functions" AS "gated_function" ON "gated_function"."id" = "gated"."function_version_id" AND "gated_function"."decorator" = 'S' INNER JOIN "logical_function_run_inputs" ON "logical_function_run_inputs"."logical_function_run_id" = "gated"."id" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "logical_function_runs" AS "gate_producer" ON "gate_producer"."id" = "logical_table_data_versions"."producer_logical_function_run_id" AND "gate_producer"."transaction_id" <> "gated"."transaction_id" AND "gate_producer"."status" <> 'C' WHERE "gated"."status" IN ('S', 'RS')"#
        );
    }
}
