//
// Copyright 2026 Tabsdata Inc.
//

//! Which upstream runs hold a run on hold.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use crate::daos::function_runs::chain::{ProducerLfr, producer_chain, producer_status_in_strict};
use crate::daos::function_runs::gates::{Gate, gate_blocks, gates};
use sea_query::{Expr, ExprTrait, SelectStatement, UnionType};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
};
use td_objects::types::basic::{LogicalFunctionRunId, LogicalFunctionRunStatus};

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// `(run, upstream)` for every Failed or OnHold upstream run of the
    /// `runs`, across executions. A subscriber's upstream runs are also the
    /// ones it waits for the commit of.
    pub async fn blockers(
        self,
        runs: &[LogicalFunctionRunId],
    ) -> Result<Vec<(LogicalFunctionRunId, LogicalFunctionRunId)>, SqlError> {
        if runs.is_empty() {
            return Ok(vec![]);
        }
        blockers_query(runs)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }
}

pub(crate) fn blockers_query(runs: &[LogicalFunctionRunId]) -> SelectStatement {
    let consumer = (
        LogicalFunctionRunInputDBIden::Table,
        LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
    );
    let mut chain = producer_chain();
    chain
        .distinct()
        .column(consumer)
        .column((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .and_where(Expr::col(consumer).is_in(runs.iter().map(SeaValue::as_sea_expr)))
        .and_where(producer_status_in_strict(&[
            LogicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::OnHold,
        ]));
    let mut gated = gates();
    gated
        .distinct()
        .column(consumer)
        .column((Gate, LogicalFunctionRunDBIden::Id))
        .and_where(Expr::col(consumer).is_in(runs.iter().map(SeaValue::as_sea_expr)))
        .and_where(gate_blocks());
    chain.union(UnionType::Distinct, gated);
    chain
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_blockers_query() {
        let runs = [LogicalFunctionRunId::try_from("fr20260101-0000000001").unwrap()];
        let sql = blockers_query(&runs).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "logical_function_run_inputs"."logical_function_run_id", "producer_lfr"."id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE ("logical_table_data_versions"."producer_logical_function_run_id" IS NULL OR "logical_table_data_versions"."producer_logical_function_run_id" <> "logical_function_run_inputs"."logical_function_run_id") AND "logical_function_run_inputs"."logical_function_run_id" IN (202601010000000001) AND ("producer_lfr"."id" IS NOT NULL AND "producer_lfr"."status" IN ('F', 'H')) UNION (SELECT DISTINCT "logical_function_run_inputs"."logical_function_run_id", "gate"."id" FROM "logical_function_runs" AS "gated" INNER JOIN "functions" AS "gated_function" ON "gated_function"."id" = "gated"."function_version_id" AND "gated_function"."decorator" = 'S' INNER JOIN "logical_function_run_inputs" ON "logical_function_run_inputs"."logical_function_run_id" = "gated"."id" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" INNER JOIN "logical_function_runs" AS "gate_producer" ON "gate_producer"."id" = "logical_table_data_versions"."producer_logical_function_run_id" AND "gate_producer"."transaction_id" <> "gated"."transaction_id" INNER JOIN "logical_function_runs" AS "gate" ON "gate"."transaction_id" = "gate_producer"."transaction_id" WHERE "logical_function_run_inputs"."logical_function_run_id" IN (202601010000000001) AND "gate"."status" IN ('F', 'H'))"#
        );
    }
}
