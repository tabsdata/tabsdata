//
// Copyright 2026 Tabsdata Inc.
//

//! Attempt-table queries: replay dispatch readiness and the stale-dispatch
//! sweep.

use crate::QueryExecutor;
use crate::daos::function_runs::PhysicalRuns;
use crate::primitives::table::claim;
use sea_query::{Expr, ExprTrait, JoinType, Order, Query, SelectStatement};
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunDBIden};
use td_objects::dxo::worker::WorkerDBIden;
use td_objects::types::basic::{AtTime, PhysicalFunctionRunStatus};

impl<E> PhysicalRuns<E>
where
    E: QueryExecutor,
{
    /// `Requested` PFRs no `WorkerDB` row references yet, oldest-triggered
    /// first. Initial dispatch mints PFR and worker in one transaction, so
    /// only replay-minted PFRs match.
    pub async fn to_dispatch(self, limit: u64) -> Result<Vec<PhysicalFunctionRunDB>, SqlError> {
        fetch_pfrs_to_dispatch_query(limit)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)
    }

    /// `Dispatched` PFRs whose `dispatched_on` is strictly before `cutoff`.
    pub async fn dispatched_before(
        self,
        cutoff: &AtTime,
    ) -> Result<Vec<PhysicalFunctionRunDB>, SqlError> {
        dispatched_before_query(cutoff)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)
    }
}

/// The claim names the PFR table: PostgreSQL refuses to lock the nullable
/// side of the outer-joined `workers`.
pub(crate) fn fetch_pfrs_to_dispatch_query(limit: u64) -> SelectStatement {
    let mut stmt = Query::select();
    for col in PhysicalFunctionRunDB::columns() {
        stmt.column((PhysicalFunctionRunDBIden::Table, col));
    }
    stmt.from(PhysicalFunctionRunDBIden::Table)
        .join(
            JoinType::LeftJoin,
            WorkerDBIden::Table,
            Expr::col((WorkerDBIden::Table, WorkerDBIden::PhysicalFunctionRunId)).equals((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::Status,
            ))
            .eq(PhysicalFunctionRunStatus::Requested.as_sea_expr()),
        )
        .and_where(Expr::col((WorkerDBIden::Table, WorkerDBIden::Id)).is_null())
        .order_by(
            (
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ValidFrom,
            ),
            Order::Asc,
        )
        .limit(limit);
    claim::<PhysicalFunctionRunDB>(&mut stmt);
    stmt.take()
}

pub(crate) fn dispatched_before_query(cutoff: &AtTime) -> SelectStatement {
    Query::select()
        .columns(PhysicalFunctionRunDB::columns())
        .from(PhysicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col(PhysicalFunctionRunDBIden::Status)
                .eq(PhysicalFunctionRunStatus::Dispatched.as_sea_expr()),
        )
        .and_where(Expr::col(PhysicalFunctionRunDBIden::DispatchedOn).lt(cutoff.as_sea_expr()))
        .order_by(PhysicalFunctionRunDBIden::DispatchedOn, Order::Asc)
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use td_common::datetime::Timestamp;

    #[test]
    fn test_fetch_pfrs_to_dispatch_query() {
        let sql = fetch_pfrs_to_dispatch_query(50).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_function_runs"."id", "physical_function_runs"."collection_id", "physical_function_runs"."function_id", "physical_function_runs"."function_version_id", "physical_function_runs"."execution_id", "physical_function_runs"."transaction_id", "physical_function_runs"."logical_function_run_id", "physical_function_runs"."triggered_by_id", "physical_function_runs"."trigger", "physical_function_runs"."base_path", "physical_function_runs"."dispatched_on", "physical_function_runs"."started_on", "physical_function_runs"."ended_on", "physical_function_runs"."status", "physical_function_runs"."function_version_patch_id", "physical_function_runs"."project_id", "physical_function_runs"."valid_from", "physical_function_runs"."valid_to" FROM "physical_function_runs" LEFT JOIN "workers" ON "workers"."physical_function_run_id" = "physical_function_runs"."id" WHERE "physical_function_runs"."status" = 'RR' AND "workers"."id" IS NULL ORDER BY "physical_function_runs"."valid_from" ASC LIMIT 50 FOR UPDATE OF "physical_function_runs" SKIP LOCKED"#
        );
    }

    #[test]
    fn test_dispatched_before_query() {
        let cutoff = AtTime::from(Timestamp::try_from(1_700_000_000_000_i64).unwrap());
        let sql = dispatched_before_query(&cutoff).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "collection_id", "function_id", "function_version_id", "execution_id", "transaction_id", "logical_function_run_id", "triggered_by_id", "trigger", "base_path", "dispatched_on", "started_on", "ended_on", "status", "function_version_patch_id", "project_id", "valid_from", "valid_to" FROM "physical_function_runs" WHERE "status" = 'DP' AND "dispatched_on" < '2023-11-14 22:13:20.000000 +00:00' ORDER BY "dispatched_on" ASC"#
        );
    }
}
