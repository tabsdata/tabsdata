//
// Copyright 2026 Tabs Data Inc.
//

//! Physical-rooted run queries: dispatch readiness and in-flight
//! checks that read directly from `physical_function_runs` without
//! consulting the logical pointer log.
//!
//! - [`PhysicalRuns::to_dispatch`] — PFRs in `Requested` for which
//!   no [`WorkerDB`] row exists yet. The replay endpoint mints such
//!   PFRs (no pointer event); the next scheduler tick picks them up
//!   here.
//! - [`PhysicalRuns::in_flight_for`] — the newest still-running PFR
//!   under a `(execution_id, function_id)`, if any. Used by the replay
//!   endpoint's no-concurrent-replay check.

use crate::QueryExecutor;
use sea_query::{Expr, ExprTrait, JoinType, Order, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunDBIden};
use td_objects::dxo::worker::WorkerDBIden;
use td_objects::types::basic::{ExecutionId, FunctionId, FunctionRunStatus};

query_handle! {
    /// Entry trait providing
    /// [`physical_runs`](PhysicalRunsEntry::physical_runs) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`PhysicalRuns`]
    /// handle.
    pub trait PhysicalRunsEntry {
        fn physical_runs(self) -> PhysicalRuns<Self> {
            PhysicalRuns { executor: self }
        }
    }
}

/// Physical-rooted run queries. Reach via
/// [`PhysicalRunsEntry::physical_runs`]:
///
/// ```ignore
/// conn.physical_runs().to_dispatch().await?;
/// conn.physical_runs().in_flight_for(&exec_id, &fn_id).await?;
/// ```
pub struct PhysicalRuns<E> {
    pub(crate) executor: E,
}

impl<E> PhysicalRuns<E>
where
    E: QueryExecutor,
{
    /// PFRs in `Requested` for which no `WorkerDB` row references
    /// them yet. The initial dispatch path mints PFR + WorkerDB in one
    /// tx (so initial PFRs never match this filter); the replay
    /// endpoint mints just the PFR and lets the next dispatch tick
    /// wrap it — that's the only producer today.
    pub async fn to_dispatch(self) -> Result<Vec<PhysicalFunctionRunDB>, SqlError> {
        let result: Vec<PhysicalFunctionRunDB> = fetch_pfrs_to_dispatch_query()
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)?;
        Ok(result)
    }

    /// The newest still-running PFR under this `(execution_id, function_id)`
    /// (status in `Requested | Running`), if any. Used to gate replay
    /// minting: a previous attempt that's already terminal (Failed /
    /// Done) is not in flight, so a new replay can be minted.
    pub async fn in_flight_for(
        self,
        execution_id: &ExecutionId,
        function_id: &FunctionId,
    ) -> Result<Option<PhysicalFunctionRunDB>, SqlError> {
        let result: Option<PhysicalFunctionRunDB> =
            fetch_in_flight_pfr_query(execution_id, function_id)
                .build_query_as()
                .fetch_optional(self.executor)
                .await
                .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)?;
        Ok(result)
    }
}

/// Replay-side dispatch readiness. `physical_function_runs` rows whose
/// status is `Requested` and that are not yet wrapped in a worker
/// message. The `LEFT JOIN … IS NULL` is the no-WorkerDB predicate.
fn fetch_pfrs_to_dispatch_query() -> sea_query::SelectStatement {
    let mut stmt = Query::select();
    for col in <PhysicalFunctionRunDB as ta_objects::Dao>::columns() {
        stmt.column((PhysicalFunctionRunDBIden::Table, col));
    }
    stmt.from(PhysicalFunctionRunDBIden::Table)
        .join(
            JoinType::LeftJoin,
            WorkerDBIden::Table,
            Expr::col((WorkerDBIden::Table, WorkerDBIden::FunctionRunId)).equals((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::Status,
            ))
            .eq(FunctionRunStatus::Requested.as_sea_value()),
        )
        .and_where(Expr::col((WorkerDBIden::Table, WorkerDBIden::Id)).is_null())
        .order_by(
            (
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::TriggeredOn,
            ),
            Order::Asc,
        );
    stmt.take()
}

/// Most-recent in-flight PFR under `(execution_id, function_id)`. Used
/// at replay-endpoint time; returns the newest by `triggered_on` so
/// the error message names the attempt that's actually blocking.
fn fetch_in_flight_pfr_query(
    execution_id: &ExecutionId,
    function_id: &FunctionId,
) -> sea_query::SelectStatement {
    let mut stmt = Query::select();
    for col in <PhysicalFunctionRunDB as ta_objects::Dao>::columns() {
        stmt.column(col);
    }
    stmt.from(PhysicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col(PhysicalFunctionRunDBIden::ExecutionId).eq(execution_id.as_sea_value()),
        )
        .and_where(Expr::col(PhysicalFunctionRunDBIden::FunctionId).eq(function_id.as_sea_value()))
        .and_where(Expr::col(PhysicalFunctionRunDBIden::Status).is_in([
            FunctionRunStatus::Requested.as_sea_value(),
            FunctionRunStatus::Running.as_sea_value(),
        ]))
        .order_by(PhysicalFunctionRunDBIden::TriggeredOn, Order::Desc)
        .limit(1);
    stmt.take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_to_dispatch_query_sql() {
        let sql = fetch_pfrs_to_dispatch_query().to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "physical_function_runs""#),
            "scans physical_function_runs: {sql}"
        );
        assert!(
            sql.contains(r#"LEFT JOIN "workers""#),
            "left-joins workers: {sql}"
        );
        assert!(
            sql.contains(r#""workers"."id" IS NULL"#),
            "filters no-worker via IS NULL: {sql}"
        );
        assert!(
            sql.contains(r#""status" = 'RR'"#),
            "filters on Requested: {sql}"
        );
    }

    #[test]
    fn test_in_flight_query_sql() {
        let exec = ExecutionId::try_from("EP20260101.0000000001").unwrap();
        let func = FunctionId::default();
        let sql = fetch_in_flight_pfr_query(&exec, &func).to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "physical_function_runs""#),
            "scans physical_function_runs: {sql}"
        );
        assert!(
            sql.contains(r#"IN ('RR', 'R')"#),
            "filters in-flight statuses: {sql}"
        );
        assert!(sql.contains("LIMIT 1"), "limits to one: {sql}");
    }
}
