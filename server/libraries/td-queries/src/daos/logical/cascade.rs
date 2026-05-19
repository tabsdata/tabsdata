//
// Copyright 2026 Tabs Data Inc.
//

//! Logical-layer cascade queries used by execute (initial-OnHold) and by
//! the worker-callback / cancel / recover downstream propagation.

use crate::QueryExecutor;
use crate::daos::logical::chain::{ChainParams, Lp, build_chain, producer_status_in_strict};
use sea_query::{Expr, ExprTrait, JoinType, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden, LogicalTableDataVersionPointerDBIden,
};
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionRunStatus, LogicalFunctionRunId, LogicalPointerStatus,
};

query_handle! {
    /// Entry trait providing
    /// [`logical_cascade`](LogicalCascadeEntry::logical_cascade) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`LogicalCascade`]
    /// handle.
    pub trait LogicalCascadeEntry {
        fn logical_cascade(self) -> LogicalCascade<Self> {
            LogicalCascade { executor: self }
        }
    }
}

/// Cascade queries against the logical layer. Reach via
/// [`LogicalCascadeEntry::logical_cascade`]:
///
/// ```ignore
/// conn.logical_cascade().blocked_ids(&execution_id, &at).await?;
/// ```
pub struct LogicalCascade<E> {
    pub(crate) executor: E,
}

impl<E> LogicalCascade<E>
where
    E: QueryExecutor,
{
    /// Find LFR ids in this execution whose upstream LFR is Failed or
    /// OnHold (or whose input pointer is Voided). Walks the logic
    /// chain — does not require concrete `physical_function_runs` rows.
    pub async fn blocked_ids(
        self,
        execution_id: &ExecutionId,
        at: &AtTime,
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        let ids: Vec<LogicalFunctionRunId> = blocked_ids_query(execution_id, at)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;
        Ok(ids)
    }

    /// One BFS hop downstream over the logic graph. See
    /// [`downstream_one_hop_query`] for the underlying semantics.
    pub async fn downstream_one_hop(
        self,
        producer_lfrs: &[LogicalFunctionRunId],
        exclude_consumers: &[LogicalFunctionRunId],
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        let ids: Vec<LogicalFunctionRunId> =
            downstream_one_hop_query(producer_lfrs, exclude_consumers)
                .build_query_scalar()
                .fetch_all(self.executor)
                .await
                .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;
        Ok(ids)
    }
}

/// One BFS hop: LFRs consuming any output LTDV whose producer is in
/// `producer_lfrs`, excluding any consumer already in `exclude_consumers`.
/// Self-loops are excluded (a function's trigger-self LFR_input row
/// references its own output LTDV). Used by the downstream-failure
/// cascade in update_status — that cascade BFS-walks over this hop until
/// the visited set stops growing, threading already-visited ids through
/// `exclude_consumers` so SQL doesn't re-emit them.
pub(crate) fn downstream_one_hop_query(
    producer_lfrs: &[LogicalFunctionRunId],
    exclude_consumers: &[LogicalFunctionRunId],
) -> sea_query::SelectStatement {
    let producer_values: Vec<_> = producer_lfrs.iter().map(|id| id.as_sea_value()).collect();
    let mut stmt = Query::select();
    stmt.distinct()
        .column((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
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
        .and_where(
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .is_in(producer_values),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .ne(Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))),
        );
    if !exclude_consumers.is_empty() {
        let exclude_values: Vec<_> = exclude_consumers
            .iter()
            .map(|id| id.as_sea_value())
            .collect();
        stmt.and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .is_not_in(exclude_values),
        );
    }
    stmt.take()
}

/// LFRs in `execution_id` whose logical chain has at least one blocking
/// input (Voided pointer or upstream LFR Failed/OnHold).
pub(crate) fn blocked_ids_query(
    execution_id: &ExecutionId,
    at: &AtTime,
) -> sea_query::SelectStatement {
    let mut chain = build_chain(ChainParams { at_bound: Some(at) });
    chain
        .column((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::Id,
        ))
        .and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(
            Expr::col((Lp, LogicalTableDataVersionPointerDBIden::Status))
                .eq(LogicalPointerStatus::Voided.as_sea_value())
                .or(producer_status_in_strict(&[
                    FunctionRunStatus::Failed,
                    FunctionRunStatus::OnHold,
                ])),
        );

    Query::select()
        .distinct()
        .column(LogicalFunctionRunDBIden::Id)
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ExecutionId,
            ))
            .eq(execution_id.as_sea_value()),
        )
        .and_where(Expr::exists(chain))
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_blocked_ids_query_sql() {
        let eid = ExecutionId::try_from("EP20260101.0000000001").unwrap();
        let at = AtTime::now();
        let sql = blocked_ids_query(&eid, &at).to_string(SqliteQueryBuilder);
        assert!(sql.contains("SELECT DISTINCT"), "should be distinct: {sql}");
        assert!(
            sql.contains(r#"FROM "logical_function_runs""#),
            "outer scans logical_function_runs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_function_run_inputs""#),
            "subquery scans logical_function_run_inputs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_version_pointers""#),
            "joins the pointer event log: {sql}"
        );
        assert!(
            sql.contains(r#""set_on" <="#),
            "bounds the latest-pointer subquery by `at`: {sql}"
        );
        assert!(
            sql.contains("'F'") && sql.contains("'H'"),
            "filters upstream by Failed and OnHold status: {sql}"
        );
    }
}
