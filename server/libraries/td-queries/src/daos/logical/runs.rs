//
// Copyright 2026 Tabs Data Inc.
//

//! Logical-rooted run queries: readiness, commit, and dispatch-time
//! enrichment of just-minted concrete `PhysicalFunctionRunDB` rows.
//!
//! - [`LogicalRuns::to_execute`] — Pending / ReScheduled LFRs whose
//!   inputs are all bound to terminal-success producers. The dispatch
//!   tick mints FR + TDVs for each via the shared mint primitive.
//! - [`LogicalRuns::to_commit`] — LFR ids whose transaction has every
//!   LFR `Done` and no blocking input on the logical chain. Caller
//!   updates those LFRs to `Committed`.
//! - [`LogicalRuns::for_dispatch`] — the denormalized
//!   `PhysicalFunctionRunToExecute` projection for a known set of just-minted
//!   FR ids; consumed by the worker-yaml builder.

use crate::QueryExecutor;
use crate::daos::logical::chain::{ChainParams, Lp, build_chain, producer_status_not_in};
use sea_query::{
    Asterisk, CommonTableExpression, Expr, ExprTrait, Func, Iden, JoinType, Order, Query,
    WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::collection::CollectionDBIden;
use td_objects::dxo::execution::ExecutionDBIden;
use td_objects::dxo::function::{FunctionDBIden, FunctionPatchDBIden};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionPointerDBIden,
};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunDBIden, PhysicalFunctionRunDBWithNamesIden,
    PhysicalFunctionRunToExecute,
};
use td_objects::dxo::user::UserDBIden;
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionRunStatus, LogicalFunctionRunId, LogicalPointerStatus,
    PhysicalFunctionRunId,
};

query_handle! {
    /// Entry trait providing [`logical_runs`](LogicalRunsEntry::logical_runs)
    /// on every [`QueryExecutor`](crate::QueryExecutor). Returns a
    /// [`LogicalRuns`] handle.
    pub trait LogicalRunsEntry {
        fn logical_runs(self) -> LogicalRuns<Self> {
            LogicalRuns { executor: self }
        }
    }
}

/// Logical-rooted run queries: readiness (`to_execute`), commit
/// readiness (`to_commit`), and dispatch-time enrichment
/// (`for_dispatch`). Reach via [`LogicalRunsEntry::logical_runs`]:
///
/// ```ignore
/// conn.logical_runs().to_execute(&at).await?;
/// conn.logical_runs().to_commit(&execution_ids).await?;
/// conn.logical_runs().for_dispatch(&fr_ids).await?;
/// ```
pub struct LogicalRuns<E> {
    pub(crate) executor: E,
}

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// LFR-rooted readiness: Pending / ReScheduled LFRs whose inputs are all
    /// bound to terminal-success producers. The dispatch tick mints FR + TDVs
    /// for each via the shared mint primitive (patch resolution happens at
    /// mint time, not here).
    pub async fn to_execute(self, at: &AtTime) -> Result<Vec<LogicalFunctionRunDB>, SqlError> {
        let result: Vec<LogicalFunctionRunDB> = fetch_lfrs_to_execute_query(at)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;

        Ok(result)
    }

    /// LFR-rooted commit readiness.
    ///
    /// Returns the LFR ids whose transaction has every LFR `Done` and no
    /// blocking input on the logical chain. Caller updates those LFRs to
    /// `Committed`. Limited to the given `execution_ids`.
    pub async fn to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        if execution_ids.is_empty() {
            return Ok(vec![]);
        }

        let result = fetch_ids_to_commit_query(execution_ids)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;

        Ok(result)
    }

    /// Fetch the denormalized `PhysicalFunctionRunToExecute` view for a known set
    /// of FR ids (no readiness predicate). Used at dispatch to enrich the
    /// just-minted FRs for the yaml builder.
    pub async fn for_dispatch(
        self,
        fr_ids: &[PhysicalFunctionRunId],
    ) -> Result<Vec<PhysicalFunctionRunToExecute>, SqlError> {
        if fr_ids.is_empty() {
            return Ok(Vec::new());
        }
        let result: Vec<PhysicalFunctionRunToExecute> = fetch_runs_for_dispatch_query(fr_ids)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalFunctionRunDB>)?;
        Ok(result)
    }
}

#[derive(Iden, Clone, Copy)]
struct TransactionsWithDoneRuns;

#[derive(Iden, Clone, Copy)]
struct TransactionsWithPendingReqs;

pub(crate) fn fetch_ids_to_commit_query(execution_ids: &[ExecutionId]) -> sea_query::WithQuery {
    let exec_values: Vec<_> = execution_ids.iter().map(|t| t.as_sea_value()).collect();

    // CTE1: transactions_with_done_runs — group LFRs (canonical status,
    // one per logical run) by transaction and require every row Done.
    let cte1_select = Query::select()
        .column(LogicalFunctionRunDBIden::TransactionId)
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(Expr::col(LogicalFunctionRunDBIden::ExecutionId).is_in(exec_values.clone()))
        .group_by_col(LogicalFunctionRunDBIden::TransactionId)
        .and_having(
            Expr::expr(Func::sum(
                Expr::case(
                    Expr::col(LogicalFunctionRunDBIden::Status)
                        .ne(FunctionRunStatus::Done.as_sea_value()),
                    1,
                )
                .finally(0),
            ))
            .eq(0),
        )
        .to_owned();

    let cte1 = CommonTableExpression::new()
        .query(cte1_select)
        .table_name(TransactionsWithDoneRuns)
        .to_owned();

    // CTE2: transactions_with_pending_reqs.
    //
    // Walks the logical chain (see [`crate::daos::logical::chain`])
    // and returns the consumer LFR's `transaction_id`. Unlike readiness,
    // the latest-pointer subquery is *not* bounded by an `at` — commit
    // runs once on the commit thread and sees whatever pointer state
    // exists right now.
    let mut cte2_select = build_chain(ChainParams { at_bound: None });
    cte2_select
        .distinct()
        .expr(Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::TransactionId,
        )))
        .join(
            JoinType::InnerJoin,
            LogicalFunctionRunDBIden::Table,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            )),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ExecutionId,
            ))
            .is_in(exec_values),
        )
        // Blocking conditions: pointer not Bound, or producer LFR not done.
        // Producer LFR NULL = lazy-mint over pre-2.0 TDV — always terminal.
        .and_where(
            Expr::col((Lp, LogicalTableDataVersionPointerDBIden::Status))
                .is_in([
                    LogicalPointerStatus::Voided.as_sea_value(),
                    LogicalPointerStatus::Pending.as_sea_value(),
                ])
                .or(producer_status_not_in(&[
                    FunctionRunStatus::Done,
                    FunctionRunStatus::Committed,
                ])),
        );

    let cte2 = CommonTableExpression::new()
        .query(cte2_select)
        .table_name(TransactionsWithPendingReqs)
        .to_owned();

    let with_clause = WithClause::new().cte(cte1).cte(cte2).to_owned();

    // Outer: pick LFR ids in transactions where every LFR is Done and
    // none of the chain checks flagged a pending requirement.
    Query::select()
        .expr(Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::Id,
        )))
        .from(LogicalFunctionRunDBIden::Table)
        .join_as(
            JoinType::Join,
            TransactionsWithDoneRuns,
            TransactionsWithDoneRuns,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .equals((
                TransactionsWithDoneRuns,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .join_as(
            JoinType::LeftJoin,
            TransactionsWithPendingReqs,
            TransactionsWithPendingReqs,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .equals((
                TransactionsWithPendingReqs,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .and_where(
            Expr::col((
                TransactionsWithPendingReqs,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .is_null(),
        )
        .to_owned()
        .with(with_clause)
}

/// LFR-rooted readiness: returns Pending/ReScheduled LFRs whose inputs are
/// all bound to terminal-success producers. Patch-agnostic — patch
/// resolution happens at mint time, not in the readiness predicate.
pub(crate) fn fetch_lfrs_to_execute_query(at: &AtTime) -> sea_query::SelectStatement {
    let mut pending_req = build_chain(ChainParams { at_bound: Some(at) });
    pending_req
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
                .is_in([
                    LogicalPointerStatus::Voided.as_sea_value(),
                    LogicalPointerStatus::Pending.as_sea_value(),
                ])
                .or(producer_status_not_in(&[
                    FunctionRunStatus::Done,
                    FunctionRunStatus::Committed,
                ])),
        );

    Query::select()
        .expr(Expr::col((LogicalFunctionRunDBIden::Table, Asterisk)))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Status,
            ))
            .is_in([
                FunctionRunStatus::Scheduled.as_sea_value(),
                FunctionRunStatus::ReScheduled.as_sea_value(),
            ]),
        )
        .and_where(Expr::exists(pending_req).not())
        .order_by(
            (
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TriggeredOn,
            ),
            Order::Desc,
        )
        .take()
}

/// FR-side projection used at dispatch enrichment. `function_version_id`
/// always holds the head fvid (FK to `functions`) and joins FunctionDB
/// directly; `function_version_patch_id` is the optional patch
/// pointer (FK to `function_patches`). Status comes natively from FR —
/// no LFR JOIN. Lives in the logical layer because its only caller is
/// the logical-driven dispatch path.
fn function_run_to_execute_select() -> sea_query::SelectStatement {
    Query::select()
        .expr(Expr::col((PhysicalFunctionRunDBIden::Table, Asterisk)))
        // Name / data_location / storage_version are head-side
        // attributes (patches don't override them). FunctionDB joins on
        // the bare `function_version_id` which always holds the head
        // fvid, so these reads are direct — no COALESCE.
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::Name)),
            PhysicalFunctionRunDBWithNamesIden::Name,
        )
        .expr_as(
            Expr::col((CollectionDBIden::Table, CollectionDBIden::Name)),
            PhysicalFunctionRunDBWithNamesIden::Collection,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::Name)),
            PhysicalFunctionRunDBWithNamesIden::Execution,
        )
        .expr_as(
            Expr::col((ExecutionDBIden::Table, ExecutionDBIden::FunctionVersionId)),
            PhysicalFunctionRunDBWithNamesIden::ExecutionFunctionVersionId,
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((UserDBIden::Table, UserDBIden::Name)),
                Expr::col((
                    PhysicalFunctionRunDBIden::Table,
                    PhysicalFunctionRunDBIden::TriggeredById,
                )),
            ])),
            PhysicalFunctionRunDBWithNamesIden::TriggeredBy,
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::DataLocation)),
            FunctionDBIden::DataLocation,
        )
        .expr_as(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::StorageVersion)),
            FunctionDBIden::StorageVersion,
        )
        // bundle_id / defined_on ARE patch-overridable. Patch
        // preferred via COALESCE (patch col first → fallback to head).
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((FunctionPatchDBIden::Table, FunctionPatchDBIden::BundleId)),
                Expr::col((FunctionDBIden::Table, FunctionDBIden::BundleId)),
            ])),
            FunctionDBIden::BundleId,
        )
        .expr_as(
            Expr::expr(Func::coalesce([
                Expr::col((FunctionPatchDBIden::Table, FunctionPatchDBIden::DefinedOn)),
                Expr::col((FunctionDBIden::Table, FunctionDBIden::DefinedOn)),
            ])),
            FunctionDBIden::DefinedOn,
        )
        .from(PhysicalFunctionRunDBIden::Table)
        .join(
            JoinType::LeftJoin,
            FunctionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionId,
            ))
            .equals((FunctionDBIden::Table, FunctionDBIden::Id)),
        )
        // Patch join: keyed on the dedicated nullable
        // `function_version_patch_id` column. NULL → no row joined;
        // FunctionPatchDB columns null in the projection.
        .join(
            JoinType::LeftJoin,
            FunctionPatchDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionPatchId,
            ))
            .equals((FunctionPatchDBIden::Table, FunctionPatchDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            CollectionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::CollectionId,
            ))
            .equals((CollectionDBIden::Table, CollectionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            ExecutionDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ExecutionId,
            ))
            .equals((ExecutionDBIden::Table, ExecutionDBIden::Id)),
        )
        .join(
            JoinType::LeftJoin,
            UserDBIden::Table,
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::TriggeredById,
            ))
            .equals((UserDBIden::Table, UserDBIden::Id)),
        )
        .order_by(
            (
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::TriggeredOn,
            ),
            Order::Desc,
        )
        .take()
}

/// Same projection/joins, filtered to a specific set of FR ids. Used at
/// dispatch to enrich just-minted `PhysicalFunctionRunDB`s into the denormalized
/// view the worker-yaml builder consumes.
pub(crate) fn fetch_runs_for_dispatch_query(
    fr_ids: &[PhysicalFunctionRunId],
) -> sea_query::SelectStatement {
    let id_values: Vec<_> = fr_ids.iter().map(|id| id.as_sea_value()).collect();
    let mut stmt = function_run_to_execute_select();
    stmt.and_where(
        Expr::col((
            PhysicalFunctionRunDBIden::Table,
            PhysicalFunctionRunDBIden::Id,
        ))
        .is_in(id_values),
    );
    stmt
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
        // CTE2 walks the logical chain — same shape as readiness, but
        // without an `at` bound (commit isn't tick-scoped).
        assert!(
            sql.contains(r#""logical_function_run_inputs""#),
            "CTE2 must scan logical_function_run_inputs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_versions""#),
            "CTE2 must join LTDV identity: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_version_pointers""#),
            "CTE2 must join the pointer event log: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "set_on" DESC, "id" DESC LIMIT 1"#),
            "CTE2 latest-pointer subquery orders by (set_on DESC, id DESC) with LIMIT 1: {sql}"
        );
        assert!(
            sql.contains(r#""producer_logical_function_run_id""#),
            "CTE2 must enforce self-loop exclusion: {sql}"
        );
        assert!(
            !sql.contains(r#""function_requirements""#),
            "CTE2 must read pending requirements through the logical layer, not function_requirements: {sql}"
        );
    }

    #[test]
    fn test_fetch_runs_for_dispatch_query_coalesces_patch_first() {
        let ids = vec![PhysicalFunctionRunId::new()];
        let sql = fetch_runs_for_dispatch_query(&ids).to_string(SqliteQueryBuilder);
        // LEFT JOIN on function_patches keyed on function_version_patch_id.
        assert!(
            sql.contains(r#"LEFT JOIN "function_patches""#),
            "must LEFT JOIN function_patches: {sql}"
        );
        assert!(
            sql.contains(r#""function_version_patch_id""#),
            "must key the patch JOIN on function_version_patch_id: {sql}"
        );
        // COALESCE with patch column FIRST, head column SECOND — patch wins.
        let coalesce_bundle =
            r#"COALESCE("function_patches"."bundle_id", "functions"."bundle_id")"#;
        let coalesce_defined =
            r#"COALESCE("function_patches"."defined_on", "functions"."defined_on")"#;
        assert!(
            sql.contains(coalesce_bundle),
            "bundle_id must coalesce patch-first then head: {sql}"
        );
        assert!(
            sql.contains(coalesce_defined),
            "defined_on must coalesce patch-first then head: {sql}"
        );
    }

    #[test]
    fn test_fetch_lfrs_to_execute_query_sql_logical_chain() {
        let sql = fetch_lfrs_to_execute_query(&AtTime::now()).to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "logical_function_runs""#),
            "outer scans logical_function_runs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_function_run_inputs""#),
            "must scan logical_function_run_inputs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_versions""#),
            "must join logical_table_data_versions: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_version_pointers""#),
            "must join the pointer event log: {sql}"
        );
        assert!(
            sql.contains(r#""producer_lfr"."status""#),
            "must read producer LFR status (LEFT JOIN, no walk through PTDV/PFR): {sql}"
        );
        assert!(
            !sql.contains(r#""physical_table_data_versions""#),
            "must NOT walk to concrete TDVs / FRs: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "set_on" DESC, "id" DESC LIMIT 1"#),
            "must order latest-pointer subquery by (set_on DESC, id DESC) with LIMIT 1: {sql}"
        );
        assert!(
            sql.contains(r#""set_on" <="#),
            "must bound the latest-pointer subquery by `at`: {sql}"
        );
    }
}
