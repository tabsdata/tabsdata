//
// Copyright 2026 Tabs Data Inc.
//

//! Function-run queries — both logical (LFR) and physical (PFR).
//!
//! - [`Lfr`] — LFR identity-table operations: readiness, commit,
//!   dispatch-time enrichment, plus a `.pointers()` constructor for the
//!   LFR pointer event log.
//! - [`Pfr`] — PFR identity-table operations: replay dispatch, in-flight
//!   checks. No pointer event log on the physical side.

use crate::QueryExecutor;
use crate::daos::pointers::{LogicalPointers, ProducerLfr};
use sea_query::{
    Asterisk, CommonTableExpression, Expr, ExprTrait, Func, Iden, JoinType, Order, Query,
    SelectStatement, SimpleExpr, SubQueryStatement, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::{Dao, DaoCore};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::collection::CollectionDBIden;
use td_objects::dxo::execution::ExecutionDBIden;
use td_objects::dxo::function::{FunctionDBIden, FunctionPatchDBIden};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalFunctionRunPointerDB, LogicalTableDataVersionDBIden,
    LogicalTableDataVersionPointerDBIden,
};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunDBIden, PhysicalFunctionRunDBWithNamesIden,
    PhysicalFunctionRunToExecute,
};
use td_objects::dxo::user::UserDBIden;
use td_objects::dxo::worker::WorkerDBIden;
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
    LogicalPointerStatus, PhysicalFunctionRunId, TriggeredOn,
};

query_handle! {
    /// Entry trait providing [`lfr`](LfrEntry::lfr) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns an [`Lfr`] handle.
    pub trait LfrEntry {
        fn lfr(self) -> Lfr<Self> {
            Lfr { executor: self }
        }
    }
}

query_handle! {
    /// Entry trait providing [`pfr`](PfrEntry::pfr) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Pfr`] handle.
    pub trait PfrEntry {
        fn pfr(self) -> Pfr<Self> {
            Pfr { executor: self }
        }
    }
}

/// LFR-rooted run queries: readiness, commit, dispatch-time
/// enrichment, plus a [`pointers`](Lfr::pointers) constructor for the
/// LFR pointer event log. Reach via [`LfrEntry::lfr`]:
///
/// ```ignore
/// conn.lfr().to_execute(&at).await?;
/// conn.lfr().to_commit(&execution_ids).await?;
/// conn.lfr().for_dispatch(&fr_ids).await?;
/// conn.lfr().pointers().at(&at).concrete(&lfr_id).await?;
/// ```
pub struct Lfr<E> {
    pub(crate) executor: E,
}

/// PFR-rooted run queries: replay dispatch readiness, in-flight checks.
/// Reach via [`PfrEntry::pfr`]:
///
/// ```ignore
/// conn.pfr().to_dispatch().await?;
/// conn.pfr().in_flight_for(&exec_id, &fn_id).await?;
/// ```
pub struct Pfr<E> {
    pub(crate) executor: E,
}

impl<E> Lfr<E>
where
    E: QueryExecutor,
{
    /// Pointer event log for LFRs (`logical_function_run_pointers`).
    pub fn pointers(self) -> LogicalPointers<E, LogicalFunctionRunPointerDB> {
        LogicalPointers::new(self.executor)
    }

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

    /// The most recent LFR for `function_id` whose `triggered_on` is
    /// strictly before `before_time` — `<` (not `<=`) cleanly excludes
    /// the in-flight LFR, whose `triggered_on` equals the caller's
    /// argument by construction. Returns `None` if the function has no
    /// prior run.
    pub async fn previous_for_function_at(
        self,
        function_id: &FunctionId,
        before_time: &TriggeredOn,
    ) -> Result<Option<LogicalFunctionRunDB>, SqlError> {
        let row = previous_lfr_query(function_id, before_time)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;
        Ok(row)
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

    /// LFR ids in `execution_id` whose upstream LFR is Failed or OnHold
    /// (or whose input pointer is Voided). Walks the logical chain —
    /// does not require concrete `physical_function_runs` rows.
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

impl<E> Pfr<E>
where
    E: QueryExecutor,
{
    /// PFRs in `Requested` for which no `WorkerDB` row references them
    /// yet. The initial dispatch path mints PFR + WorkerDB in one tx
    /// (so initial PFRs never match this filter); the replay endpoint
    /// mints just the PFR and lets the next dispatch tick wrap it —
    /// that's the only producer today.
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

/// Joined-with-AS alias for the latest LTDV pointer event row.
#[derive(Iden, Clone, Copy)]
struct Lp;

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
                    LogicalFunctionRunStatus::Failed,
                    LogicalFunctionRunStatus::OnHold,
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

pub(crate) fn previous_lfr_query(
    function_id: &FunctionId,
    before_time: &TriggeredOn,
) -> sea_query::SelectStatement {
    Query::select()
        .columns(LogicalFunctionRunDB::columns())
        .from(LogicalFunctionRunDB::table())
        .and_where(Expr::col(LogicalFunctionRunDBIden::FunctionId).eq(function_id.as_sea_value()))
        .and_where(Expr::col(LogicalFunctionRunDBIden::TriggeredOn).lt(before_time.as_sea_value()))
        .order_by(LogicalFunctionRunDBIden::TriggeredOn, Order::Desc)
        .order_by(LogicalFunctionRunDBIden::Id, Order::Desc)
        .limit(1)
        .take()
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
                        .ne(LogicalFunctionRunStatus::Done.as_sea_value()),
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
                    LogicalFunctionRunStatus::Done,
                    LogicalFunctionRunStatus::Committed,
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
                    LogicalFunctionRunStatus::Done,
                    LogicalFunctionRunStatus::Committed,
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
                LogicalFunctionRunStatus::Scheduled.as_sea_value(),
                LogicalFunctionRunStatus::ReScheduled.as_sea_value(),
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
            .eq(LogicalFunctionRunStatus::Requested.as_sea_value()),
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
/// at replay-endpoint time; returns the newest by `triggered_on` so the
/// error message names the attempt that's actually blocking.
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
            LogicalFunctionRunStatus::Requested.as_sea_value(),
            LogicalFunctionRunStatus::Running.as_sea_value(),
        ]))
        .order_by(PhysicalFunctionRunDBIden::TriggeredOn, Order::Desc)
        .limit(1);
    stmt.take()
}

// ---------------------------------------------------------------------------
// Chain SQL primitives — shared scaffolding for the LFR readiness/blocked
// queries above. The chain walks `FFR_input → LTDV → latest pointer event →
// producer_LFR (status)`. NULL `producer_lfr` (lazy-mint over a pre-2.0
// concrete TDV) is treated as terminal.
// ---------------------------------------------------------------------------

/// True when the producer LFR is present (not a lazy-mint) AND its
/// status is one of `statuses`. NULL producer (lazy-mint) does not
/// qualify — use when "lazy = terminal/healthy" should NOT trigger the
/// predicate (e.g. cascade "is upstream failed?" checks).
pub(crate) fn producer_status_in_strict(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_not_null()
        .and(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_in(values))
}

/// True when the producer LFR is present AND its status is NOT one of
/// `statuses`. NULL producer (lazy-mint) does not qualify — readiness
/// treats lazy-mints as terminal/done. Use as the readiness-blocking
/// predicate (status not in {Done, Committed}).
pub(crate) fn producer_status_not_in(statuses: &[LogicalFunctionRunStatus]) -> SimpleExpr {
    let values: Vec<_> = statuses.iter().map(|s| s.as_sea_value()).collect();
    Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Id))
        .is_not_null()
        .and(Expr::col((ProducerLfr, LogicalFunctionRunDBIden::Status)).is_not_in(values))
}

/// Shape parameters. `at_bound = Some(at)` bounds the latest-pointer
/// subquery by `set_on <= at` (readiness, blocked).
pub(crate) struct ChainParams<'a> {
    pub(crate) at_bound: Option<&'a AtTime>,
}

/// Build a `SelectStatement` joining `logical_function_run_inputs →
/// logical_table_data_versions → latest pointer event` and the
/// NULL-safe self-loop exclusion in the WHERE. Caller adds projection,
/// outer scope, and any additional filters on `LTDV.status` or
/// `Lp.status`.
pub(crate) fn build_chain(params: ChainParams<'_>) -> SelectStatement {
    let ChainParams { at_bound } = params;

    let mut latest_id_subq_select = Query::select()
        .column(LogicalTableDataVersionPointerDBIden::Id)
        .from(LogicalTableDataVersionPointerDBIden::Table)
        .and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
            ))
            .equals((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
        )
        .to_owned();
    if let Some(at) = at_bound {
        latest_id_subq_select.and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::SetOn,
            ))
            .lte(at.as_sea_value()),
        );
    }
    latest_id_subq_select
        .order_by(LogicalTableDataVersionPointerDBIden::SetOn, Order::Desc)
        .order_by(LogicalTableDataVersionPointerDBIden::Id, Order::Desc)
        .limit(1);
    let latest_id_subq = Expr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(latest_id_subq_select)),
    );

    let mut stmt = Query::select()
        .from(LogicalFunctionRunInputDBIden::Table)
        .to_owned();

    stmt.join(
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
        LogicalTableDataVersionPointerDBIden::Table,
        Lp,
        Expr::col((Lp, LogicalTableDataVersionPointerDBIden::Id)).eq(latest_id_subq),
    )
    .join_as(
        JoinType::LeftJoin,
        LogicalFunctionRunDBIden::Table,
        ProducerLfr,
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
    )
    .and_where(
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .is_null()
        .or(Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .ne(Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        )))),
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
