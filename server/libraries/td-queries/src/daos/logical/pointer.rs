//
// Copyright 2026 Tabs Data Inc.
//

//! Pointer-event state-machine operations for LFR / LTDV.
//!
//! Each operation appends a single event to the relevant pointer-event
//! table. The "current" pointer for a logical identity is the latest
//! event row by `(set_on DESC, id DESC)` for that logical id. The
//! caller must already hold a transactional connection.
//!
//! LFR and LTDV share a common state machine. The schema shape lives on
//! [`LogicalPointerDao`] (in `td_objects` — emitted by the `Dao` macro's
//! `logical_pointer(...)` arg).
//!
//! ## Surface
//!
//! Sync builders [`redirect_event`] / [`void_event`] take the latest
//! pointer event the caller already has and return `Option<P>` — `None`
//! when the operation is idempotent (already in the target state). The
//! caller inserts the event. Splitting "build" from "insert" keeps the
//! helper sync, which dodges the HRTB `Send` bound that `service_factory`
//! synthesizes for layer fns and lets these helpers be called freely
//! from inside any tower chain.
//!
//! The async view [`LogicalPointers::concrete_at`] is reached via the
//! [`LogicalPointersEntry::logical_pointers`] handle, matching the
//! per-DAO handle pattern used by [`crate::PhysicalFunctionRuns`],
//! [`crate::Sessions`], etc.

use crate::daos::logical::chain::{ProducerLfr, producer_status_in_or_lazy};
use crate::primitives::chunked::MAX_QUERY_CONDITIONS;
use crate::{ChunkedQueries, LiveEntry, QueryExecutor, VersionedEntry};
use futures_util::future::BoxFuture;
use sea_query::{
    Alias, Asterisk, CommonTableExpression, Expr, ExprTrait, Func, Order, OverStatement, Query,
    WindowStatement, WithClause,
};
use std::collections::HashMap;
use std::hash::Hash;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_error::{ApiError, TdError, api_error};
use td_objects::SeaValue;
use td_objects::and;
use td_objects::dxo::logical::{
    LogicalPointerDao, LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::lte;
use td_objects::types::basic::{
    AtTime, HasData, LogicalPointerReason, LogicalPointerStatus, PhysicalFunctionRunStatus,
    PhysicalTableDataVersionId, TableId, TriggeredOn, UserId,
};

query_handle! {
    /// Entry trait providing
    /// [`logical_pointers`](LogicalPointersEntry::logical_pointers) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`LogicalPointers`]
    /// handle.
    pub trait LogicalPointersEntry {
        fn logical_pointers(self) -> LogicalPointers<Self> {
            LogicalPointers { executor: self }
        }
    }
}

/// Async view operations on logical pointer event logs (LFR or
/// LTDV). Reach via [`LogicalPointersEntry::logical_pointers`]:
///
/// ```ignore
/// conn.logical_pointers()
///     .concrete_at::<LogicalFunctionRunPointerDB>(&lfr_id, &at).await?;
/// ```
///
/// Sync builders for redirect/void events are at module scope —
/// see [`redirect_event`] / [`void_event`].
pub struct LogicalPointers<E> {
    pub(crate) executor: E,
}

impl<E> LogicalPointers<E>
where
    E: QueryExecutor,
{
    /// Concrete id this logical pointer was *bound to* on or before
    /// `at`. `Ok(None)` when pre-mint, currently Pending, or Voided.
    /// Explicit `status == Bound` filter — Pending and Voided events
    /// carry a NULL concrete by construction, so the filter is
    /// belt-and-braces today, but it pins the semantic to the
    /// docstring so a future state with a non-NULL concrete on a
    /// non-Bound event can't silently mislead callers.
    pub async fn concrete_at<P: LogicalPointerDao>(
        self,
        logical_id: &P::LogicalId,
        at: &AtTime,
    ) -> Result<Option<P::ConcreteId>, SqlError> {
        let row: Option<P> = self
            .executor
            .versioned(at)
            .fetch_optional(logical_id)
            .await?;
        Ok(row.and_then(|p| {
            if matches!(p.status(), LogicalPointerStatus::Bound) {
                p.concrete_id().cloned()
            } else {
                None
            }
        }))
    }

    /// Auto-dispatch `Pending → Bound` (or idempotent `Bound → same`).
    /// Caller must already have fetched the latest pointer for
    /// `logical_id`. Refuses `Bound → different concrete` (would be a
    /// silent rebind without operator action — use [`redirect_event`]
    /// for that), `Voided → Bound` (a voided identity must be
    /// explicitly revived via [`redirect_event`]), and `Requested → Bound`
    /// (a replay attempt is in flight — the worker callback's
    /// redirect-at-finish layer is the only path that flips
    /// `Requested → Bound`).
    pub async fn bind_with_known_state<P: LogicalPointerDao>(
        self,
        logical_id: &P::LogicalId,
        new_concrete_id: &P::ConcreteId,
        latest: &P,
        set_on: &AtTime,
        set_by_id: &UserId,
    ) -> Result<(), TdError> {
        match latest.status() {
            LogicalPointerStatus::Bound => {
                if latest.concrete_id() == Some(new_concrete_id) {
                    return Ok(());
                }
                return Err(api_error!(
                    ApiError::InternalError,
                    "{} {} already Bound to {:?}; refusing to rebind to {} automatically",
                    P::KIND,
                    logical_id,
                    latest.concrete_id(),
                    new_concrete_id,
                ));
            }
            LogicalPointerStatus::Voided => {
                return Err(api_error!(
                    ApiError::InternalError,
                    "{} {} is Voided; cannot bind",
                    P::KIND,
                    logical_id,
                ));
            }
            LogicalPointerStatus::Pending => {}
        }
        let event = P::build_event(
            logical_id,
            LogicalPointerStatus::Bound,
            Some(new_concrete_id.clone()),
            *set_on,
            *set_by_id,
            LogicalPointerReason::InitialDispatch,
        )?;
        self.executor.live().insert::<P>(&event).await?;
        Ok(())
    }

    /// Latest pointer event row per partition, batched. Each
    /// `logical_id` in `ids` maps to its latest event row at `at`;
    /// partitions with no events at all are absent from the map.
    pub async fn latest_pointers_chunked<P>(
        self,
        ids: &[P::LogicalId],
        at: &AtTime,
    ) -> Result<HashMap<P::LogicalId, P>, SqlError>
    where
        P: LogicalPointerDao,
        P::LogicalId: Hash + Eq,
    {
        let rows: Vec<P> = self
            .executor
            .versioned(at)
            .fetch_all_chunked::<P>(ids)
            .await?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            out.insert(row.logical_id().clone(), row);
        }
        Ok(out)
    }

    /// Method form of [`latest_bound_tdv_with_data_for_table`] (free
    /// function below). Returns a boxed future so the multi-step
    /// `self.reuse()` + await + `self.reuse()` body doesn't trip the
    /// HRTB inference failure in `SeaExecutor::Reused<'a>` when called
    /// from tower layer fns (the layer macro synthesizes a `for<'a>`
    /// bound that opaque `async fn` returns can't satisfy with this
    /// pattern; `BoxFuture` erases the lifetime).
    pub fn latest_bound_tdv_with_data_for_table<'a>(
        self,
        table_id: &'a TableId,
        triggered_on: &'a TriggeredOn,
        at: &'a AtTime,
        committed_only: bool,
    ) -> BoxFuture<'a, Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError>>
    where
        Self: 'a + Send,
    {
        Box::pin(async move {
            let mut executor = self.executor;
            let ltdvs: Vec<LogicalTableDataVersionDB> = executor
                .reuse()
                .live()
                .fetch_all(and!(table_id, lte!(triggered_on)))
                .await?;
            if ltdvs.is_empty() {
                return Ok(None);
            }

            let ltdv_ids: Vec<_> = ltdvs.iter().map(|l| l.id.clone()).collect();
            let pointers = executor
                .reuse()
                .logical_pointers()
                .latest_pointers_chunked::<LogicalTableDataVersionPointerDB>(&ltdv_ids, at)
                .await?;

            let bound_tdv_ids: Vec<PhysicalTableDataVersionId> = ltdvs
                .iter()
                .filter_map(|ltdv| {
                    pointers
                        .get(&ltdv.id)
                        .filter(|p| matches!(p.status, LogicalPointerStatus::Bound))
                        .and_then(|p| p.physical_table_data_version_id.clone())
                })
                .collect();
            if bound_tdv_ids.is_empty() {
                return Ok(None);
            }

            let tdvs: Vec<PhysicalTableDataVersionDBWithNames> = executor
                .reuse()
                .fetch_all_chunked::<PhysicalTableDataVersionDBWithNames, _>(&bound_tdv_ids)
                .await?;
            let tdvs_by_id: HashMap<
                PhysicalTableDataVersionId,
                PhysicalTableDataVersionDBWithNames,
            > = tdvs.into_iter().map(|t| (t.id.clone(), t)).collect();

            Ok(ltdvs.into_iter().find_map(|ltdv| {
                let pointer = pointers.get(&ltdv.id)?;
                if !matches!(pointer.status, LogicalPointerStatus::Bound) {
                    return None;
                }
                let tdv_id = pointer.physical_table_data_version_id.as_ref()?;
                let tdv = tdvs_by_id.get(tdv_id)?;
                if !matches!(tdv.has_data, Some(HasData::TRUE)) {
                    return None;
                }
                if committed_only && tdv.status != PhysicalFunctionRunStatus::Done {
                    return None;
                }
                Some(tdv.clone())
            }))
        })
    }

    /// Concrete id of the second-most-recent `Bound` event for
    /// `logical_id`. `Ok(None)` when the partition has fewer than
    /// two `Bound` events — i.e. no prior binding to roll back to.
    pub async fn previous_bound<P>(
        self,
        logical_id: &P::LogicalId,
    ) -> Result<Option<P::ConcreteId>, SqlError>
    where
        P: LogicalPointerDao,
    {
        let row: Option<P> = previous_bound_query::<P>(logical_id)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<P>)?;
        Ok(row.and_then(|p| p.concrete_id().cloned()))
    }

    /// Per-partition second-most-recent `Bound` concrete id, batched.
    /// Each `logical_id` in `ids` maps to `Some(concrete)` if a prior
    /// binding exists, or is absent from the map otherwise. Issues one
    /// query per chunk of [`MAX_QUERY_CONDITIONS`] ids.
    pub async fn previous_bound_chunked<P>(
        mut self,
        ids: &[P::LogicalId],
    ) -> Result<HashMap<P::LogicalId, P::ConcreteId>, SqlError>
    where
        P: LogicalPointerDao,
        P::LogicalId: Hash + Eq,
    {
        let mut out: HashMap<P::LogicalId, P::ConcreteId> = HashMap::new();
        for chunk in ids.chunks(MAX_QUERY_CONDITIONS) {
            let rows: Vec<P> = previous_bound_chunked_query::<P>(chunk)
                .build_query_as()
                .fetch_all(self.executor.reuse())
                .await
                .map_err(SqlError::on_read::<P>)?;
            for row in rows {
                if let Some(concrete) = row.concrete_id().cloned() {
                    out.insert(row.logical_id().clone(), concrete);
                }
            }
        }
        Ok(out)
    }
}

/// `PTDV.id IN (…)` filter expression matching every PTDV canonically
/// bound under a sticky-`Committed` LTDV. Walks
/// `LTDV(status=Committed) → latest Bound LTDV-pointer event →
/// physical_table_data_version_id`. Drop into any predicate Expr that
/// wants the "committed-only" projection of physical table data
/// versions — e.g. the listing endpoint's `uncommitted=false` filter.
///
/// PFR.status is per-attempt audit on a different entity and never
/// participates here.
pub fn ptdv_is_canonical_committed() -> sea_query::SimpleExpr {
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBIden;
    Expr::col(PhysicalTableDataVersionDBIden::Id)
        .in_subquery(canonical_committed_ptdv_ids_subquery())
}

/// Internal SQL builder for [`ptdv_is_canonical_committed`]. Kept
/// non-pub so callers reach for the high-level filter rather than
/// reassembling the subquery themselves.
fn canonical_committed_ptdv_ids_subquery() -> sea_query::SelectStatement {
    use sea_query::Alias;
    use td_objects::dxo::logical::{
        LogicalFunctionRunDBIden, LogicalTableDataVersionDBIden,
        LogicalTableDataVersionPointerDBIden,
    };
    use td_objects::types::basic::FunctionRunStatus;

    let ltdvp = Alias::new("ltdvp");

    // Inner: the latest pointer-event id per LTDV (correlated to the
    // outer JOIN's LTDV identity row).
    let latest_event_per_ltdv = Query::select()
        .column(LogicalTableDataVersionPointerDBIden::Id)
        .from(LogicalTableDataVersionPointerDBIden::Table)
        .and_where(
            Expr::col(LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId).equals((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
        )
        .order_by(LogicalTableDataVersionPointerDBIden::SetOn, Order::Desc)
        .order_by(LogicalTableDataVersionPointerDBIden::Id, Order::Desc)
        .limit(1)
        .to_owned();

    Query::select()
        .expr(Expr::col((
            ltdvp.clone(),
            LogicalTableDataVersionPointerDBIden::PhysicalTableDataVersionId,
        )))
        .from(LogicalTableDataVersionDBIden::Table)
        .join_as(
            sea_query::JoinType::InnerJoin,
            LogicalTableDataVersionPointerDBIden::Table,
            ltdvp.clone(),
            Expr::col((ltdvp.clone(), LogicalTableDataVersionPointerDBIden::Id))
                .in_subquery(latest_event_per_ltdv),
        )
        // Producer LFR is the canonical status source. LEFT JOIN so
        // lazy-mints (NULL producer) still qualify as "committed" —
        // their underlying PTDVs are pre-2.0 already-committed data.
        .join_as(
            sea_query::JoinType::LeftJoin,
            LogicalFunctionRunDBIden::Table,
            ProducerLfr,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
        )
        .and_where(producer_status_in_or_lazy(&[FunctionRunStatus::Committed]))
        .and_where(
            Expr::col((ltdvp, LogicalTableDataVersionPointerDBIden::Status))
                .eq(LogicalPointerStatus::Bound.as_sea_value()),
        )
        .to_owned()
}

fn previous_bound_query<P: LogicalPointerDao>(
    logical_id: &P::LogicalId,
) -> sea_query::SelectStatement {
    Query::select()
        .columns(P::columns())
        .from(P::table())
        .and_where(Expr::col(P::logical_id_col()).eq(logical_id.as_sea_value()))
        .and_where(Expr::col(P::status_col()).eq(LogicalPointerStatus::Bound.as_sea_value()))
        .order_by(P::version_order_col(), Order::Desc)
        .order_by(P::pk_col(), Order::Desc)
        .limit(1)
        .offset(1)
        .take()
}

/// Per-partition second-most-recent `Bound` row for the given chunk.
/// Window function `ROW_NUMBER() OVER (PARTITION BY logical_id ORDER
/// BY set_on DESC, pk DESC)` ranked over `Bound`-status rows; the
/// outer SELECT keeps only `rn = 2`.
fn previous_bound_chunked_query<P: LogicalPointerDao>(
    chunk: &[P::LogicalId],
) -> sea_query::WithQuery {
    let id_values: Vec<_> = chunk.iter().map(|id| id.as_sea_value()).collect();
    let cte_name = Alias::new("ranked");
    let rn_col = Alias::new("rn");

    let window = WindowStatement::new()
        .partition_by(P::logical_id_col())
        .order_by_expr(Expr::col(P::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(P::pk_col()), Order::Desc)
        .take();

    let ranked = Query::select()
        .columns(P::columns())
        .from(P::table())
        .and_where(Expr::col(P::logical_id_col()).is_in(id_values))
        .and_where(Expr::col(P::status_col()).eq(LogicalPointerStatus::Bound.as_sea_value()))
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(Vec::<Expr>::new()),
            window,
            rn_col.clone(),
        )
        .take();

    let with = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .query(ranked)
                .table_name(cte_name.clone())
                .to_owned(),
        )
        .to_owned();

    Query::select()
        .column(Asterisk)
        .from(cte_name)
        .and_where(Expr::col(rn_col).eq(2))
        .take()
        .with(with)
}

/// Build a `Bound` event tagged with `reason`, or `None` if idempotent.
///
/// Idempotency: when `latest` is already `Bound` to `new_concrete_id`,
/// returns `Ok(None)` — re-issuing the same redirect must not append a
/// duplicate event. Otherwise returns `Ok(Some(event))` and the caller
/// inserts the event into the relevant pointer table.
///
/// Allowed by the state machine:
/// - `Pending → Bound(new_concrete_id)` (commit a pending identity),
/// - `Requested → Bound(new_concrete_id)` (worker finish on a replay),
/// - `Bound(old) → Bound(new_concrete_id)` when `old != new_concrete_id`,
/// - `Voided → Bound(new_concrete_id)` (revive a voided identity).
///
/// `reason` distinguishes operator-driven redirects (`OperatorRedirect`,
/// requires `set_by_id` = the operator's user id, never `UserId::admin`)
/// from worker-finish replay redirects (`ReplayRedirect`,
/// `set_by_id` = the FR's `triggered_by_id`).
pub fn redirect_event<P: LogicalPointerDao>(
    latest: &P,
    logical_id: &P::LogicalId,
    new_concrete_id: &P::ConcreteId,
    set_on: &AtTime,
    set_by_id: &UserId,
    reason: LogicalPointerReason,
) -> Result<Option<P>, TdError> {
    if latest.status() == LogicalPointerStatus::Bound
        && latest.concrete_id() == Some(new_concrete_id)
    {
        return Ok(None);
    }
    let event = P::build_event(
        logical_id,
        LogicalPointerStatus::Bound,
        Some(new_concrete_id.clone()),
        *set_on,
        *set_by_id,
        reason,
    )?;
    Ok(Some(event))
}

/// Build a `Voided` event tagged with `reason`, or `None` if idempotent.
///
/// Idempotency: when `latest` is already `Voided`, returns `Ok(None)`.
/// Otherwise returns `Ok(Some(event))` and the caller inserts the
/// event into the relevant pointer table. The void event row carries a
/// NULL concrete id; the prior binding (if any) is reachable by
/// walking back to the previous event row in the log.
///
/// Allowed by the state machine:
/// - `Pending → Voided`,
/// - `Requested → Voided` (a replay attempt failed; previous Bound
///   stands),
/// - `Bound(_) → Voided`.
///
/// `reason` distinguishes operator-driven voids (`OperatorForceVoid`,
/// requires `set_by_id` = operator user id) from replay failure-side
/// voids (`ReplayVoid`, `set_by_id` = the FR's `triggered_by_id`).
pub fn void_event<P: LogicalPointerDao>(
    latest: &P,
    logical_id: &P::LogicalId,
    set_on: &AtTime,
    set_by_id: &UserId,
    reason: LogicalPointerReason,
) -> Result<Option<P>, TdError> {
    if latest.status() == LogicalPointerStatus::Voided {
        return Ok(None);
    }
    let event = P::build_event(
        logical_id,
        LogicalPointerStatus::Voided,
        None,
        *set_on,
        *set_by_id,
        reason,
    )?;
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_objects::Buildable;
    use td_objects::dxo::logical::LogicalFunctionRunPointerDB;
    use td_objects::types::basic::{LogicalFunctionRunId, PhysicalFunctionRunId, UserId};

    fn build_pointer(
        logical_id: LogicalFunctionRunId,
        status: LogicalPointerStatus,
        concrete: Option<PhysicalFunctionRunId>,
    ) -> LogicalFunctionRunPointerDB {
        LogicalFunctionRunPointerDB::builder()
            .logical_function_run_id(logical_id)
            .status(status)
            .physical_function_run_id(concrete)
            .set_on(AtTime::now())
            .set_by_id(UserId::admin())
            .reason(LogicalPointerReason::InitialDispatch)
            .build()
            .unwrap()
    }

    #[test]
    fn test_redirect_event_idempotent_when_bound_same_concrete() {
        let id = LogicalFunctionRunId::new();
        let fr = PhysicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Bound, Some(fr.clone()));
        let result = redirect_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &fr,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorRedirect,
        )
        .unwrap();
        assert!(
            result.is_none(),
            "redirect must no-op when already Bound to same concrete"
        );
    }

    #[test]
    fn test_redirect_event_emits_when_bound_different_concrete() {
        let id = LogicalFunctionRunId::new();
        let old = PhysicalFunctionRunId::new();
        let new = PhysicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Bound, Some(old));
        let result = redirect_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &new,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorRedirect,
        )
        .unwrap();
        let event = result.expect("redirect to a different concrete must emit");
        assert_eq!(event.status, LogicalPointerStatus::Bound);
        assert_eq!(event.physical_function_run_id.as_ref(), Some(&new));
        assert_eq!(event.reason, LogicalPointerReason::OperatorRedirect);
    }

    #[test]
    fn test_redirect_event_emits_when_pending() {
        let id = LogicalFunctionRunId::new();
        let new = PhysicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Pending, None);
        let result = redirect_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &new,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorRedirect,
        )
        .unwrap();
        assert!(result.is_some(), "redirect from Pending must emit");
    }

    #[test]
    fn test_redirect_event_emits_when_voided() {
        let id = LogicalFunctionRunId::new();
        let new = PhysicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Voided, None);
        let result = redirect_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &new,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorRedirect,
        )
        .unwrap();
        assert!(result.is_some(), "redirect from Voided (revive) must emit");
    }

    #[test]
    fn test_void_event_idempotent_when_already_voided() {
        let id = LogicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Voided, None);
        let result = void_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorForceVoid,
        )
        .unwrap();
        assert!(result.is_none(), "void must no-op when already Voided");
    }

    #[test]
    fn test_void_event_emits_when_bound() {
        let id = LogicalFunctionRunId::new();
        let fr = PhysicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Bound, Some(fr));
        let result = void_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorForceVoid,
        )
        .unwrap();
        let event = result.expect("void from Bound must emit");
        assert_eq!(event.status, LogicalPointerStatus::Voided);
        assert!(event.physical_function_run_id.is_none());
        assert_eq!(event.reason, LogicalPointerReason::OperatorForceVoid);
    }

    #[test]
    fn test_void_event_emits_when_pending() {
        let id = LogicalFunctionRunId::new();
        let latest = build_pointer(id.clone(), LogicalPointerStatus::Pending, None);
        let result = void_event::<LogicalFunctionRunPointerDB>(
            &latest,
            &id,
            &AtTime::now(),
            &UserId::admin(),
            LogicalPointerReason::OperatorForceVoid,
        )
        .unwrap();
        assert!(result.is_some(), "void from Pending must emit");
    }

    // ---------------------------------------------------------------------
    // Temporal-view reads: `LogicalPointers::concrete_at` parameterised
    // over the pointer DAO `P`. Covers both LFR and LTDV through that
    // single method.
    // ---------------------------------------------------------------------

    mod concrete_at {
        use crate::test_utils::SeedEntry;
        use crate::{LiveEntry, LogicalPointersEntry};
        use td_database::pool::TdPool;
        use td_objects::Buildable;
        use td_objects::dxo::function::FunctionRegister;
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalPointerDao,
            LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
        };
        use td_objects::types::basic::{
            AtTime, BundleId, CollectionName, Decorator, LogicalPointerReason,
            LogicalPointerStatus, PhysicalFunctionRunId, PhysicalTableDataVersionId, TableId,
            TableNameDto, TransactionKey, Trigger, UserId,
        };

        /// `function_run_at` walks the pointer event log:
        /// - before the mint, returns None
        /// - at the InitialMint event, the LFR is still Pending so
        ///   `physical_function_run_id` is None
        /// - at or after the bind event, returns the bound concrete FR id
        #[td_test::test(sqlx)]
        #[tokio::test]
        async fn test_function_run_at_walks_pointers(db: TdPool) {
            let collection = db
                .seed()
                .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
                .await
                .unwrap();
            let create = FunctionRegister::builder()
                .try_name("f0")
                .unwrap()
                .try_description("d")
                .unwrap()
                .bundle_id(BundleId::default())
                .try_snippet("s")
                .unwrap()
                .decorator(Decorator::Publisher)
                .dependencies(None)
                .triggers(None)
                .tables(vec![TableNameDto::try_from("t0").unwrap()])
                .try_runtime_values("rt")
                .unwrap()
                .reuse_frozen_tables(false)
                .build()
                .unwrap();
            let function = db.seed().function(&collection, &create).await.unwrap();
            let execution = db.seed().execution(&function).await.unwrap();
            let transaction = db
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let before_mint = AtTime::now();
            let mint_at = AtTime::now();
            let lfr = LogicalFunctionRunDB::builder()
                .collection_id(collection.id)
                .execution_id(execution.id.clone())
                .transaction_id(transaction.id.clone())
                .function_id(function.function_id)
                .function_version_id(function.id.clone())
                .trigger(Trigger::Manual)
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .build()
                .unwrap();

            let tx = db.begin().await.unwrap();
            let mut conn = tx.into_connection();
            (&mut conn)
                .live()
                .insert::<LogicalFunctionRunDB>(&lfr)
                .await
                .unwrap();
            let mint = LogicalFunctionRunPointerDB::initial_mint_for(&lfr).unwrap();
            (&mut conn)
                .live()
                .insert::<LogicalFunctionRunPointerDB>(&mint)
                .await
                .unwrap();

            let bind_at = AtTime::now();
            let fr_id = PhysicalFunctionRunId::new();
            let bind_event = LogicalFunctionRunPointerDB::build_event(
                &lfr.id,
                LogicalPointerStatus::Bound,
                Some(fr_id.clone()),
                bind_at,
                UserId::admin(),
                LogicalPointerReason::InitialDispatch,
            )
            .unwrap();
            (&mut conn)
                .live()
                .insert::<LogicalFunctionRunPointerDB>(&bind_event)
                .await
                .unwrap();

            let pre_mint = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalFunctionRunPointerDB>(&lfr.id, &before_mint)
                .await
                .unwrap();
            assert!(pre_mint.is_none());

            let at_mint = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalFunctionRunPointerDB>(&lfr.id, &mint_at)
                .await
                .unwrap();
            assert!(at_mint.is_none());

            let after_bind = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalFunctionRunPointerDB>(&lfr.id, &bind_at)
                .await
                .unwrap();
            assert_eq!(after_bind, Some(fr_id.clone()));

            let later = AtTime::now();
            let after_later = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalFunctionRunPointerDB>(&lfr.id, &later)
                .await
                .unwrap();
            assert_eq!(after_later, Some(fr_id));
        }

        /// `table_data_version_at` mirror of `function_run_at`: pre-mint None,
        /// at-mint None (Pending), post-bind Some(tdv_id).
        #[td_test::test(sqlx)]
        #[tokio::test]
        async fn test_table_data_version_at_walks_pointers(db: TdPool) {
            let collection = db
                .seed()
                .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
                .await
                .unwrap();
            let create = FunctionRegister::builder()
                .try_name("f0")
                .unwrap()
                .try_description("d")
                .unwrap()
                .bundle_id(BundleId::default())
                .try_snippet("s")
                .unwrap()
                .decorator(Decorator::Publisher)
                .dependencies(None)
                .triggers(None)
                .tables(vec![TableNameDto::try_from("t0").unwrap()])
                .try_runtime_values("rt")
                .unwrap()
                .reuse_frozen_tables(false)
                .build()
                .unwrap();
            let function = db.seed().function(&collection, &create).await.unwrap();
            let execution = db.seed().execution(&function).await.unwrap();
            let transaction = db
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let before_mint = AtTime::now();
            let mint_at = AtTime::now();
            let table_id = TableId::default();
            let ltdv = LogicalTableDataVersionDB::builder()
                .collection_id(collection.id)
                .execution_id(execution.id.clone())
                .transaction_id(transaction.id.clone())
                .table_id(table_id)
                .producer_logical_function_run_id(None)
                .function_param_pos(None)
                .triggered_on(execution.triggered_on)
                .triggered_by_id(execution.triggered_by_id)
                .build()
                .unwrap();

            let tx = db.begin().await.unwrap();
            let mut conn = tx.into_connection();
            (&mut conn)
                .live()
                .insert::<LogicalTableDataVersionDB>(&ltdv)
                .await
                .unwrap();
            let mint = LogicalTableDataVersionPointerDB::initial_mint_for(&ltdv).unwrap();
            (&mut conn)
                .live()
                .insert::<LogicalTableDataVersionPointerDB>(&mint)
                .await
                .unwrap();

            let bind_at = AtTime::now();
            let tdv_id = PhysicalTableDataVersionId::new();
            let bind_event = LogicalTableDataVersionPointerDB::build_event(
                &ltdv.id,
                LogicalPointerStatus::Bound,
                Some(tdv_id.clone()),
                bind_at,
                UserId::admin(),
                LogicalPointerReason::InitialDispatch,
            )
            .unwrap();
            (&mut conn)
                .live()
                .insert::<LogicalTableDataVersionPointerDB>(&bind_event)
                .await
                .unwrap();

            let pre_mint = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalTableDataVersionPointerDB>(&ltdv.id, &before_mint)
                .await
                .unwrap();
            assert!(pre_mint.is_none());

            let at_mint = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalTableDataVersionPointerDB>(&ltdv.id, &mint_at)
                .await
                .unwrap();
            assert!(at_mint.is_none());

            let after_bind = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalTableDataVersionPointerDB>(&ltdv.id, &bind_at)
                .await
                .unwrap();
            assert_eq!(after_bind, Some(tdv_id.clone()));

            let later = AtTime::now();
            let after_later = (&mut conn)
                .logical_pointers()
                .concrete_at::<LogicalTableDataVersionPointerDB>(&ltdv.id, &later)
                .await
                .unwrap();
            assert_eq!(after_later, Some(tdv_id));
        }
    }
}
