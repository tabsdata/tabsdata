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
//! Sync builders [`redirect_event`] / [`void_event`] take the latest
//! pointer event the caller already has and return `Option<P>` — `None`
//! when idempotent. The caller inserts. Async reads go through the
//! entity handles' `.pointers()` constructor; the at-time variant adds
//! an `.at(&at)` step (mirrors [`crate::VersionedEntry`]).

use crate::{LiveEntry, QueryExecutor, VersionedEntry};
use sea_query::{Expr, ExprTrait, Iden, Order, Query};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use ta_objects::errors::SqlError;
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_error::{ApiError, TdError, api_error};
use td_objects::SeaValue;
use td_objects::dxo::logical::LogicalPointerDao;
use td_objects::types::basic::{
    AtTime, LogicalPointerReason, LogicalPointerStatus, ProjectId, UserId,
};

/// Pointer-event log handle, type-pinned to a specific
/// [`LogicalPointerDao`] `P`. Reached from the entity handle via
/// `.pointers()`:
///
/// ```ignore
/// conn.lfr().pointers().concrete(&lfr_id).await?;
/// conn.ltdv().pointers().at(&at).latest_chunked(&ltdv_ids).await?;
/// ```
///
/// At-time reads live behind [`at`](Self::at) (returns [`PointersAt`]).
pub struct LogicalPointers<E, P: LogicalPointerDao> {
    executor: E,
    _phantom: PhantomData<P>,
}

impl<E, P: LogicalPointerDao> LogicalPointers<E, P> {
    pub(crate) fn new(executor: E) -> Self {
        Self {
            executor,
            _phantom: PhantomData,
        }
    }

    /// Pin a point in time. Returns a [`PointersAt`] for at-time event
    /// reads — sibling of [`crate::VersionedEntry::versioned`] in shape.
    pub fn at(self, at: &AtTime) -> PointersAt<'_, E, P> {
        PointersAt {
            at,
            executor: self.executor,
            _phantom: PhantomData,
        }
    }
}

impl<E, P> LogicalPointers<E, P>
where
    E: QueryExecutor + Domained<Domain = P::Db>,
    P: LogicalPointerDao,
{
    /// Concrete id this logical pointer is *currently* bound to.
    /// `Ok(None)` when the latest event is `Pending` or `Voided` —
    /// i.e. when there is no live binding *right now*, even if a
    /// prior `Bound` event exists in history.
    pub async fn concrete(
        self,
        logical_id: &P::LogicalId,
    ) -> Result<Option<P::ConcreteId>, SqlError> {
        let row: Option<P> = Query::select()
            .columns(P::columns())
            .from(P::table())
            .and_where(Expr::col(P::partition_col()).eq(logical_id.as_sea_value()))
            .order_by(P::version_order_col(), Order::Desc)
            .order_by(P::pk_col(), Order::Desc)
            .limit(1)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<P>)?;
        Ok(bound_concrete(row))
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
    pub async fn bind_with_known_state(
        self,
        project_id: &ProjectId,
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
            *project_id,
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
}

/// At-time pointer-event surface. Reached via
/// [`LogicalPointers::at`]; mirrors the [`crate::Versioned`] shape
/// (builder step pins `at`, terminal methods perform the read).
pub struct PointersAt<'a, E, P: LogicalPointerDao> {
    pub(crate) at: &'a AtTime,
    pub(crate) executor: E,
    pub(crate) _phantom: PhantomData<P>,
}

impl<'a, E, P> PointersAt<'a, E, P>
where
    E: QueryExecutor + Domained<Domain = P::Db>,
    P: LogicalPointerDao,
{
    /// Concrete id this logical pointer was *bound to* on or before
    /// the wrapper's `at`. `Ok(None)` when pre-mint, Pending, or Voided.
    pub async fn concrete(
        self,
        logical_id: &P::LogicalId,
    ) -> Result<Option<P::ConcreteId>, SqlError> {
        let row: Option<P> = self
            .executor
            .versioned(self.at)
            .fetch_optional(logical_id)
            .await?;
        Ok(bound_concrete(row))
    }

    /// Latest pointer event row per partition at-or-before `at`,
    /// batched. Each `logical_id` in `ids` maps to its latest event
    /// row at `at`; partitions with no events at all are absent from
    /// the map.
    pub async fn latest_chunked(
        self,
        ids: &[P::LogicalId],
    ) -> Result<HashMap<P::LogicalId, P>, SqlError>
    where
        P::LogicalId: Hash + Eq,
    {
        let rows: Vec<P> = self
            .executor
            .versioned(self.at)
            .fetch_all_chunked::<P>(ids)
            .await?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            out.insert(row.logical_id().clone(), row);
        }
        Ok(out)
    }
}

/// Joined-with-AS alias for the producer LFR (canonical status source
/// for an LTDV). LEFT JOIN — NULL when the LTDV is a lazy-mint over a
/// pre-2.0 concrete TDV (no logical-layer producer row exists).
#[derive(Iden, Clone, Copy)]
pub(crate) struct ProducerLfr;

/// Reduce the latest pointer-event row to its bound concrete id, or
/// `None` if the event is `Pending` / `Voided`. Pending and Voided rows
/// carry a NULL concrete by construction, so the status guard is
/// belt-and-braces today, but it pins the semantic so a future state
/// with a non-NULL concrete on a non-Bound event can't silently mislead
/// callers.
fn bound_concrete<P: LogicalPointerDao>(row: Option<P>) -> Option<P::ConcreteId> {
    row.and_then(|p| {
        if matches!(p.status(), LogicalPointerStatus::Bound) {
            p.concrete_id().cloned()
        } else {
            None
        }
    })
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
    project_id: &ProjectId,
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
        *project_id,
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
/// **Currently unused in production** — kept (with tests) as the
/// state-machine counterpart to [`redirect_event`]. The void path is
/// covered by the pointer-event design but no live caller exercises it
/// yet; the operator force-void and replay-void flows will reach for
/// this helper when they land.
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
    project_id: &ProjectId,
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
        *project_id,
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
    use td_objects::types::basic::{
        LogicalFunctionRunId, PhysicalFunctionRunId, ProjectId, UserId,
    };

    fn build_pointer(
        logical_id: LogicalFunctionRunId,
        status: LogicalPointerStatus,
        concrete: Option<PhysicalFunctionRunId>,
    ) -> LogicalFunctionRunPointerDB {
        LogicalFunctionRunPointerDB::builder()
            .project_id(ProjectId::default())
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
            &ProjectId::default(),
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
    // Temporal-view reads: `LogicalPointers::at(..).concrete(..)`
    // parameterised over the pointer DAO `P`. Covers both LFR and
    // LTDV through that single method.
    // ---------------------------------------------------------------------

    mod at_concrete {
        use crate::test_utils::SeedEntry;
        use crate::{LfrEntry, LiveEntry, LtdvEntry};
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
        #[td_test::test(db)]
        async fn test_function_run_at_walks_pointers(db: TdPool) {
            let p = db.seed().test_project().await.unwrap();
            let collection = db
                .scoped(p.id)
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
            let function = db
                .scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await
                .unwrap();
            let execution = db.scoped(p.id).seed().execution(&function).await.unwrap();
            let transaction = db
                .scoped(p.id)
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let before_mint = AtTime::now();
            let lfr = LogicalFunctionRunDB::builder()
                .project_id(collection.project_id)
                .group_id(collection.group_id)
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

            let mut db = db.begin().await.unwrap().into_connection();

            db.live()
                .insert::<LogicalFunctionRunDB>(&lfr)
                .await
                .unwrap();
            let mint = LogicalFunctionRunPointerDB::initial_mint_for(&lfr).unwrap();
            db.live()
                .insert::<LogicalFunctionRunPointerDB>(&mint)
                .await
                .unwrap();

            let bind_at = AtTime::now();
            let fr_id = PhysicalFunctionRunId::new();
            let bind_event = LogicalFunctionRunPointerDB::build_event(
                lfr.project_id,
                &lfr.id,
                LogicalPointerStatus::Bound,
                Some(fr_id.clone()),
                bind_at,
                UserId::admin(),
                LogicalPointerReason::InitialDispatch,
            )
            .unwrap();
            db.live()
                .insert::<LogicalFunctionRunPointerDB>(&bind_event)
                .await
                .unwrap();

            let pre_mint = db
                .lfr()
                .pointers()
                .at(&before_mint)
                .concrete(&lfr.id)
                .await
                .unwrap();
            assert!(pre_mint.is_none());

            let after_bind = db
                .lfr()
                .pointers()
                .at(&bind_at)
                .concrete(&lfr.id)
                .await
                .unwrap();
            assert_eq!(after_bind, Some(fr_id.clone()));

            let later = AtTime::now();
            let after_later = db
                .lfr()
                .pointers()
                .at(&later)
                .concrete(&lfr.id)
                .await
                .unwrap();
            assert_eq!(after_later, Some(fr_id));
        }

        /// `table_data_version_at` mirror of `function_run_at`: pre-mint None,
        /// at-mint None (Pending), post-bind Some(tdv_id).
        #[td_test::test(db)]
        async fn test_table_data_version_at_walks_pointers(db: TdPool) {
            let p = db.seed().test_project().await.unwrap();
            let collection = db
                .scoped(p.id)
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
            let function = db
                .scoped(p.id)
                .seed()
                .function(&collection, &create)
                .await
                .unwrap();
            let execution = db.scoped(p.id).seed().execution(&function).await.unwrap();
            let transaction = db
                .scoped(p.id)
                .seed()
                .transaction(&execution, &TransactionKey::try_from("ANY").unwrap())
                .await
                .unwrap();

            let before_mint = AtTime::now();
            let table_id = TableId::default();
            let ltdv = LogicalTableDataVersionDB::builder()
                .project_id(collection.project_id)
                .group_id(collection.group_id)
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

            let mut conn = db.begin().await.unwrap().into_connection();

            conn.live()
                .insert::<LogicalTableDataVersionDB>(&ltdv)
                .await
                .unwrap();
            let mint = LogicalTableDataVersionPointerDB::initial_mint_for(&ltdv).unwrap();
            conn.live()
                .insert::<LogicalTableDataVersionPointerDB>(&mint)
                .await
                .unwrap();

            let bind_at = AtTime::now();
            let tdv_id = PhysicalTableDataVersionId::new();
            let bind_event = LogicalTableDataVersionPointerDB::build_event(
                ltdv.project_id,
                &ltdv.id,
                LogicalPointerStatus::Bound,
                Some(tdv_id.clone()),
                bind_at,
                UserId::admin(),
                LogicalPointerReason::InitialDispatch,
            )
            .unwrap();
            conn.live()
                .insert::<LogicalTableDataVersionPointerDB>(&bind_event)
                .await
                .unwrap();

            let pre_mint = conn
                .ltdv()
                .pointers()
                .at(&before_mint)
                .concrete(&ltdv.id)
                .await
                .unwrap();
            assert!(pre_mint.is_none());

            let after_bind = conn
                .ltdv()
                .pointers()
                .at(&bind_at)
                .concrete(&ltdv.id)
                .await
                .unwrap();
            assert_eq!(after_bind, Some(tdv_id.clone()));

            let later = AtTime::now();
            let after_later = conn
                .ltdv()
                .pointers()
                .at(&later)
                .concrete(&ltdv.id)
                .await
                .unwrap();
            assert_eq!(after_later, Some(tdv_id));
        }
    }
}
