//
// Copyright 2026 Tabsdata Inc.
//

//! Pointer-event state-machine operations for LFR / LTDV.
//!
//! Each operation appends a single event to the relevant pointer-event
//! table. The "current" pointer for a logical identity is the latest
//! event row for that logical id, resolved through the trigger-maintained
//! `_current` peer table rather than by ranking the event log. The caller
//! must already hold a transactional connection.
//!
//! LFR and LTDV share a common state machine. The schema shape lives on
//! [`LogicalPointerDao`] (in `td_objects` — emitted by the `Dao` macro's
//! `logical_pointer(...)` arg). The batched writers own their reads: callers
//! pass intents, not pre-fetched rows.

use crate::primitives::chunked::{
    MAX_INSERT_ROWS, MAX_QUERY_CONDITIONS, fetch_all_current_chunk_query, insert_vec_query,
    warn_if_chunked,
};
use crate::{CurrentEntry, QueryExecutor, VersionedEntry};
use sea_query::Iden;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use ta_objects::DaoCore;
use ta_objects::errors::SqlError;
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::logical::{
    LogicalFunctionRunPointerDB, LogicalPointerDao, LogicalTableDataVersionPointerDB,
};
use td_objects::types::basic::{
    AtTime, LogicalFunctionRunId, LogicalPointerReason, LogicalPointerStatus,
    LogicalTableDataVersionId, PhysicalFunctionRunId, PhysicalTableDataVersionId, ProjectId,
    TriggeredOn, UserId,
};

/// Pointer-event log handle, type-pinned to a specific
/// [`LogicalPointerDao`] `P`. Reached from the entity handle via
/// `.pointers()`:
///
/// ```ignore
/// conn.lfr().pointers().concrete(&lfr_id).await?;
/// conn.ltdv().pointers().at(&at).concrete(&ltdv_id).await?;
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
    /// prior `Bound` event exists in history. Resolved from the
    /// trigger-maintained `_current` peer (one indexed row per logical id).
    pub async fn concrete(
        self,
        logical_id: &P::LogicalId,
    ) -> Result<Option<P::ConcreteId>, SqlError> {
        let row: Option<P> = self
            .executor
            .current()
            .fetch_optional::<P>(logical_id)
            .await?;
        Ok(bound_concrete(row))
    }

    /// Bind every `Pending` pointer; returns already-`Bound` ids (a re-run,
    /// deferred to redirect-at-finish). `Voided` is an error.
    pub async fn bind_all(
        mut self,
        binds: &[Bind<P::LogicalId, P::ConcreteId>],
        set_on: &AtTime,
        set_by_id: &UserId,
    ) -> Result<HashSet<P::LogicalId>, TdError>
    where
        P::LogicalId: Hash + Eq,
    {
        if binds.is_empty() {
            return Ok(HashSet::new());
        }
        let ids: Vec<P::LogicalId> = binds.iter().map(|b| b.logical_id.clone()).collect();
        let latest = current_by_id::<E, P>(&mut self.executor, &ids).await?;

        let mut deferred = HashSet::new();
        let mut events = Vec::with_capacity(binds.len());
        for bind in binds {
            let current = latest.get(&bind.logical_id).ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "no {} pointer event for {} at dispatch (identity should carry an InitialMint)",
                    P::KIND,
                    bind.logical_id,
                )
            })?;
            match current.status() {
                LogicalPointerStatus::Bound => {
                    deferred.insert(bind.logical_id.clone());
                }
                LogicalPointerStatus::Voided => {
                    return Err(api_error!(
                        ApiError::InternalError,
                        "{} {} is Voided; cannot bind",
                        P::KIND,
                        bind.logical_id,
                    ));
                }
                LogicalPointerStatus::Pending => events.push(P::build_event(
                    bind.project_id,
                    &bind.logical_id,
                    LogicalPointerStatus::Bound,
                    Some(bind.new_concrete_id.clone()),
                    *set_on,
                    *set_by_id,
                    LogicalPointerReason::InitialDispatch,
                )?),
            }
        }
        insert_events::<E, P>(&mut self.executor, &events).await?;
        Ok(deferred)
    }
}

async fn current_by_id<E, P>(
    executor: &mut E,
    ids: &[P::LogicalId],
) -> Result<HashMap<P::LogicalId, P>, SqlError>
where
    E: QueryExecutor + Domained<Domain = P::Db>,
    P: LogicalPointerDao,
    P::LogicalId: Hash + Eq,
{
    warn_if_chunked::<P>(ids.len());
    let mut out = HashMap::new();
    for chunk in ids.chunks(MAX_QUERY_CONDITIONS) {
        let rows: Vec<P> = fetch_all_current_chunk_query::<P, P::LogicalId>(chunk, None)?
            .build_query_as()
            .fetch_all(executor.reuse())
            .await
            .map_err(SqlError::on_read::<P>)?;
        for r in rows {
            out.insert(r.logical_id().clone(), r);
        }
    }
    Ok(out)
}

async fn insert_events<E, P>(executor: &mut E, events: &[P]) -> Result<(), SqlError>
where
    E: QueryExecutor + Domained<Domain = P::Db>,
    P: LogicalPointerDao,
{
    for chunk in events.chunks(MAX_INSERT_ROWS) {
        insert_vec_query::<P>(chunk)?
            .build_query()
            .execute(executor.reuse())
            .await
            .map_err(|e| SqlError::on_write::<P>(e, &[]))?;
    }
    Ok(())
}

pub struct Bind<Id, Concrete> {
    pub project_id: ProjectId,
    pub logical_id: Id,
    pub new_concrete_id: Concrete,
}

pub struct ReplayRedirect {
    pub project_id: ProjectId,
    pub logical_function_run_id: LogicalFunctionRunId,
    pub new_physical_function_run_id: PhysicalFunctionRunId,
    pub reporting_triggered_on: TriggeredOn,
    pub set_by_id: UserId,
}

impl<E> LogicalPointers<E, LogicalFunctionRunPointerDB>
where
    E: QueryExecutor + Domained<Domain = <LogicalFunctionRunPointerDB as DaoCore>::Db>,
{
    /// Append `Bound(ReplayRedirect)` for each item that [`is_replay_attempt`].
    /// Returns the redirected LFR ids, which gate the caller's output-LTDV pass.
    pub async fn redirect_replay_attempts(
        mut self,
        items: &[ReplayRedirect],
        set_on: &AtTime,
    ) -> Result<HashSet<LogicalFunctionRunId>, TdError> {
        if items.is_empty() {
            return Ok(HashSet::new());
        }
        let ids: Vec<LogicalFunctionRunId> = items
            .iter()
            .map(|i| i.logical_function_run_id.clone())
            .collect();
        let latest =
            current_by_id::<E, LogicalFunctionRunPointerDB>(&mut self.executor, &ids).await?;

        let mut redirected = HashSet::new();
        let mut events = Vec::new();
        for item in items {
            let Some(current) = latest.get(&item.logical_function_run_id) else {
                continue;
            };
            if !is_replay_attempt(
                current,
                &item.new_physical_function_run_id,
                &item.reporting_triggered_on,
            ) {
                continue;
            }
            events.push(LogicalFunctionRunPointerDB::build_event(
                item.project_id,
                &item.logical_function_run_id,
                LogicalPointerStatus::Bound,
                Some(item.new_physical_function_run_id.clone()),
                *set_on,
                item.set_by_id,
                LogicalPointerReason::ReplayRedirect,
            )?);
            redirected.insert(item.logical_function_run_id.clone());
        }
        insert_events::<E, LogicalFunctionRunPointerDB>(&mut self.executor, &events).await?;
        Ok(redirected)
    }
}

pub struct OutputRedirect {
    pub project_id: ProjectId,
    pub logical_table_data_version_id: LogicalTableDataVersionId,
    pub new_physical_table_data_version_id: PhysicalTableDataVersionId,
    pub set_by_id: UserId,
}

impl<E> LogicalPointers<E, LogicalTableDataVersionPointerDB>
where
    E: QueryExecutor + Domained<Domain = <LogicalTableDataVersionPointerDB as DaoCore>::Db>,
{
    /// Redirect output LTDV pointers to freshly-produced TDVs. No monotonicity
    /// gate — the producer LFR's [`is_replay_attempt`] already established it.
    pub async fn redirect_outputs(
        mut self,
        items: &[OutputRedirect],
        set_on: &AtTime,
    ) -> Result<(), TdError> {
        if items.is_empty() {
            return Ok(());
        }
        let ids: Vec<LogicalTableDataVersionId> = items
            .iter()
            .map(|i| i.logical_table_data_version_id.clone())
            .collect();
        let latest =
            current_by_id::<E, LogicalTableDataVersionPointerDB>(&mut self.executor, &ids).await?;

        let mut events = Vec::new();
        for item in items {
            let Some(current) = latest.get(&item.logical_table_data_version_id) else {
                continue;
            };
            if let Some(event) = redirect_event::<LogicalTableDataVersionPointerDB>(
                &item.project_id,
                current,
                &item.logical_table_data_version_id,
                &item.new_physical_table_data_version_id,
                set_on,
                &item.set_by_id,
                LogicalPointerReason::ReplayRedirect,
            )? {
                events.push(event);
            }
        }
        insert_events::<E, LogicalTableDataVersionPointerDB>(&mut self.executor, &events).await?;
        Ok(())
    }
}

/// The reporting PFR is a *replay attempt* iff the LFR's current pointer is
/// `Bound` to a *different* PFR and the reporting PFR postdates that binding.
/// The monotonicity guard (`triggered_on > set_on`) stops an older PFR that
/// finishes late from regressing a newer replay's binding.
fn is_replay_attempt(
    pointer: &LogicalFunctionRunPointerDB,
    reporting_pfr_id: &PhysicalFunctionRunId,
    reporting_triggered_on: &TriggeredOn,
) -> bool {
    if !matches!(pointer.status, LogicalPointerStatus::Bound) {
        return false;
    }
    if pointer.physical_function_run_id.as_ref() == Some(reporting_pfr_id) {
        return false;
    }
    AtTime::try_from(*reporting_triggered_on)
        .map(|t| t > pointer.set_on)
        .unwrap_or(false)
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

/// Build a `Bound(new_concrete_id)` event, or `None` when already `Bound` to
/// the same id (idempotent).
fn redirect_event<P: LogicalPointerDao>(
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

/// Build a `Voided` event, or `None` when already `Voided` (idempotent). Kept
/// as the state-machine counterpart to [`redirect_event`]; no live caller yet.
#[allow(dead_code)]
fn void_event<P: LogicalPointerDao>(
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
    use td_objects::dxo::logical::LogicalFunctionRunPointerDB;
    use td_objects::types::basic::{
        LogicalFunctionRunId, LogicalFunctionRunPointerId, PhysicalFunctionRunId, ProjectId, UserId,
    };

    fn build_pointer(
        logical_id: LogicalFunctionRunId,
        status: LogicalPointerStatus,
        concrete: Option<PhysicalFunctionRunId>,
    ) -> LogicalFunctionRunPointerDB {
        LogicalFunctionRunPointerDB {
            id: LogicalFunctionRunPointerId::default(),
            project_id: ProjectId::default(),
            logical_function_run_id: logical_id,
            status,
            physical_function_run_id: concrete,
            set_on: AtTime::now(),
            set_by_id: UserId::admin(),
            reason: LogicalPointerReason::InitialDispatch,
        }
    }

    mod replay_attempt {
        use super::*;
        use td_common::datetime::Timestamp;

        fn at(ms: i64) -> AtTime {
            AtTime::try_from(Timestamp::try_from(ms).unwrap()).unwrap()
        }

        fn pointer(
            status: LogicalPointerStatus,
            pfr: Option<PhysicalFunctionRunId>,
            reason: LogicalPointerReason,
            set_on: AtTime,
        ) -> LogicalFunctionRunPointerDB {
            LogicalFunctionRunPointerDB::build_event(
                ProjectId::default(),
                &LogicalFunctionRunId::new(),
                status,
                pfr,
                set_on,
                UserId::admin(),
                reason,
            )
            .unwrap()
        }

        #[test]
        fn initial_dispatch_is_not_replay() {
            let pfr_id = PhysicalFunctionRunId::new();
            let p = pointer(
                LogicalPointerStatus::Bound,
                Some(pfr_id.clone()),
                LogicalPointerReason::InitialDispatch,
                at(1_000),
            );
            assert!(!is_replay_attempt(
                &p,
                &pfr_id,
                &TriggeredOn::try_from(*at(2_000)).unwrap()
            ));
        }

        #[test]
        fn bound_to_different_and_newer_is_replay() {
            let p = pointer(
                LogicalPointerStatus::Bound,
                Some(PhysicalFunctionRunId::new()),
                LogicalPointerReason::ReplayRedirect,
                at(1_000),
            );
            let reporting = PhysicalFunctionRunId::new();
            assert!(is_replay_attempt(
                &p,
                &reporting,
                &TriggeredOn::try_from(*at(2_000)).unwrap()
            ));
        }

        /// Monotonicity guard: an older PFR finishing late after a newer replay
        /// already bound the LFR must NOT trigger a redirect.
        #[test]
        fn older_pfr_finishing_late_is_not_replay() {
            let p = pointer(
                LogicalPointerStatus::Bound,
                Some(PhysicalFunctionRunId::new()),
                LogicalPointerReason::ReplayRedirect,
                at(2_000),
            );
            let stale = PhysicalFunctionRunId::new();
            assert!(!is_replay_attempt(
                &p,
                &stale,
                &TriggeredOn::try_from(*at(1_000)).unwrap()
            ));
        }

        #[test]
        fn pending_pointer_is_not_replay() {
            let p = pointer(
                LogicalPointerStatus::Pending,
                None,
                LogicalPointerReason::InitialMint,
                at(1_000),
            );
            let reporting = PhysicalFunctionRunId::new();
            assert!(!is_replay_attempt(
                &p,
                &reporting,
                &TriggeredOn::try_from(*at(2_000)).unwrap()
            ));
        }

        #[test]
        fn voided_pointer_is_not_replay() {
            let p = pointer(
                LogicalPointerStatus::Voided,
                None,
                LogicalPointerReason::OperatorForceVoid,
                at(1_000),
            );
            let reporting = PhysicalFunctionRunId::new();
            assert!(!is_replay_attempt(
                &p,
                &reporting,
                &TriggeredOn::try_from(*at(2_000)).unwrap()
            ));
        }
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
        use td_objects::dxo::function::FunctionRegister;
        use td_objects::dxo::logical::{
            LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalPointerDao,
            LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
        };
        use td_objects::types::basic::ReuseFrozen;
        use td_objects::types::basic::{
            AtTime, BundleId, CollectionName, Decorator, LogicalFunctionRunId,
            LogicalFunctionRunStatus, LogicalPointerReason, LogicalPointerStatus,
            LogicalTableDataVersionId, PhysicalFunctionRunId, PhysicalTableDataVersionId, Private,
            TableId, TableNameDto, TransactionKey, Trigger, UserId,
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
            let create = FunctionRegister {
                name: "f0".try_into().unwrap(),
                description: "d".try_into().unwrap(),
                bundle_id: BundleId::default(),
                snippet: "s".try_into().unwrap(),
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: None,
                triggers: None,
                tables: vec![TableNameDto::try_from("t0").unwrap()].into(),
                table_types: None,
                runtime_values: "rt".try_into().unwrap(),
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
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
            let lfr = LogicalFunctionRunDB {
                id: LogicalFunctionRunId::new(),
                project_id: collection.project_id,
                collection_id: collection.id,
                execution_id: execution.id.clone(),
                transaction_id: transaction.id.clone(),
                function_id: function.function_id,
                function_version_id: function.id.clone(),
                trigger: Trigger::Manual,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                status: LogicalFunctionRunStatus::Scheduled,
                group_id: collection.group_id,
            };

            let mut db = db.begin().await.unwrap();

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
            assert_eq!(after_later, Some(fr_id.clone()));

            let live = db.lfr().pointers().concrete(&lfr.id).await.unwrap();
            assert_eq!(
                live,
                Some(fr_id),
                "live concrete() must resolve via the maintained _current peer"
            );
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
            let create = FunctionRegister {
                name: "f0".try_into().unwrap(),
                description: "d".try_into().unwrap(),
                bundle_id: BundleId::default(),
                snippet: "s".try_into().unwrap(),
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: None,
                triggers: None,
                tables: vec![TableNameDto::try_from("t0").unwrap()].into(),
                table_types: None,
                runtime_values: "rt".try_into().unwrap(),
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
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
            let ltdv = LogicalTableDataVersionDB {
                id: LogicalTableDataVersionId::new(),
                project_id: collection.project_id,
                collection_id: collection.id,
                execution_id: execution.id.clone(),
                transaction_id: transaction.id.clone(),
                table_id,
                producer_logical_function_run_id: None,
                function_param_pos: None,
                private: Private::FALSE,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                group_id: collection.group_id,
            };

            let mut conn = db.begin().await.unwrap();

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
            assert_eq!(after_later, Some(tdv_id.clone()));

            let live = conn.ltdv().pointers().concrete(&ltdv.id).await.unwrap();
            assert_eq!(
                live,
                Some(tdv_id),
                "live concrete() must resolve via the maintained _current peer"
            );
        }
    }
}
