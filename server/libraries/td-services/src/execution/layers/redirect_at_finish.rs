//
// Copyright 2026 Tabs Data Inc.
//

//! On `Done` from a replay PFR, append `Bound(PFR_new, ReplayRedirect)`
//! to the LFR pointer log and `Bound(TDV_new, ReplayRedirect)` to each
//! output LTDV pointer log so downstream readers see the new outputs.
//! Initial dispatches and non-`Done` outcomes are no-ops here.

use crate::logical::layers::index_lfrs_by_exec_fn;
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Deref;
use td_common::execution_status::WorkerCallbackStatus;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunPointerDB, LogicalTableDataVersionDB,
    LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::worker::UpdateWorkerExecution;
use td_objects::types::basic::{
    AtTime, ExecutionId, LogicalFunctionRunId, LogicalPointerReason, LogicalPointerStatus,
    LogicalTableDataVersionId, PhysicalFunctionRunId, TriggeredOn,
};
use td_queries::{ChunkedQueries, LfrEntry, LiveEntry, LtdvEntry, redirect_event};
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, instrument};

#[instrument(
    name = "callback.redirect_at_finish",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn redirect_pointers_at_finish(
    mut conn: MutRef<DynamicConnection<'static>>,
    physical_function_runs: Ref<Vec<PhysicalFunctionRunDB>>,
    update: Ref<UpdateWorkerExecution>,
    at: Ref<AtTime>,
) -> Result<(), TdError> {
    if !matches!(update.status, WorkerCallbackStatus::Done) {
        return Ok(());
    }
    if physical_function_runs.is_empty() {
        return Ok(());
    }

    // Resolve all LFRs via (execution_id, function_id) in one chunked
    // fetch over distinct executions.
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.execution_id.clone())
        .unique()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
        .await?;
    let lfr_by_fr_key = index_lfrs_by_exec_fn(all_lfrs);

    // Distinct LFR ids touched by this batch — drives the latest-pointer
    // fetch (which doubles as our early-out check).
    let lfr_ids: Vec<LogicalFunctionRunId> = physical_function_runs
        .iter()
        .filter_map(|f| {
            lfr_by_fr_key
                .get(&f.execution_id)
                .and_then(|inner| inner.get(&f.function_id))
                .map(|l| l.id.clone())
        })
        .unique()
        .collect();

    let latest_lfr_by_id: HashMap<LogicalFunctionRunId, LogicalFunctionRunPointerDB> = conn
        .lfr()
        .pointers()
        .latest_pointers_chunked(&lfr_ids, at.deref())
        .await?;

    // Early out: if no reporting PFR is a replay attempt (i.e. its
    // LFR's latest Bound pointer names a different PFR *and* the
    // reporting PFR is newer than the current binding), there's
    // nothing to flip and we can skip the TDV / LTDV pre-fetches.
    let any_replay_attempt = physical_function_runs.iter().any(|fr| {
        let lfr_id = lfr_by_fr_key
            .get(&fr.execution_id)
            .and_then(|inner| inner.get(&fr.function_id))
            .map(|l| &l.id);
        lfr_id
            .and_then(|id| latest_lfr_by_id.get(id))
            .is_some_and(|p| is_replay_attempt(p, &fr.id, &fr.triggered_on))
    });
    if !any_replay_attempt {
        return Ok(());
    }

    // Output TDVs across all reporting FRs.
    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|f| f.id.clone())
        .collect();
    let all_tdvs: Vec<PhysicalTableDataVersionDB> = conn
        .fetch_all_chunked::<PhysicalTableDataVersionDB, PhysicalFunctionRunId>(&fr_ids)
        .await?;
    let tdvs_by_fr: HashMap<PhysicalFunctionRunId, Vec<PhysicalTableDataVersionDB>> = all_tdvs
        .into_iter()
        .into_group_map_by(|t| t.physical_function_run_id.clone());

    // LTDVs produced by any LFR in this batch, grouped by producer LFR.
    let producer_filters: Vec<Option<LogicalFunctionRunId>> =
        lfr_ids.iter().map(|id| Some(id.clone())).collect();
    let all_ltdvs: Vec<LogicalTableDataVersionDB> = if producer_filters.is_empty() {
        Vec::new()
    } else {
        conn.fetch_all_chunked::<LogicalTableDataVersionDB, Option<LogicalFunctionRunId>>(
            &producer_filters,
        )
        .await?
    };
    let ltdvs_by_producer: HashMap<LogicalFunctionRunId, Vec<LogicalTableDataVersionDB>> =
        all_ltdvs
            .into_iter()
            .filter_map(|f| f.producer_logical_function_run_id.clone().map(|p| (p, f)))
            .into_group_map();

    // Latest LTDV pointers across every LTDV we might touch.
    let ltdv_ids: Vec<LogicalTableDataVersionId> = ltdvs_by_producer
        .values()
        .flatten()
        .map(|f| f.id.clone())
        .collect();
    let latest_ltdv_by_id: HashMap<LogicalTableDataVersionId, LogicalTableDataVersionPointerDB> =
        conn.ltdv()
            .pointers()
            .latest_pointers_chunked(&ltdv_ids, at.deref())
            .await?;

    for fr in physical_function_runs.iter() {
        let Some(lfr) = lfr_by_fr_key
            .get(&fr.execution_id)
            .and_then(|inner| inner.get(&fr.function_id))
        else {
            continue;
        };
        let lfr_id = &lfr.id;
        let Some(latest_lfr) = latest_lfr_by_id.get(lfr_id) else {
            continue;
        };
        if !is_replay_attempt(latest_lfr, &fr.id, &fr.triggered_on) {
            continue;
        }

        if let Some(event) = redirect_event::<LogicalFunctionRunPointerDB>(
            latest_lfr,
            lfr_id,
            &fr.id,
            at.deref(),
            &fr.triggered_by_id,
            LogicalPointerReason::ReplayRedirect,
        )? {
            conn.live()
                .insert::<LogicalFunctionRunPointerDB>(&event)
                .await?;
            debug!(
                lfr_id = %lfr_id,
                fr_id = %fr.id,
                "redirect_at_finish: LFR pointer Bound → Bound(FR_new) on replay-attempt success"
            );
        }

        let Some(tdvs) = tdvs_by_fr.get(&fr.id) else {
            continue;
        };
        let empty = Vec::new();
        let ltdvs = ltdvs_by_producer.get(lfr_id).unwrap_or(&empty);
        for tdv in tdvs {
            let Some(ltdv) = ltdvs.iter().find(|l| l.table_id == tdv.table_id) else {
                continue;
            };
            let Some(latest_ltdv) = latest_ltdv_by_id.get(&ltdv.id) else {
                continue;
            };
            if let Some(event) = redirect_event::<LogicalTableDataVersionPointerDB>(
                latest_ltdv,
                &ltdv.id,
                &tdv.id,
                at.deref(),
                &fr.triggered_by_id,
                LogicalPointerReason::ReplayRedirect,
            )? {
                conn.live()
                    .insert::<LogicalTableDataVersionPointerDB>(&event)
                    .await?;
                debug!(
                    ltdv_id = %ltdv.id,
                    tdv_id = %tdv.id,
                    "redirect_at_finish: LTDV pointer Bound → Bound(TDV_new) on replay-attempt success"
                );
            }
        }
    }
    Ok(())
}

/// The reporting PFR is a *replay attempt* iff:
/// 1. the LFR's latest pointer is `Bound` to a *different* PFR, and
/// 2. the reporting PFR was triggered *after* that pointer was written.
///
/// Condition (2) is the monotonicity guard: if an older PFR finishes
/// late (after a newer replay has already bound), its callback must not
/// regress the binding back to the older PFR. The `triggered_on >
/// set_on` test catches that — the original PFR's `triggered_on`
/// predates the replay binding's `set_on`, so we early-out.
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
    // Monotonicity: reporting PFR must postdate the current binding.
    AtTime::try_from(*reporting_triggered_on)
        .map(|t| t > pointer.set_on)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::datetime::Timestamp;
    use td_objects::Buildable;
    use td_objects::dxo::logical::{LogicalFunctionRunPointerDB, LogicalPointerDao};
    use td_objects::types::basic::{
        AtTime, CollectionId, ExecutionId, FunctionId, FunctionVersionId, LogicalFunctionRunId,
        LogicalPointerReason, LogicalPointerStatus, PhysicalFunctionRunId,
        PhysicalFunctionRunStatus, TransactionId, Trigger, TriggeredOn, UserId,
    };

    fn at(ms: i64) -> AtTime {
        AtTime::try_from(Timestamp::try_from(ms).unwrap()).unwrap()
    }

    fn mk_pointer_at(
        status: LogicalPointerStatus,
        pfr: Option<PhysicalFunctionRunId>,
        reason: LogicalPointerReason,
        set_on: AtTime,
    ) -> LogicalFunctionRunPointerDB {
        LogicalFunctionRunPointerDB::build_event(
            &LogicalFunctionRunId::new(),
            status,
            pfr,
            set_on,
            UserId::admin(),
            reason,
        )
        .unwrap()
    }

    fn mk_pfr_at(id: PhysicalFunctionRunId, triggered_on: AtTime) -> PhysicalFunctionRunDB {
        PhysicalFunctionRunDB::builder()
            .id(id)
            .collection_id(CollectionId::default())
            .function_id(FunctionId::default())
            .function_version_id(FunctionVersionId::new())
            .execution_id(ExecutionId::new())
            .transaction_id(TransactionId::new())
            .triggered_on(TriggeredOn::try_from(*triggered_on).unwrap())
            .triggered_by_id(UserId::admin())
            .trigger(Trigger::Manual)
            .status(PhysicalFunctionRunStatus::Done)
            .build()
            .unwrap()
    }

    #[test]
    fn test_initial_dispatch_is_not_replay() {
        let pfr_id = PhysicalFunctionRunId::new();
        let p = mk_pointer_at(
            LogicalPointerStatus::Bound,
            Some(pfr_id.clone()),
            LogicalPointerReason::InitialDispatch,
            at(1_000),
        );
        let pfr = mk_pfr_at(pfr_id, at(2_000));
        assert!(!is_replay_attempt(&p, &pfr.id, &pfr.triggered_on));
    }

    #[test]
    fn test_replay_attempt_when_bound_differs_and_newer() {
        let bound_pfr_id = PhysicalFunctionRunId::new();
        let reporting_pfr_id = PhysicalFunctionRunId::new();
        let p = mk_pointer_at(
            LogicalPointerStatus::Bound,
            Some(bound_pfr_id),
            LogicalPointerReason::ReplayRedirect,
            at(1_000),
        );
        let pfr = mk_pfr_at(reporting_pfr_id, at(2_000));
        assert!(is_replay_attempt(&p, &pfr.id, &pfr.triggered_on));
    }

    /// Monotonicity guard: an older PFR finishing late after a newer
    /// replay has already bound the LFR must NOT trigger redirect.
    #[test]
    fn test_older_pfr_finishing_late_is_not_replay() {
        let bound_pfr_id = PhysicalFunctionRunId::new(); // the replay
        let stale_pfr_id = PhysicalFunctionRunId::new(); // the original, slow
        let p = mk_pointer_at(
            LogicalPointerStatus::Bound,
            Some(bound_pfr_id),
            LogicalPointerReason::ReplayRedirect,
            at(2_000), // replay bound here
        );
        let pfr = mk_pfr_at(stale_pfr_id, at(1_000)); // original predates the binding
        assert!(!is_replay_attempt(&p, &pfr.id, &pfr.triggered_on));
    }

    #[test]
    fn test_pending_pointer_is_not_replay() {
        let pfr = mk_pfr_at(PhysicalFunctionRunId::new(), at(2_000));
        let p = mk_pointer_at(
            LogicalPointerStatus::Pending,
            None,
            LogicalPointerReason::InitialMint,
            at(1_000),
        );
        assert!(!is_replay_attempt(&p, &pfr.id, &pfr.triggered_on));
    }

    #[test]
    fn test_voided_pointer_is_not_replay() {
        let pfr = mk_pfr_at(PhysicalFunctionRunId::new(), at(2_000));
        let p = mk_pointer_at(
            LogicalPointerStatus::Voided,
            None,
            LogicalPointerReason::OperatorForceVoid,
            at(1_000),
        );
        assert!(!is_replay_attempt(&p, &pfr.id, &pfr.triggered_on));
    }
}
