//
// Copyright 2026 Tabsdata Inc.
//

use crate::internal_api::InternalApi;
use crate::logical::index_lfrs_by_exec_fn;
use crate::scheduler::request::{put_cron_trigger_start, put_stage_trigger_start};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap, HashSet};
use td_bridge::BridgeProviderHandle;
use td_bridge::observer::{
    ObservedWorkload, WorkloadKind, WorkloadObservation, WorkloadObserver, WorkloadPhase,
};
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::RecoveryConfig;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithBundleBasePath};
use td_objects::dxo::logical::LogicalFunctionRunDB;
use td_objects::dxo::logical::LogicalFunctionRunInsertDB;
use td_objects::dxo::physical_function_run::{PhysicalFunctionRunDB, PhysicalFunctionRunUpdateDB};
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames, WorkerUpdateDB};
use td_objects::or;
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionVersionId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus, ProjectId, WorkerId, WorkerStatus,
};
use td_queries::Queries;
use td_storage::Storage;
use tracing::{debug, instrument, warn};

#[derive(Debug, Clone, Default)]
pub struct CronCandidates(pub Vec<WorkerDBWithNames>);

#[derive(Debug, Clone, Default)]
pub struct StageCandidates(pub Vec<WorkerDBWithNames>);

#[derive(Debug, Clone)]
pub struct FnAttempt {
    pub pfr: PhysicalFunctionRunDB,
    pub worker_id: Option<WorkerId>,
}

#[derive(Debug, Clone, Default)]
pub struct FnCandidates(pub Vec<FnAttempt>);

/// Function/bundle locations for the stage candidates, pre-fetched so
/// recreates run without a DB connection.
#[derive(Debug, Clone, Default)]
pub struct StageFunctions(pub HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>);

/// One read-only pass over the DB: everything classify and the recreates
/// need, so no connection is held during the orchestrator calls.
#[derive(Debug, Clone)]
pub struct RecoverySnapshot {
    pub at: AtTime,
    pub cron: Vec<WorkerDBWithNames>,
    pub stage: Vec<WorkerDBWithNames>,
    pub fn_attempts: Vec<FnAttempt>,
    pub stage_functions: HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>,
}

/// `recover`/`lfr` are provisional until [`finalize_recovery_actions`]
/// resolves them against run state re-read inside the write transaction.
#[derive(Debug, Clone)]
pub struct FnRecoveryAction {
    pub worker_id: Option<WorkerId>,
    pub pfr_id: PhysicalFunctionRunId,
    pub project_id: ProjectId,
    pub lfr: Option<LogicalFunctionRunDB>,
    pub recover: bool,
    pub remove: Option<(WorkloadKind, String)>,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryDecisions {
    pub fn_actions: Vec<FnRecoveryAction>,
    pub cron_recreates: Vec<WorkerDBWithNames>,
    pub stage_recreates: Vec<WorkerDBWithNames>,
    /// Deleted now, recreated next tick once observed absent.
    pub stage_restarts: Vec<(WorkerId, WorkloadKind, String)>,
    pub orphan_removals: Vec<(WorkloadKind, String)>,
}

/// Function actions whose workload removal succeeded; the rest retry next
/// tick.
#[derive(Debug, Clone, Default)]
pub struct RecoveryActions {
    pub fn_actions: Vec<FnRecoveryAction>,
}

impl RecoveryActions {
    pub fn by_project(self) -> Vec<(ProjectId, Self)> {
        let mut grouped: BTreeMap<ProjectId, Self> = BTreeMap::new();
        for action in self.fn_actions {
            grouped
                .entry(action.project_id)
                .or_default()
                .fn_actions
                .push(action);
        }
        grouped.into_iter().collect()
    }
}

#[instrument(name = "recovery.fn_candidates", skip_all, level = "debug", err(Debug))]
pub async fn fn_candidates<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
) -> Result<FnCandidates, TdError> {
    let mut pfrs: Vec<PhysicalFunctionRunDB> = conn
        .history()
        .fetch_all::<PhysicalFunctionRunDB>(or!(
            PhysicalFunctionRunStatus::Dispatched,
            PhysicalFunctionRunStatus::Running
        ))
        .await?;
    if pfrs.is_empty() {
        return Ok(FnCandidates::default());
    }
    // Oldest-dispatched first, so the per-tick cap never starves a run.
    pfrs.sort_by_key(|a| a.base.dispatched_on);

    // Condition columns resolve by exact field type: the nullable worker
    // column needs `Option`-wrapped keys.
    let pfr_ids: Vec<Option<PhysicalFunctionRunId>> =
        pfrs.iter().map(|pfr| Some(pfr.base.id.clone())).collect();
    let workers: Vec<WorkerDB> = conn.chunked().fetch_all::<WorkerDB>(&pfr_ids, ()).await?;
    let worker_by_pfr: HashMap<&PhysicalFunctionRunId, &WorkerDB> = workers
        .iter()
        .filter_map(|worker| {
            worker
                .base
                .physical_function_run_id
                .as_ref()
                .map(|pfr_id| (pfr_id, worker))
        })
        .collect();

    let attempts = pfrs
        .iter()
        .map(|pfr| FnAttempt {
            worker_id: worker_by_pfr
                .get(&pfr.base.id)
                .map(|worker| worker.base.id.clone()),
            pfr: pfr.clone(),
        })
        .collect();
    Ok(FnCandidates(attempts))
}

pub async fn stage_trigger_functions<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    stage: &StageCandidates,
) -> Result<StageFunctions, TdError> {
    if stage.0.is_empty() {
        return Ok(StageFunctions::default());
    }
    let fvids: Vec<FunctionVersionId> = stage
        .0
        .iter()
        .map(|worker| worker.base.base.function_version_id.clone())
        .collect();
    let functions = conn
        .logical_runs()
        .with_bundle_base_path(&fvids)
        .await?
        .into_iter()
        .map(|function| (function.base.base.id.clone(), function))
        .collect();
    Ok(StageFunctions(functions))
}

/// Version ids of the candidates' functions that are still current.
#[derive(Debug, Clone, Default)]
pub struct CurrentTriggerVersions(pub HashSet<FunctionVersionId>);

pub async fn current_trigger_versions<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    cron: &CronCandidates,
    stage: &StageCandidates,
) -> Result<CurrentTriggerVersions, TdError> {
    let fvids: Vec<FunctionVersionId> = cron
        .0
        .iter()
        .chain(stage.0.iter())
        .map(|worker| worker.base.base.function_version_id.clone())
        .unique()
        .collect();
    if fvids.is_empty() {
        return Ok(CurrentTriggerVersions::default());
    }
    let current: Vec<FunctionDB> = conn
        .latest()
        .chunked()
        .fetch_all::<FunctionDB>(&fvids, ())
        .await?;
    Ok(CurrentTriggerVersions(
        current
            .into_iter()
            .map(|function| function.base.id)
            .collect(),
    ))
}

pub fn build_recovery_snapshot(
    at: &AtTime,
    cron: &CronCandidates,
    stage: &StageCandidates,
    fn_attempts: &FnCandidates,
    stage_functions: &StageFunctions,
    current: &CurrentTriggerVersions,
) -> Result<RecoverySnapshot, TdError> {
    // Outdated-version trigger workers are not recovery's to maintain
    // (fn update/disable supersedes them; their leftover workloads fall to
    // the orphan pass).
    let is_current =
        |worker: &&WorkerDBWithNames| current.0.contains(&worker.base.base.function_version_id);
    Ok(RecoverySnapshot {
        at: *at,
        cron: cron.0.iter().filter(is_current).cloned().collect(),
        stage: stage.0.iter().filter(is_current).cloned().collect(),
        fn_attempts: fn_attempts.0.clone(),
        stage_functions: stage_functions.0.clone(),
    })
}

fn dt(at: &AtTime) -> DateTime<Utc> {
    ***at
}

fn before(time: &Option<AtTime>, cutoff: &DateTime<Utc>) -> bool {
    time.as_ref().map(|t| dt(t) < *cutoff).unwrap_or(false)
}

/// Undated counts as old.
fn before_or_undated(time: &Option<AtTime>, cutoff: &DateTime<Utc>) -> bool {
    time.as_ref().map(|t| dt(t) < *cutoff).unwrap_or(true)
}

fn kind_rank(kind: &WorkloadKind) -> u8 {
    if kind.holds_a_schedule() { 0 } else { 1 }
}

pub fn classify_divergence(
    config: &RecoveryConfig,
    observation: &WorkloadObservation,
    snapshot: &RecoverySnapshot,
) -> RecoveryDecisions {
    let now = dt(&snapshot.at);
    let grace_cutoff =
        now - chrono::Duration::from_std(config.dispatch_grace.0).unwrap_or_default();
    let prerun_cutoff =
        now - chrono::Duration::from_std(config.max_pre_running_time.0).unwrap_or_default();
    let completion_cutoff =
        now - chrono::Duration::from_std(config.max_completion_time.0).unwrap_or_default();
    let running_cutoff = config
        .max_running_time
        .0
        .and_then(|d| chrono::Duration::from_std(d).ok())
        .map(|d| now - d);

    // One workload per worker id; a CronJob wins over a leaked child.
    let mut workload_by_worker: HashMap<WorkerId, &ObservedWorkload> = HashMap::new();
    for workload in &observation.workloads {
        let Some(worker_id) = &workload.worker_id else {
            continue;
        };
        workload_by_worker
            .entry(worker_id.clone())
            .and_modify(|current| {
                if kind_rank(&workload.kind) < kind_rank(&current.kind) {
                    *current = workload;
                }
            })
            .or_insert(workload);
    }

    let mut decisions = RecoveryDecisions::default();

    for attempt in &snapshot.fn_attempts {
        let workload = attempt
            .worker_id
            .as_ref()
            .and_then(|worker_id| workload_by_worker.get(worker_id).copied());
        let dispatched_old = before(&attempt.pfr.base.dispatched_on.map(|d| *d), &grace_cutoff);

        // (remove workload?, recover policy?, reason)
        let outcome: Option<(bool, bool, String)> = match workload {
            None if dispatched_old => Some((false, true, "function workload missing".to_string())),
            None => None,
            Some(w) => match w.phase {
                WorkloadPhase::Pending => {
                    if attempt.pfr.base.status == PhysicalFunctionRunStatus::Dispatched
                        && before_or_undated(&w.phase_since, &prerun_cutoff)
                        && dispatched_old
                    {
                        Some((true, true, "exceeded max pre-running time".to_string()))
                    } else if attempt.pfr.base.status == PhysicalFunctionRunStatus::Running
                        && running_cutoff
                            .map(|cutoff| before(&attempt.pfr.base.started_on.map(|s| *s), &cutoff))
                            .unwrap_or(false)
                    {
                        Some((true, false, "exceeded max running time".to_string()))
                    } else {
                        None
                    }
                }
                // Function pods run the fn in init phase: Running means the
                // completion report is executing.
                WorkloadPhase::Running => {
                    if before_or_undated(&w.phase_since, &completion_cutoff) && dispatched_old {
                        Some((true, true, "exceeded max completion time".to_string()))
                    } else {
                        None
                    }
                }
                WorkloadPhase::Succeeded => {
                    // Completion budget, not dispatch grace: the terminal
                    // callback may still be retrying through an outage.
                    let finished_old = match &w.finished_at {
                        Some(finished) => dt(finished) < completion_cutoff,
                        None => before(
                            &attempt.pfr.base.dispatched_on.map(|d| *d),
                            &completion_cutoff,
                        ),
                    };
                    if finished_old {
                        // Outputs never bound; a fresh attempt is the only
                        // convergence. Terminal workloads reap on their own.
                        Some((
                            false,
                            true,
                            "workload succeeded without callback".to_string(),
                        ))
                    } else {
                        None
                    }
                }
                WorkloadPhase::Failed => {
                    // Grace lets the worker's own failure callback (with the
                    // real error) land first.
                    let failed_old = match &w.finished_at {
                        Some(finished) => dt(finished) < grace_cutoff,
                        None => dispatched_old,
                    };
                    if failed_old {
                        let reason = w
                            .message
                            .clone()
                            .unwrap_or_else(|| "function workload failed".to_string());
                        // No auto-recover: could be a crash loop.
                        Some((false, false, reason))
                    } else {
                        None
                    }
                }
                WorkloadPhase::Unknown => {
                    // Strictly dated: one undatable blip must not kill.
                    if before(&w.phase_since, &grace_cutoff) && dispatched_old {
                        Some((true, true, "workload state unknown".to_string()))
                    } else {
                        None
                    }
                }
                WorkloadPhase::Present => None,
            },
        };

        if let Some((remove, recover, reason)) = outcome {
            decisions.fn_actions.push(FnRecoveryAction {
                worker_id: attempt.worker_id.clone(),
                pfr_id: attempt.pfr.base.id.clone(),
                project_id: attempt.pfr.project_id,
                lfr: None,
                recover,
                remove: remove
                    .then(|| workload.map(|w| (w.kind, w.name.clone())))
                    .flatten(),
                reason,
            });
        }
    }

    for worker in &snapshot.cron {
        if !workload_by_worker.contains_key(&worker.base.base.id) {
            decisions.cron_recreates.push(worker.clone());
        }
    }

    for worker in &snapshot.stage {
        match workload_by_worker.get(&worker.base.base.id) {
            None => decisions.stage_recreates.push(worker.clone()),
            Some(w) => {
                let restart = match w.phase {
                    WorkloadPhase::Running | WorkloadPhase::Present => false,
                    WorkloadPhase::Pending => before_or_undated(&w.phase_since, &prerun_cutoff),
                    // Streamers are services: any exit is a restart.
                    WorkloadPhase::Succeeded | WorkloadPhase::Failed => true,
                    WorkloadPhase::Unknown => before(&w.phase_since, &grace_cutoff),
                };
                if restart {
                    decisions.stage_restarts.push((
                        worker.base.base.id.clone(),
                        w.kind,
                        w.name.clone(),
                    ));
                }
            }
        }
    }

    // Reverse direction: live workloads no candidate expects. Terminal ones
    // reap on their own, keeping their logs until then.
    let mut expected: HashSet<&WorkerId> = HashSet::new();
    for attempt in &snapshot.fn_attempts {
        if let Some(worker_id) = &attempt.worker_id {
            expected.insert(worker_id);
        }
    }
    for worker in &snapshot.cron {
        expected.insert(&worker.base.base.id);
    }
    for worker in &snapshot.stage {
        expected.insert(&worker.base.base.id);
    }
    for workload in &observation.workloads {
        let Some(worker_id) = &workload.worker_id else {
            continue;
        };
        if expected.contains(worker_id) {
            continue;
        }
        if matches!(
            workload.phase,
            WorkloadPhase::Succeeded | WorkloadPhase::Failed
        ) {
            continue;
        }
        if before_or_undated(&workload.phase_since, &grace_cutoff) {
            decisions
                .orphan_removals
                .push((workload.kind, workload.name.clone()));
        }
    }

    let cap = config.max_recoveries_per_tick.0 as usize;
    for (name, len) in [
        ("fn_actions", decisions.fn_actions.len()),
        ("cron_recreates", decisions.cron_recreates.len()),
        ("stage_recreates", decisions.stage_recreates.len()),
        ("stage_restarts", decisions.stage_restarts.len()),
        ("orphan_removals", decisions.orphan_removals.len()),
    ] {
        if len > cap {
            warn!(
                list = name,
                total = len,
                cap,
                "recovery.classify: capped; the remainder recovers next tick"
            );
        }
    }
    decisions.fn_actions.truncate(cap);
    decisions.cron_recreates.truncate(cap);
    decisions.stage_recreates.truncate(cap);
    decisions.stage_restarts.truncate(cap);
    decisions.orphan_removals.truncate(cap);

    if !decisions.fn_actions.is_empty()
        || !decisions.cron_recreates.is_empty()
        || !decisions.stage_recreates.is_empty()
        || !decisions.stage_restarts.is_empty()
        || !decisions.orphan_removals.is_empty()
    {
        debug!(
            fn_actions = decisions.fn_actions.len(),
            cron_recreates = decisions.cron_recreates.len(),
            stage_recreates = decisions.stage_recreates.len(),
            stage_restarts = decisions.stage_restarts.len(),
            orphan_removals = decisions.orphan_removals.len(),
            "recovery.classify: divergence found"
        );
    }
    decisions
}

/// Orchestrator actions before DB outcomes: a function action whose removal
/// failed is dropped — its DB outcome must not land while the workload may
/// still be alive.
#[instrument(name = "recovery.execute_workload_removals", skip_all, level = "debug")]
pub async fn execute_workload_removals(
    observer: &dyn WorkloadObserver,
    decisions: &RecoveryDecisions,
) -> RecoveryActions {
    let mut fn_actions = Vec::with_capacity(decisions.fn_actions.len());
    for action in &decisions.fn_actions {
        if let Some((kind, name)) = &action.remove
            && let Err(e) = observer.remove(kind, name).await
        {
            warn!(
                workload = %name,
                reason = ?e,
                "recovery: workload removal failed; retrying next tick"
            );
            continue;
        }
        fn_actions.push(action.clone());
    }

    for (worker_id, kind, name) in &decisions.stage_restarts {
        warn!(worker_id = %worker_id, workload = %name, "recovery: restarting streamer workload");
        if let Err(e) = observer.remove(kind, name).await {
            warn!(workload = %name, reason = ?e, "recovery: streamer removal failed; retrying next tick");
        }
    }

    for (kind, name) in &decisions.orphan_removals {
        warn!(workload = %name, "recovery: removing orphan workload");
        if let Err(e) = observer.remove(kind, name).await {
            warn!(workload = %name, reason = ?e, "recovery: orphan removal failed; retrying next tick");
        }
    }

    RecoveryActions { fn_actions }
}

/// Recreate the missing trigger workloads; a failed put retries next tick.
#[instrument(name = "recovery.recreate_triggers", skip_all, level = "debug")]
pub async fn recreate_trigger_workloads(
    bridge: &BridgeProviderHandle,
    storage: &Storage,
    internal_api: &InternalApi,
    decisions: &RecoveryDecisions,
    stage_functions: &HashMap<FunctionVersionId, FunctionDBWithBundleBasePath>,
) {
    for worker in &decisions.cron_recreates {
        warn!(worker_id = %worker.base.base.id, "recovery: recreating cron workload");
        if let Err(e) = put_cron_trigger_start(bridge, internal_api, worker).await {
            warn!(
                worker_id = %worker.base.base.id,
                reason = ?e,
                "recovery: cron recreate failed; retrying next tick"
            );
        }
    }
    for worker in &decisions.stage_recreates {
        warn!(worker_id = %worker.base.base.id, "recovery: recreating streamer workload");
        if let Err(e) =
            put_stage_trigger_start(bridge, storage, internal_api, stage_functions, worker).await
        {
            warn!(
                worker_id = %worker.base.base.id,
                reason = ?e,
                "recovery: streamer recreate failed; retrying next tick"
            );
        }
    }
}

/// Re-reads run state inside the write transaction: an attempt a concurrent
/// callback terminalized drops out (the callback wins), and the recover
/// policy resolves against current LFR status.
#[instrument(
    name = "recovery.finalize_recovery_actions",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn finalize_recovery_actions<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    actions: &RecoveryActions,
) -> Result<RecoveryActions, TdError> {
    if actions.fn_actions.is_empty() {
        return Ok((*actions).clone());
    }

    let pfr_ids: Vec<PhysicalFunctionRunId> = actions
        .fn_actions
        .iter()
        .map(|action| action.pfr_id.clone())
        .unique()
        .collect();
    let current_pfrs: Vec<PhysicalFunctionRunDB> = conn
        .history()
        .chunked()
        .fetch_all::<PhysicalFunctionRunDB>(&pfr_ids, ())
        .await?;
    let pfr_by_id: HashMap<&PhysicalFunctionRunId, &PhysicalFunctionRunDB> =
        current_pfrs.iter().map(|pfr| (&pfr.base.id, pfr)).collect();

    let execution_ids: Vec<ExecutionId> = current_pfrs
        .iter()
        .map(|pfr| pfr.base.execution_id.clone())
        .unique()
        .collect();
    let lfrs: Vec<LogicalFunctionRunDB> = conn
        .chunked()
        .fetch_all::<LogicalFunctionRunDB>(&execution_ids, ())
        .await?;
    let lfr_by_exec_fn = index_lfrs_by_exec_fn(lfrs);

    let mut fn_actions = Vec::with_capacity(actions.fn_actions.len());
    for action in &actions.fn_actions {
        let Some(pfr) = pfr_by_id.get(&action.pfr_id) else {
            continue;
        };
        if !PhysicalFunctionRunStatus::IN_FLIGHT.contains(&pfr.base.status) {
            debug!(
                function_run_id = %action.pfr_id,
                "recovery: attempt terminalized concurrently; its callback wins"
            );
            continue;
        }
        let lfr = lfr_by_exec_fn
            .get(&pfr.base.execution_id)
            .and_then(|by_fn| by_fn.get(&pfr.base.function_id))
            .cloned();
        let lfr_recoverable = lfr
            .as_ref()
            .map(|lfr| {
                matches!(
                    lfr.base.status,
                    LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::Running
                )
            })
            .unwrap_or(false);
        let recover = action.recover && lfr_recoverable;
        warn!(
            worker_id = ?action.worker_id,
            function_run_id = %action.pfr_id,
            recover,
            reason = %action.reason,
            "recovery: failing function-run attempt"
        );
        fn_actions.push(FnRecoveryAction {
            lfr,
            recover,
            ..action.clone()
        });
    }

    Ok(RecoveryActions { fn_actions })
}

pub fn failed_worker_update(at: &AtTime) -> Result<WorkerUpdateDB, TdError> {
    Ok(WorkerUpdateDB {
        started_on: None,
        ended_on: Some((*at).into()),
        status: WorkerStatus::Failed,
    })
}

pub fn failed_worker_ids(actions: &RecoveryActions) -> Result<Vec<WorkerId>, TdError> {
    Ok(actions
        .fn_actions
        .iter()
        .filter_map(|action| action.worker_id.clone())
        .unique()
        .collect())
}

/// Failed attempts' late callbacks drop at the in-flight filter.
#[instrument(
    name = "recovery.fail_recovered_attempts",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn fail_recovered_attempts<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    at: &AtTime,
    actions: &RecoveryActions,
) -> Result<(), TdError> {
    let pfr_ids: Vec<PhysicalFunctionRunId> = actions
        .fn_actions
        .iter()
        .map(|action| action.pfr_id.clone())
        .unique()
        .collect();
    if pfr_ids.is_empty() {
        return Ok(());
    }
    let update = PhysicalFunctionRunUpdateDB::failed_at(at)?;
    conn.chunked().update(&update, &pfr_ids).await?;
    Ok(())
}

/// Running LFRs with a recover verdict: `Running → ReScheduled` is not a
/// legal transition, so these are failed first (same transaction) and then
/// recovered as Failed.
pub fn fail_running_lfrs_for_recover(
    actions: &RecoveryActions,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    Ok(actions
        .fn_actions
        .iter()
        .filter(|action| action.recover)
        .filter_map(|action| action.lfr.clone())
        .filter(|lfr| lfr.base.status == LogicalFunctionRunStatus::Running)
        .unique_by(|lfr| lfr.base.id.clone())
        .collect())
}

pub fn recover_lfrs_from_actions(
    actions: &RecoveryActions,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    Ok(actions
        .fn_actions
        .iter()
        .filter(|action| action.recover)
        .filter_map(|action| action.lfr.clone())
        .filter(|lfr| {
            matches!(
                lfr.base.status,
                LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::Running
            )
        })
        // Running rows were failed just above in this transaction; the
        // status validator reads the in-memory row, so reflect that here.
        .map(|lfr| match lfr.base.status {
            LogicalFunctionRunStatus::Running => LogicalFunctionRunDB {
                base: LogicalFunctionRunInsertDB {
                    status: LogicalFunctionRunStatus::Failed,
                    ..lfr.base
                },
                ..lfr
            },
            _ => lfr,
        })
        .unique_by(|lfr| lfr.base.id.clone())
        .collect())
}

pub fn fail_lfrs_from_actions(
    actions: &RecoveryActions,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    Ok(actions
        .fn_actions
        .iter()
        .filter(|action| !action.recover)
        .filter_map(|action| action.lfr.clone())
        .filter(|lfr| {
            matches!(
                lfr.base.status,
                LogicalFunctionRunStatus::Requested | LogicalFunctionRunStatus::Running
            )
        })
        .unique_by(|lfr| lfr.base.id.clone())
        .collect())
}
