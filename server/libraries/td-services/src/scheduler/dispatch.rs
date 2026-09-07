//
// Copyright 2025 Tabsdata Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use serde_yaml::Value;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use td_bridge::BridgeProviderHandle;
use td_bridge::messaging::{
    OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue,
};
use td_common::worker::{MessageAction, RequestMessagePayload, WorkerName};
use td_database::access::{AcrossProjects, DbPool, Grant, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_objects::and;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunStatusUpdateDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::{WorkerDB, WorkerMessageStatusUpdateDB};
use td_objects::dxo::yaml::function::FunctionInput;
use td_objects::types::basic::{
    LogicalFunctionRunId, LogicalFunctionRunStatus, PhysicalFunctionRunId, ProjectId, WorkerId,
};
use td_queries::Queries;
use tracing::{debug, error, instrument, warn};

#[instrument(
    name = "scheduler.dispatch_tick",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn unlock_workers_request(
    bridge: &BridgeProviderHandle,
    db: &DbPool<AcrossProjects>,
    grant: Grant,
) -> Result<(), TdError> {
    let message_queue = bridge.message_queue();
    let messages = message_queue.locked_messages().await;
    if messages.is_empty() {
        return Ok(());
    }
    debug!(
        locked_count = messages.len(),
        "scheduler.dispatch_tick: {} locked message(s) to process",
        messages.len()
    );

    let (by_project, unresolved) = messages_by_project(db, grant, &messages).await?;
    // Rolled back, not dropped: a message left in the locked set is retried
    // every tick forever.
    for message in unresolved {
        warn!(
            message_id = %message.id,
            "dispatch: locked message has no resolvable worker; rolling back"
        );
        message_queue.rollback(&message).await.unwrap_or_else(
            |e| error!(message_id = %message.id, "dispatch: rollback failed: {e:#?}"),
        );
    }

    // One transaction per project, so a project whose messages fail does not
    // abort the others' unlocks.
    for (project, messages) in by_project {
        let mut trx = db.in_project(&project).begin_exclusive().await?;
        debug!(
            project_id = %project,
            message_count = messages.len(),
            "scheduler.dispatch_tick: unlocking one project's messages"
        );
        let failed =
            unlock_workers_request_infallible(message_queue, trx.get(grant), messages).await;
        trx.commit().await?;
        // A rollback deletes the workload: no connection may be open for it.
        for message in failed {
            message_queue.rollback(&message).await.unwrap_or_else(
                |e| error!(message_id = %message.id, "dispatch: rollback failed: {e:#?}"),
            );
        }
    }
    Ok(())
}

/// Group the locked messages by the project owning their worker, returning the
/// ones whose worker could not be resolved separately for the caller to
/// dispose of.
type GroupedMessages = (
    Vec<(ProjectId, Vec<OrchestratorMessage<Value>>)>,
    Vec<OrchestratorMessage<Value>>,
);

async fn messages_by_project(
    db: &DbPool<AcrossProjects>,
    grant: Grant,
    messages: &[OrchestratorMessage<Value>],
) -> Result<GroupedMessages, TdError> {
    let worker_ids: Vec<WorkerId> = messages
        .iter()
        .filter_map(|m| WorkerId::try_from(m.id.as_str()).ok())
        .collect();
    let mut conn = db.read().await?;
    let workers: Vec<WorkerDB> = conn
        .get(grant)
        .chunked()
        .fetch_all::<WorkerDB>(&worker_ids, ())
        .await?;
    let project_of: HashMap<&WorkerId, &ProjectId> = workers
        .iter()
        .map(|w| (&w.base.id, &w.project_id))
        .collect();

    let mut grouped: BTreeMap<ProjectId, Vec<OrchestratorMessage<Value>>> = BTreeMap::new();
    let mut unresolved = Vec::new();
    for message in messages {
        let project = WorkerId::try_from(message.id.as_str())
            .ok()
            .and_then(|id| project_of.get(&id).map(|p| *(*p)));
        match project {
            Some(project) => grouped.entry(project).or_default().push(message.clone()),
            None => unresolved.push(message.clone()),
        }
    }
    Ok((grouped.into_iter().collect(), unresolved))
}

async fn unlock_workers_request_infallible<S: SeesProjectRows>(
    message_queue: &dyn OrchestratorMessageQueue,
    conn: &mut DomainConnection<Apiserver, S>,
    messages: Vec<OrchestratorMessage<Value>>,
) -> Vec<OrchestratorMessage<Value>> {
    let message_count = messages.len();
    let mut to_rollback = Vec::new();

    for message in messages {
        if let Ok(worker_id) = WorkerId::try_from(message.id.as_str())
            && let OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload) =
                &message.payload
        {
            let res = match WorkerName::from_str(&payload.worker) {
                Ok(WorkerName::Function) => {
                    unlock_function_runner_worker(
                        &mut *conn,
                        message_queue,
                        &message,
                        worker_id.clone(),
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::CronTrigger) => {
                    unlock_cron_trigger_workers(
                        &mut *conn,
                        message_queue,
                        &message,
                        worker_id.clone(),
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::StageTrigger) => {
                    unlock_stage_trigger_workers(
                        &mut *conn,
                        message_queue,
                        &message,
                        worker_id.clone(),
                        payload,
                    )
                    .await
                }
                Err(e) => Err(api_error!(
                    ApiError::InternalError,
                    "Scheduled locked message [{worker_id}] has unknown worker [{:?}]: {e:?}",
                    payload.worker
                )),
            };

            if let Err(ref e) = res {
                warn!(
                    worker_id = %worker_id,
                    reason = ?e,
                    "dispatch: rolling back worker"
                );
                to_rollback.push(message);
            }
        } else {
            warn!(
                "Scheduled locked message [{}] is not a OrchestratorRequestMessagePayload: {:?}",
                message.id, message.payload
            );
        };
    }

    if message_count > 0 {
        debug!(
            locked_count = message_count,
            rollback_count = to_rollback.len(),
            "scheduler.dispatch_tick: end"
        );
    }
    to_rollback
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "function_runner"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_function_runner_worker<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: WorkerId,
    payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    let MessageAction::Start = &payload.action else {
        return Err(api_error!(
            ApiError::InternalError,
            "Function runner [{}] action for locked message [{worker_id}] is not supported: {payload:?}",
            &payload.action,
        ));
    };
    let Some(context) = &payload.context else {
        return Err(api_error!(
            ApiError::InternalError,
            "Start function locked message [{worker_id}] has no context: {payload:?}"
        ));
    };
    let context: FunctionInput = serde_yaml::from_value(context.clone()).map_err(|e| {
        api_error!(
            ApiError::InternalError,
            "Start function locked message [{worker_id}] has invalid context [{context:?}]: {e:?}",
        )
    })?;

    // Resolve the FR and its LFR. The branch (initial vs replay) is
    // determined entirely by LFR.status — Requested = initial dispatch
    // in flight; sticky-terminal = replay dispatch (LFR is canonical
    // and doesn't move during a replay).
    let pfr: PhysicalFunctionRunDB = conn
        .fetch_one::<PhysicalFunctionRunDB>(&context.info.function_run_id)
        .await?;
    // The branch below is decided from `lfr.status` and the rollback writes it.
    let lfr: LogicalFunctionRunDB = conn
        .fetch_one::<LogicalFunctionRunDB>(&pfr.base.logical_function_run_id)
        .await?;

    let branch = match &lfr.base.status {
        LogicalFunctionRunStatus::Requested => DispatchBranch::Initial,
        LogicalFunctionRunStatus::Committed
        | LogicalFunctionRunStatus::Canceled
        | LogicalFunctionRunStatus::Yanked
        | LogicalFunctionRunStatus::Done
        | LogicalFunctionRunStatus::Failed => DispatchBranch::Replay,
        _ => {
            warn!(
                worker_id = %worker_id,
                physical_function_run_id = %pfr.base.id,
                lfr_id = %lfr.base.id,
                lfr_status = %lfr.base.status,
                "dispatch: LFR in unexpected state for dispatch"
            );
            // No rollback: every status reaching this arm is one that must not
            // be forced back to Scheduled — Running or OnHold would re-dispatch
            // a run that is live or blocked.
            return Err(api_error!(
                ApiError::InternalError,
                "Start function locked message [{}] for LFR [{}] in unexpected state: {}",
                worker_id,
                lfr.base.id,
                lfr.base.status,
            ));
        }
    };

    commit_unlock(
        conn,
        message_queue,
        message,
        &worker_id,
        &lfr.base.id,
        &pfr.base.id,
        branch,
    )
    .await
}

/// Which dispatch path this worker is on. Decides rollback semantics on
/// unlock failure (initial flips LFR.status back; replay is a no-op
/// at the data layer — the message-queue retry path drives recovery).
#[derive(Clone, Copy)]
enum DispatchBranch {
    Initial,
    Replay,
}

impl DispatchBranch {
    fn name(&self) -> &'static str {
        match self {
            DispatchBranch::Initial => "initial",
            DispatchBranch::Replay => "replay",
        }
    }
}

async fn commit_unlock<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: &WorkerId,
    lfr_id: &LogicalFunctionRunId,
    pfr_id: &PhysicalFunctionRunId,
    branch: DispatchBranch,
) -> Result<(), TdError> {
    let update = WorkerMessageStatusUpdateDB::unlocked();
    let outcome: Result<(), String> = async {
        conn.update(&update, worker_id)
            .await
            .map_err(|e| e.to_string())?;
        message_queue
            .commit(message)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => {
            debug!(
                worker_id = %worker_id,
                physical_function_run_id = %pfr_id,
                branch = branch.name(),
                "dispatch: function_runner dispatched"
            );
            Ok(())
        }
        Err(e) => {
            if matches!(branch, DispatchBranch::Initial) {
                rollback_initial_function_run(conn, worker_id, lfr_id).await;
            }
            // Replay path: queue-only retry; no PFR mutation here. The
            // PFR stays at `Dispatched`; operator recover
            // (PFR.status Failed→Requested) is the only path that
            // re-enters `to_dispatch`.
            Err(api_error!(
                ApiError::InternalError,
                "Start function locked message [{}] for LFR [{}] dispatch failed: {}",
                worker_id,
                lfr_id,
                e
            ))
        }
    }
}

async fn rollback_initial_function_run<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_id: &WorkerId,
    lfr_id: &LogicalFunctionRunId,
) {
    // Conditional on `Requested`: the read feeding this was unlocked, and
    // forcing a run that has since moved on back to Scheduled would re-run it.
    let update = LogicalFunctionRunStatusUpdateDB::scheduled();
    match conn
        .update(&update, and!(lfr_id, LogicalFunctionRunStatus::Requested))
        .await
    {
        Ok(result) if result.rows_affected() == 0 => {
            warn!(
                worker_id = %message_id,
                lfr_id = %lfr_id,
                "dispatch: LFR no longer Requested; nothing to roll back"
            );
        }
        Ok(_) => {
            error!(
                "Rolled back LFR status to Scheduled for locked message [{message_id}] for LFR [{lfr_id}] in DB"
            );
        }
        Err(e) => {
            error!(
                worker_id = %message_id,
                lfr_id = %lfr_id,
                "Failed to rollback LFR status to Scheduled: {e:#?}"
            );
        }
    }
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "cron_trigger"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_cron_trigger_workers<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    unlock_simple_worker(
        conn,
        message_queue,
        message,
        worker_id,
        "CronTrigger",
        "cron_trigger",
    )
    .await
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "stage_trigger"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_stage_trigger_workers<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    unlock_simple_worker(
        conn,
        message_queue,
        message,
        worker_id,
        "StageTrigger",
        "stage_trigger",
    )
    .await
}

async fn unlock_simple_worker<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: WorkerId,
    error_label: &str,
    log_kind: &str,
) -> Result<(), TdError> {
    let update = WorkerMessageStatusUpdateDB::unlocked();
    conn.update(&update, &worker_id).await.map_err(|e| {
        api_error!(
            ApiError::InternalError,
            "{error_label} locked message [{worker_id}] dispatch failed: {e}"
        )
    })?;
    message_queue.commit(message).await.map_err(|e| {
        api_error!(
            ApiError::InternalError,
            "{error_label} locked message [{worker_id}] dispatch failed: {e}"
        )
    })?;
    debug!(worker_id = %worker_id, "dispatch: {log_kind} dispatched");
    Ok(())
}
