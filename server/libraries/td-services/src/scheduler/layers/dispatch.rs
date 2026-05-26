//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use std::ops::Deref;
use std::str::FromStr;
use serde_yaml::Value;
use td_common::server::{
    MessageAction, OrchestratorMessage, OrchestratorMessagePayload, OrchestratorMessageQueue,
    OrchestratorMessageQueueHandle, RequestMessagePayload, WorkerName,
};
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_execution::SchedulerNotify;
use td_objects::and;
use td_objects::dxo::logical::{LogicalFunctionRunDB, UpdateLogicalFunctionRunDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB};
use td_objects::dxo::yaml::function_runner::v2::FunctionInput;
use td_objects::types::basic::{
    LogicalFunctionRunId, LogicalFunctionRunStatus, PhysicalFunctionRunId, WorkerId,
};
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};
use tracing::{debug, error, instrument, warn};

#[instrument(name = "scheduler.notify_dispatcher", skip_all, level = "debug")]
pub async fn notify_dispatcher(notify: Ref<SchedulerNotify>) -> Result<(), TdError> {
    notify.notify_dispatcher();
    Ok(())
}

#[instrument(
    name = "scheduler.dispatch_tick",
    skip_all,
    level = "debug",
    err(Debug)
)]
pub async fn unlock_workers_request(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<(), TdError> {
    unlock_workers_request_infallible(message_queue, &mut conn).await;
    Ok(())
}

async fn unlock_workers_request_infallible(
    message_queue: Ref<OrchestratorMessageQueueHandle>,
    conn: &mut DynamicConnection<'_>,
) {
    let messages = message_queue.locked_messages().await;

    let message_count = messages.len();
    if message_count > 0 {
        debug!(
            locked_count = message_count,
            "scheduler.dispatch_tick: {} locked message(s) to process", message_count
        );
    }
    let mut rollback_count: usize = 0;

    for message in messages {
        if let Ok(worker_id) = WorkerId::try_from(&message.id)
            && let OrchestratorMessagePayload::OrchestratorRequestMessagePayload(payload) =
                &message.payload
        {
            let res = match WorkerName::from_str(&payload.worker) {
                Ok(WorkerName::Function) => {
                    unlock_function_runner_worker(
                        &mut *conn,
                        message_queue.deref(),
                        &message,
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::CronTrigger) => {
                    unlock_cron_trigger_workers(
                        &mut *conn,
                        message_queue.deref(),
                        &message,
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::StageTrigger) => {
                    unlock_stage_trigger_workers(
                        &mut *conn,
                        message_queue.deref(),
                        &message,
                        worker_id,
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
                rollback_count += 1;
                message_queue
                    .rollback(&message)
                    .await
                    .unwrap_or_else(
                        |e| error!(worker_id = %worker_id, "dispatch: rollback failed: {e:#?}"),
                    )
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
            rollback_count = rollback_count,
            "scheduler.dispatch_tick: end"
        );
    }
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "function_runner"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_function_runner_worker(
    conn: &mut DynamicConnection<'_>,
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
        .live()
        .fetch_one::<PhysicalFunctionRunDB>(&context.info.function_run_id)
        .await?;
    let lfr: LogicalFunctionRunDB = conn
        .live()
        .fetch_one::<LogicalFunctionRunDB>(and!(&pfr.execution_id, &pfr.function_id))
        .await?;

    let branch = match &lfr.status {
        LogicalFunctionRunStatus::Requested => DispatchBranch::Initial,
        LogicalFunctionRunStatus::Committed
        | LogicalFunctionRunStatus::Canceled
        | LogicalFunctionRunStatus::Yanked
        | LogicalFunctionRunStatus::Done
        | LogicalFunctionRunStatus::Failed => DispatchBranch::Replay,
        _ => {
            warn!(
                worker_id = %worker_id,
                physical_function_run_id = %pfr.id,
                lfr_id = %lfr.id,
                lfr_status = %lfr.status,
                "dispatch: LFR in unexpected state for dispatch; rolling back"
            );
            rollback_initial_function_run(conn, &worker_id, &lfr.id).await;
            return Err(api_error!(
                ApiError::InternalError,
                "Start function locked message [{}] for LFR [{}] in unexpected state: {}",
                worker_id,
                lfr.id,
                lfr.status,
            ));
        }
    };

    commit_unlock(conn, message_queue, message, &worker_id, &lfr.id, &pfr.id, branch).await
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

async fn commit_unlock(
    conn: &mut DynamicConnection<'_>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: &WorkerId,
    lfr_id: &LogicalFunctionRunId,
    pfr_id: &PhysicalFunctionRunId,
    branch: DispatchBranch,
) -> Result<(), TdError> {
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    let outcome: Result<(), String> = async {
        conn.live()
            .update::<_, WorkerDB>(&update, worker_id)
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
            // Replay path: queue-only retry; no PFR or pointer mutation
            // here. The PFR stays at `Dispatched`; operator recover
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

async fn rollback_initial_function_run(
    conn: &mut DynamicConnection<'_>,
    message_id: &WorkerId,
    lfr_id: &LogicalFunctionRunId,
) {
    // Initial path: LFR status lives on the LFR row. Rollback flips
    // it back to Scheduled so the next tick picks it up again.
    let update = match UpdateLogicalFunctionRunDB::scheduled().await {
        Ok(u) => u,
        Err(e) => {
            error!(
                worker_id = %message_id,
                lfr_id = %lfr_id,
                "Failed to build rollback update: {e:#?}"
            );
            return;
        }
    };
    match conn
        .live()
        .update::<_, LogicalFunctionRunDB>(&update, lfr_id)
        .await
    {
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
pub async fn unlock_cron_trigger_workers(
    conn: &mut DynamicConnection<'_>,
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
pub async fn unlock_stage_trigger_workers(
    conn: &mut DynamicConnection<'_>,
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

async fn unlock_simple_worker(
    conn: &mut DynamicConnection<'_>,
    message_queue: &dyn OrchestratorMessageQueue,
    message: &OrchestratorMessage<Value>,
    worker_id: WorkerId,
    error_label: &str,
    log_kind: &str,
) -> Result<(), TdError> {
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    conn.live()
        .update::<_, WorkerDB>(&update, &worker_id)
        .await
        .map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "{error_label} locked message [{worker_id}] dispatch failed: {e}"
            )
        })?;
    message_queue
        .commit(message)
        .await
        .map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "{error_label} locked message [{worker_id}] dispatch failed: {e}"
            )
        })?;
    debug!(worker_id = %worker_id, "dispatch: {log_kind} dispatched");
    Ok(())
}
