//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use std::ops::Deref;
use std::str::FromStr;
use td_common::server::{
    MessageAction, RequestMessagePayload, SupervisorMessagePayload, WorkerMessageQueue, WorkerName,
};
use td_database::dynamic::DynamicConnection;
use td_error::{ApiError, TdError, api_error};
use td_execution::SchedulerNotify;
use td_objects::dxo::function_run::{FunctionRunDB, UpdateFunctionRunDB};
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB};
use td_objects::dxo::yaml::function_runner::v2::FunctionInput;
use td_objects::types::basic::{FunctionRunId, FunctionRunStatus, WorkerId};
use td_queries::BasicQueries;
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
pub async fn unlock_workers_request<T: WorkerMessageQueue>(
    message_queue: Ref<T>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<(), TdError> {
    unlock_workers_request_infallible(message_queue, &mut conn).await;
    Ok(())
}

async fn unlock_workers_request_infallible<T: WorkerMessageQueue>(
    message_queue: Ref<T>,
    conn: &mut DynamicConnection<'_>,
) {
    let messages = message_queue.locked_messages::<serde_yaml::Value>().await;

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
            && let SupervisorMessagePayload::SupervisorRequestMessagePayload(payload) =
                &message.payload
        {
            let res = match WorkerName::from_str(&payload.worker) {
                Ok(WorkerName::Function) => {
                    unlock_function_runner_worker(
                        &mut *conn,
                        message_queue.deref(),
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::CronTrigger) => {
                    unlock_cron_trigger_workers(
                        &mut *conn,
                        message_queue.deref(),
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::StageTrigger) => {
                    unlock_stage_trigger_workers(
                        &mut *conn,
                        message_queue.deref(),
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
                    .rollback(&worker_id.to_string())
                    .await
                    .unwrap_or_else(
                        |e| error!(worker_id = %worker_id, "dispatch: rollback failed: {e:#?}"),
                    )
            }
        } else {
            warn!(
                "Scheduled locked message [{}] is not a SupervisorRequestMessagePayload: {:?}",
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
pub async fn unlock_function_runner_worker<T: WorkerMessageQueue>(
    conn: &mut DynamicConnection<'_>,
    message_queue: &T,
    worker_id: WorkerId,
    payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    match &payload.action {
        MessageAction::Start => {
            if let Some(context) = &payload.context {
                match serde_yaml::from_value::<FunctionInput>(context.clone()) {
                    Ok(context) => {
                        // Look for the function run
                        let function_run: FunctionRunDB = conn
                            .fetch_one::<FunctionRunDB>(&context.info.function_run_id)
                            .await?;

                        // Discard if unexpected status found
                        if !matches!(function_run.status, FunctionRunStatus::RunRequested) {
                            warn!(
                                worker_id = %worker_id,
                                function_run_id = %function_run.id,
                                status = %function_run.status,
                                "dispatch: function_run not in RunRequested state; rolling back"
                            );
                            rollback_function_run_db(conn, &worker_id, &function_run.id).await?;
                            Err(api_error!(
                                ApiError::InternalError,
                                "Start function locked message [{}] for function run [{}] is not in RR state: {}",
                                worker_id,
                                function_run.id,
                                function_run.status
                            ))?
                        }

                        // Unlock
                        let update = UpdateWorkerMessageStatusDB::unlocked().await?;
                        match conn.update::<_, WorkerDB>(&update, &worker_id).await {
                            Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
                                Ok(_) => {
                                    debug!(
                                        worker_id = %worker_id,
                                        function_run_id = %function_run.id,
                                        "dispatch: function_runner dispatched"
                                    );
                                    Ok(())
                                }
                                Err(e) => {
                                    rollback_function_run_db(conn, &worker_id, &function_run.id)
                                        .await?;
                                    Err(api_error!(
                                        ApiError::InternalError,
                                        "Start function locked message [{}] for function run [{}] dispatch failed: {}",
                                        worker_id,
                                        function_run.id,
                                        e
                                    ))?
                                }
                            },
                            Err(e) => {
                                rollback_function_run_db(conn, &worker_id, &function_run.id)
                                    .await?;
                                Err(api_error!(
                                    ApiError::InternalError,
                                    "Start function locked message [{}] for function run [{}] dispatch failed: {}",
                                    worker_id,
                                    function_run.id,
                                    e
                                ))?
                            }
                        }
                    }
                    Err(e) => Err(api_error!(
                        ApiError::InternalError,
                        "Start function locked message [{worker_id}] has invalid context [{context:?}]: {e:?}",
                    ))?,
                }
            } else {
                Err(api_error!(
                    ApiError::InternalError,
                    "Start function locked message [{worker_id}] has no context: {payload:?}"
                ))?
            }
        }
        action => Err(api_error!(
            ApiError::InternalError,
            "Function runner [{action}] action for locked message [{worker_id}] is not supported: {payload:?}"
        )),
    }
}

async fn rollback_function_run_db(
    conn: &mut DynamicConnection<'_>,
    message_id: &WorkerId,
    function_run_id: &FunctionRunId,
) -> Result<(), TdError> {
    let update = UpdateFunctionRunDB::scheduled()?;
    match conn
        .update::<_, FunctionRunDB>(&update, function_run_id)
        .await
    {
        Ok(_) => {
            error!(
                "Rolled back status to scheduled for locked message [{message_id}] for function run [{function_run_id}] in DB"
            );
        }
        Err(e) => {
            error!(
                worker_id = %message_id,
                function_run_id = %function_run_id,
                "Failed to rollback status to scheduled: {e:#?}"
            );
        }
    }

    Ok(())
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "cron_trigger"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_cron_trigger_workers<T: WorkerMessageQueue>(
    conn: &mut DynamicConnection<'_>,
    message_queue: &T,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    // TODO add validation
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    match conn.update::<_, WorkerDB>(&update, &worker_id).await {
        Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
            Ok(_) => {
                debug!(worker_id = %worker_id, "dispatch: cron_trigger dispatched");
                Ok(())
            }
            Err(e) => Err(api_error!(
                ApiError::InternalError,
                "CronTrigger locked message [{}] dispatch failed: {}",
                worker_id,
                e
            ))?,
        },
        Err(e) => Err(api_error!(
            ApiError::InternalError,
            "CronTrigger locked message [{}] dispatch failed: {}",
            worker_id,
            e
        ))?,
    }
}

#[instrument(
    name = "scheduler.unlock_worker",
    skip_all,
    fields(worker_id = %worker_id, kind = "stage_trigger"),
    level = "debug",
    err(Debug)
)]
pub async fn unlock_stage_trigger_workers<T: WorkerMessageQueue>(
    conn: &mut DynamicConnection<'_>,
    message_queue: &T,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    // TODO add validation
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    match conn.update::<_, WorkerDB>(&update, &worker_id).await {
        Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
            Ok(_) => {
                debug!(worker_id = %worker_id, "dispatch: stage_trigger dispatched");
                Ok(())
            }
            Err(e) => Err(api_error!(
                ApiError::InternalError,
                "StageTrigger locked message [{}] dispatch failed: {}",
                worker_id,
                e
            ))?,
        },
        Err(e) => Err(api_error!(
            ApiError::InternalError,
            "StageTrigger locked message [{}] dispatch failed: {}",
            worker_id,
            e
        ))?,
    }
}
