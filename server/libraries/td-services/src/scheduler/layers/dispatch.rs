//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use sqlx::SqliteConnection;
use std::ops::Deref;
use std::str::FromStr;
use td_common::server::{
    MessageAction, RequestMessagePayload, SupervisorMessagePayload, WorkerMessageQueue, WorkerName,
};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::function_run::{FunctionRunDB, UpdateFunctionRunDB};
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB};
use td_objects::dxo::yaml::function_runner::v2::FunctionInput;
use td_objects::sql::{DaoQueries, SelectBy, UpdateBy};
use td_objects::types::basic::{FunctionRunId, FunctionRunStatus, WorkerId};
use td_tower::extractors::{Connection, IntoMutSqlConnection, SrvCtx};
use tracing::{error, trace, warn};

pub async fn unlock_workers_request<T: WorkerMessageQueue>(
    message_queue: SrvCtx<T>,
    queries: SrvCtx<DaoQueries>,
    Connection(connection): Connection,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    unlock_workers_request_infallible(message_queue, queries, conn).await;
    Ok(())
}

async fn unlock_workers_request_infallible<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    conn: &mut SqliteConnection,
) {
    let messages = message_queue.locked_messages::<serde_yaml::Value>().await;

    for message in messages {
        if let Ok(worker_id) = WorkerId::try_from(&message.id)
            && let SupervisorMessagePayload::SupervisorRequestMessagePayload(payload) =
                &message.payload
        {
            let res = match WorkerName::from_str(&payload.worker) {
                Ok(WorkerName::FUNCTION) => {
                    unlock_function_runner_worker(
                        queries.deref(),
                        &mut *conn,
                        message_queue.deref(),
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::CRON) => {
                    unlock_cron_workers(
                        queries.deref(),
                        &mut *conn,
                        message_queue.deref(),
                        worker_id,
                        payload,
                    )
                    .await
                }
                Ok(WorkerName::WATCHDOG) => {
                    unlock_watchdog_workers(
                        queries.deref(),
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

            if res.is_err() {
                message_queue
                    .rollback(&worker_id.to_string())
                    .await
                    .unwrap_or_else(|e| error!("Failed to rollback worker [{worker_id}]: {e}"))
            }
        } else {
            warn!(
                "Scheduled locked message [{}] is not a SupervisorRequestMessagePayload: {:?}",
                message.id, message.payload
            );
        };
    }
}

pub async fn unlock_function_runner_worker<T: WorkerMessageQueue>(
    queries: &DaoQueries,
    conn: &mut SqliteConnection,
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
                        let function_run: FunctionRunDB = queries
                            .select_by::<FunctionRunDB>(&context.info.function_run_id)?
                            .build_query_as()
                            .fetch_one(&mut *conn)
                            .await
                            .map_err(handle_sql_err)?;

                        // Discard if unexpected status found
                        if !matches!(function_run.status, FunctionRunStatus::RunRequested) {
                            rollback_function_run_db(queries, conn, &worker_id, &function_run.id)
                                .await?;
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
                        match queries
                            .update_by::<_, WorkerDB>(&update, &worker_id)?
                            .build()
                            .execute(&mut *conn)
                            .await
                        {
                            Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
                                Ok(_) => {
                                    trace!(
                                        "Start function locked message [{}] for function run [{}] is dispatched",
                                        worker_id, function_run.id
                                    );
                                    Ok(())
                                }
                                Err(e) => {
                                    rollback_function_run_db(
                                        queries,
                                        conn,
                                        &worker_id,
                                        &function_run.id,
                                    )
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
                                rollback_function_run_db(
                                    queries,
                                    conn,
                                    &worker_id,
                                    &function_run.id,
                                )
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
    queries: &DaoQueries,
    conn: &mut SqliteConnection,
    message_id: &WorkerId,
    function_run_id: &FunctionRunId,
) -> Result<(), TdError> {
    let update = UpdateFunctionRunDB::scheduled()?;
    match queries
        .update_by::<_, FunctionRunDB>(&update, &function_run_id)?
        .build()
        .execute(&mut *conn)
        .await
    {
        Ok(_) => {
            error!(
                "Rolled back status to scheduled for locked message [{message_id}] for function run [{function_run_id}] in DB"
            );
        }
        Err(e) => {
            error!(
                "Failed to rollback status to scheduled for locked message [{message_id}] for function run [{function_run_id}]: {e}"
            );
        }
    }

    Ok(())
}

pub async fn unlock_cron_workers<T: WorkerMessageQueue>(
    queries: &DaoQueries,
    conn: &mut SqliteConnection,
    message_queue: &T,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    // TODO add validation
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    match queries
        .update_by::<_, WorkerDB>(&update, &worker_id)?
        .build()
        .execute(&mut *conn)
        .await
    {
        Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
            Ok(_) => {
                trace!("Cron locked message [{}] is dispatched", worker_id);
                Ok(())
            }
            Err(e) => Err(api_error!(
                ApiError::InternalError,
                "Cron locked message [{}] dispatch failed: {}",
                worker_id,
                e
            ))?,
        },
        Err(e) => Err(api_error!(
            ApiError::InternalError,
            "Cron locked message [{}] dispatch failed: {}",
            worker_id,
            e
        ))?,
    }
}

pub async fn unlock_watchdog_workers<T: WorkerMessageQueue>(
    queries: &DaoQueries,
    conn: &mut SqliteConnection,
    message_queue: &T,
    worker_id: WorkerId,
    _payload: &RequestMessagePayload,
) -> Result<(), TdError> {
    // TODO add validation
    let update = UpdateWorkerMessageStatusDB::unlocked().await?;
    match queries
        .update_by::<_, WorkerDB>(&update, &worker_id)?
        .build()
        .execute(&mut *conn)
        .await
    {
        Ok(_) => match message_queue.commit(&worker_id.to_string()).await {
            Ok(_) => {
                trace!("Watchdog locked message [{}] is dispatched", worker_id);
                Ok(())
            }
            Err(e) => Err(api_error!(
                ApiError::InternalError,
                "Watchdog locked message [{}] dispatch failed: {}",
                worker_id,
                e
            ))?,
        },
        Err(e) => Err(api_error!(
            ApiError::InternalError,
            "Watchdog locked message [{}] dispatch failed: {}",
            worker_id,
            e
        ))?,
    }
}
