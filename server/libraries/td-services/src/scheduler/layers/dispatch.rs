//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use itertools::{Either, Itertools};
use sqlx::SqliteConnection;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;
use td_common::server::{SupervisorMessagePayload, WorkerMessageQueue, WorkerName};
use td_error::TdError;
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::function_run::{FunctionRunDB, UpdateFunctionRunDB};
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB};
use td_objects::dxo::yaml::cron::v1::CronInput;
use td_objects::dxo::yaml::function_runner::v2::FunctionInput;
use td_objects::dxo::yaml::watchdog::v1::WatchdogInput;
use td_objects::sql::{DaoQueries, FindBy, UpdateBy};
use td_objects::types::basic::{FunctionRunId, FunctionRunStatus, WorkerId};
use td_tower::extractors::{Connection, IntoMutSqlConnection, SrvCtx};
use tracing::{error, trace};

pub async fn unlock_workers<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
) -> Result<(), TdError> {
    let messages = message_queue.locked_messages::<serde_yaml::Value>().await;
    let (
        function_runner_messages,
        cron_messages,
        watchdog_messages,
        invalid_requests,
        invalid_messages
    ) = messages.into_iter().fold(
        (HashMap::new(), HashMap::new(), HashMap::new(), HashSet::new(), HashSet::new()),
        |(mut function_runner_messages, mut cron_messages, mut watchdog_messages, mut invalid_requests, mut invalid_messages), message| {
            if let Ok(worker_id) = WorkerId::try_from(&message.id) &&
                let SupervisorMessagePayload::SupervisorRequestMessagePayload(
                    payload,
                ) = &message.payload
            {
                match &payload.context {
                    None => {
                        error!("Scheduled locked message [{worker_id}] has no context: {payload:?}");
                        invalid_requests.insert(worker_id);
                    }
                    Some(context) => {
                        match WorkerName::from_str(&payload.worker) {
                            Ok(WorkerName::FUNCTION) => {
                                match serde_yaml::from_value(context.clone()) {
                                    Ok(context) => {
                                        function_runner_messages.insert(worker_id, context);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Scheduled locked message [{worker_id}] has invalid context [{context:?}]: {e:?}",
                                        );
                                        invalid_requests.insert(worker_id);
                                    }
                                }
                            }
                            Ok(WorkerName::CRON) => {
                                match serde_yaml::from_value(context.clone()) {
                                    Ok(context) => {
                                        cron_messages.insert(worker_id, context);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Scheduled locked message [{worker_id}] has invalid context [{context:?}]: {e:?}",
                                        );
                                        invalid_requests.insert(worker_id);
                                    }
                                }
                            }
                            Ok(WorkerName::WATCHDOG) => {
                                match serde_yaml::from_value(context.clone()) {
                                    Ok(context) => {
                                        watchdog_messages.insert(worker_id, context);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Scheduled locked message [{worker_id}] has invalid context [{context:?}]: {e:?}",
                                        );
                                        invalid_requests.insert(worker_id);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Scheduled locked message [{worker_id}] has invalid worker [{:?}]: {e:?}", payload.worker);
                                invalid_requests.insert(worker_id);
                            }
                        };
                    }
                }
            } else {
                error!(
                    "Scheduled locked message [{}] is not a SupervisorRequestMessagePayload: {:?}",
                    message.id, message.payload
                );
                invalid_messages.insert(message.id);
            };
            (function_runner_messages, cron_messages, watchdog_messages, invalid_requests, invalid_messages)
        },
    );

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;
    unlock_function_runner_workers(
        queries.deref(),
        &mut *conn,
        message_queue.deref(),
        function_runner_messages,
    )
    .await?;
    unlock_cron_workers(
        queries.deref(),
        &mut *conn,
        message_queue.deref(),
        cron_messages,
    )
    .await?;
    unlock_watchdog_workers(
        queries.deref(),
        &mut *conn,
        message_queue.deref(),
        watchdog_messages,
    )
    .await?;
    rollback_invalid(
        queries.deref(),
        &mut *conn,
        message_queue.deref(),
        invalid_requests,
        invalid_messages,
    )
    .await?;
    Ok(())
}

pub async fn unlock_function_runner_workers<T: WorkerMessageQueue>(
    queries: &DaoQueries,
    conn: &mut SqliteConnection,
    message_queue: &T,
    messages: HashMap<WorkerId, FunctionInput>,
) -> Result<(), TdError> {
    // Filter all invalid messages
    let function_run_ids = messages
        .iter()
        .map(|(id, m)| (m.info.function_run_id, (id, m)))
        .collect::<HashMap<_, _>>();

    let mut errors = Vec::new();

    // Find all associated function runs
    let found_function_runs: Vec<FunctionRunDB> = {
        // TODO this should be chunked
        let lookup: Vec<_> = function_run_ids.keys().copied().collect();
        queries
            .find_by::<FunctionRunDB>(&lookup)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?
    };
    let found_function_runs: HashMap<_, _> =
        found_function_runs.iter().map(|f| (f.id, f)).collect();

    // Rollback all message for function runs that are not found
    let not_found_runs = function_run_ids
        .iter()
        .filter(|(id, _)| !found_function_runs.contains_key(*id))
        .collect::<Vec<_>>();
    for (f_id, (w_id, _)) in not_found_runs {
        trace!("Locked message [{w_id}] for function run [{f_id}] was not found in queue");
        message_queue
            .rollback(&w_id.to_string())
            .await
            .unwrap_or_else(|err| errors.push(err.into()));
    }

    // Split in valid and invalid function runs
    let (valid_runs, invalid_runs): (Vec<_>, Vec<_>) =
        found_function_runs.into_iter().partition_map(|(id, f)| {
            if matches!(f.status, FunctionRunStatus::RunRequested) {
                Either::Left(id)
            } else {
                error!(
                    "Scheduled locked message [{}] for function run [{}] is not in RR state: {}",
                    id, f.id, f.status
                );
                Either::Right(id)
            }
        });

    // Commit all valid function runs
    for f_id in &valid_runs {
        let (w_id, _message) = function_run_ids[f_id];
        let update = UpdateWorkerMessageStatusDB::unlocked()?;
        // TODO the queue should use this typed too
        match queries
            .update_by::<_, WorkerDB>(&update, &w_id)?
            .build()
            .execute(&mut *conn)
            .await
        {
            Ok(_) => match message_queue.commit(&w_id.to_string()).await {
                Ok(_) => {
                    trace!(
                        "Scheduled locked message [{w_id}] for function run [{f_id}] is committed"
                    );
                }
                Err(e) => {
                    error!(
                        "Scheduled locked message [{w_id}] for function run [{f_id}] commit failed: {e}"
                    );
                    rollback_function_run_db(queries, conn, w_id, f_id)
                        .await
                        .unwrap_or_else(|e| errors.push(e));
                    message_queue
                        .rollback(&w_id.to_string())
                        .await
                        .unwrap_or_else(|err| errors.push(err.into()));
                }
            },
            Err(e) => {
                error!(
                    "Scheduled locked message [{w_id}] for function run [{f_id}] commit failed: {e}"
                );
            }
        }
    }

    // Rollback all invalid function runs
    for f_id in &invalid_runs {
        let (w_id, _message) = function_run_ids[f_id];
        rollback_function_run_db(queries, &mut *conn, w_id, f_id)
            .await
            .unwrap_or_else(|e| errors.push(e));
        message_queue
            .rollback(&w_id.to_string())
            .await
            .unwrap_or_else(|err| errors.push(err.into()));
    }

    // We try to run everything and avoid failures.
    if !errors.is_empty() {
        error!("Errors found: {errors:?}");
    }
    Ok(())
}

pub async fn unlock_cron_workers<T: WorkerMessageQueue>(
    _queries: &DaoQueries,
    _conn: &mut SqliteConnection,
    message_queue: &T,
    messages: HashMap<WorkerId, CronInput>,
) -> Result<(), TdError> {
    let mut errors = Vec::new();

    // TODO add validation
    for (w_id, _input) in messages {
        message_queue
            .commit(&w_id.to_string())
            .await
            .unwrap_or_else(|e| errors.push(e));
    }

    // We try to run everything and avoid failures.
    if !errors.is_empty() {
        error!("Errors found: {errors:?}");
    }
    Ok(())
}

pub async fn unlock_watchdog_workers<T: WorkerMessageQueue>(
    _queries: &DaoQueries,
    _conn: &mut SqliteConnection,
    message_queue: &T,
    messages: HashMap<WorkerId, WatchdogInput>,
) -> Result<(), TdError> {
    let mut errors = Vec::new();

    // TODO add validation
    for (w_id, _input) in messages {
        message_queue
            .commit(&w_id.to_string())
            .await
            .unwrap_or_else(|e| errors.push(e));
    }

    // We try to run everything and avoid failures.
    if !errors.is_empty() {
        error!("Errors found: {errors:?}");
    }
    Ok(())
}

async fn rollback_invalid<T: WorkerMessageQueue>(
    _queries: &DaoQueries,
    _conn: &mut SqliteConnection,
    message_queue: &T,
    invalid_requests: HashSet<WorkerId>,
    invalid_messages: HashSet<String>,
) -> Result<(), TdError> {
    let mut errors = Vec::new();
    for id in invalid_requests {
        // TODO this should also rollback in db
        message_queue
            .rollback(&id.to_string())
            .await
            .unwrap_or_else(|err| errors.push(err));
    }

    for id in invalid_messages {
        message_queue
            .rollback(&id)
            .await
            .unwrap_or_else(|err| errors.push(err));
    }

    if !errors.is_empty() {
        error!("Errors found: {errors:?}");
    }
    Ok(())
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
