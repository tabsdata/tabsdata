//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use http::Method;
use itertools::{Either, Itertools};
use sqlx::SqliteConnection;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;
use td_common::server::{
    Callback, HttpCallback, MessageAction, RequestMessagePayload, SupervisorMessagePayload,
    WorkerClass, WorkerMessageQueue, WorkerName,
};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::function_requirement::FunctionRequirementDBWithNames;
use td_objects::dxo::function_run::{FunctionRunDB, FunctionRunToExecuteDB, UpdateFunctionRunDB};
use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
use td_objects::dxo::worker::{UpdateWorkerMessageStatusDB, WorkerDB};
use td_objects::dxo::yaml::WorkerInput;
use td_objects::dxo::yaml::cron::v1::CronInput;
use td_objects::dxo::yaml::function_runner::v2::{
    FunctionInfo, FunctionInput, InputTable, InputTableVersion, OutputTable, OutputTableVersion,
};
use td_objects::dxo::yaml::location::Location;
use td_objects::dxo::yaml::watchdog::v1::WatchdogInput;
use td_objects::rest_urls::{BASE_URL, UPDATE_FUNCTION_RUN};
use td_objects::sql::{DaoQueries, FindBy, SelectBy, UpdateBy};
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{
    EnvPrefix, FunctionRunId, FunctionRunStatus, WorkerId, WorkerMessageStatus, WorkerStatus,
    WorkerType,
};
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use tracing::{error, trace};
use url::Url;

pub async fn create_locked_function_runner_workers<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    SrvCtx(storage): SrvCtx<Storage>,
    SrvCtx(server_addresses): SrvCtx<InternalServerAddresses>,
    Connection(connection): Connection,
    Input(function_runs): Input<Vec<FunctionRunToExecuteDB>>,
) -> Result<Vec<WorkerDB>, TdError> {
    let futures: Vec<_> = function_runs
        .iter()
        .map(|f| {
            let message_queue = message_queue.clone();
            let queries = queries.clone();
            let connection = connection.clone();
            let storage = storage.clone();
            let server_addresses = server_addresses.clone();
            async move {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                // Build callback
                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();
                let function_run_id = f.id.to_string();
                let endpoint = UPDATE_FUNCTION_RUN.replace("{function_run_id}", &function_run_id);
                let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                let callback_url = Url::parse(&callback_url).map_err(|e| {
                    api_error!(
                        ApiError::InternalError,
                        "Cannot create execution callback Url: {e}"
                    )
                })?;

                let http_callback = HttpCallback {
                    url: callback_url,
                    method: Method::POST,
                    headers: HashMap::default(),
                    body: true,
                };
                let callback = Callback::Http(http_callback);

                // Build states map
                let mut get_states = HashSet::new();

                // Build storage location
                let storage_location = StorageLocation::try_from(&f.storage_version).unwrap();

                // Bundle location
                let (path, _) = storage_location
                    .builder(&f.data_location)
                    .collection(&f.collection_id)
                    .function(&f.bundle_id)
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                get_states.insert(env_prefix.clone());
                let bundle_location = Location::builder()
                    .uri(external_path)
                    .env_prefix(env_prefix)
                    .build()?;

                // Function run content location
                let (path, _) = storage_location
                    .builder(&f.data_location)
                    .collection(&f.collection_id)
                    .transaction(&f.transaction_id)
                    .function_version(&f.function_version_id)
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                get_states.insert(env_prefix.clone());
                let function_run_location = Location::builder()
                    .uri(external_path)
                    .env_prefix(env_prefix)
                    .build()?;

                // Build message info
                let info = FunctionInfo::builder()
                    .collection_id(f.collection_id)
                    .collection(f.collection.clone())
                    .function_version_id(f.function_version_id)
                    .function(f.name.clone())
                    .function_run_id(f.id)
                    .function_bundle(bundle_location)
                    .triggered_on(f.triggered_on)
                    .transaction_id(f.transaction_id)
                    .execution_id(f.execution_id)
                    .execution_name(f.execution.clone())
                    .function_data(function_run_location)
                    .build()?;

                // Build input tables
                let requirements: Vec<FunctionRequirementDBWithNames> = queries
                    .select_by::<FunctionRequirementDBWithNames>(&f.id)?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                let mut input_tables_map = HashMap::new();
                for req in requirements.iter() {
                    if let (Some(dependency_pos), Some(input_idx)) =
                        (&req.requirement_dependency_pos, &req.requirement_input_idx)
                    {
                        let (found_data_version, location) =
                            match req.requirement_table_data_version_id {
                                Some(data_version_id) => {
                                    let found_data_version: TableDataVersionDBWithNames = queries
                                        .select_by::<TableDataVersionDBWithNames>(&data_version_id)?
                                        .build_query_as()
                                        .fetch_one(&mut *conn)
                                        .await
                                        .map_err(handle_sql_err)?;

                                    if let Some(with_data_data_version_id) =
                                        found_data_version.with_data_table_data_version_id
                                    {
                                        let data_version_with_data =
                                            if with_data_data_version_id == found_data_version.id {
                                                Cow::Borrowed(&found_data_version)
                                            } else {
                                                let res = queries
                                                    .select_by::<TableDataVersionDBWithNames>(
                                                        &with_data_data_version_id,
                                                    )?
                                                    .build_query_as()
                                                    .fetch_one(&mut *conn)
                                                    .await
                                                    .map_err(handle_sql_err)?;
                                                Cow::Owned(res)
                                            };

                                        let (path, _) = storage_location
                                            .builder(&data_version_with_data.data_location)
                                            .collection(&data_version_with_data.collection_id)
                                            .data(&data_version_with_data.id)
                                            .table(
                                                &data_version_with_data.table_id,
                                                &data_version_with_data.table_version_id,
                                            )
                                            .build();
                                        let (external_path, mount_def) =
                                            storage.to_external_uri(&path)?;
                                        let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                                        get_states.insert(env_prefix.clone());
                                        let location = Location::builder()
                                            .uri(external_path)
                                            .env_prefix(env_prefix)
                                            .build()?;
                                        (Some(found_data_version), Some(location))
                                    } else {
                                        (Some(found_data_version), None)
                                    }
                                }
                                None => (None, None),
                            };

                        let input_table = InputTableVersion::builder()
                            .name(req.requirement_table.clone())
                            .collection_id(req.collection_id)
                            .collection(req.collection.clone())
                            .table_id(req.requirement_table_id)
                            .table_version_id(req.requirement_table_version_id)
                            .execution_id(found_data_version.as_ref().map(|v| v.execution_id))
                            .transaction_id(found_data_version.as_ref().map(|v| v.transaction_id))
                            .function_run_id(found_data_version.as_ref().map(|v| v.function_run_id))
                            .triggered_on(
                                found_data_version
                                    .as_ref()
                                    .map(|v| v.triggered_on.try_into())
                                    .transpose()?,
                            )
                            .table_data_version_id(found_data_version.as_ref().map(|v| v.id))
                            .location(location)
                            .input_idx(input_idx)
                            .table_pos(dependency_pos)
                            .version_pos(req.requirement_version_pos)
                            .build()?;

                        input_tables_map
                            .entry(**dependency_pos)
                            .or_insert_with(Vec::new)
                            .push(input_table);
                    }
                }

                // Build output tables
                let tables: Vec<TableDataVersionDBWithNames> = queries
                    .select_by::<TableDataVersionDBWithNames>(&f.id)?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                let mut output_tables_map = HashMap::new();
                for table in tables.iter() {
                    let (path, _) = StorageLocation::current()
                        .builder(&f.data_location)
                        .collection(&table.collection_id)
                        .data(&table.id)
                        .table(&table.table_id, &table.table_version_id)
                        .build();
                    let (external_path, mount_def) = storage.to_external_uri(&path)?;
                    let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                    get_states.insert(env_prefix.clone());
                    let location = Location::builder()
                        .uri(external_path)
                        .env_prefix(env_prefix)
                        .build()?;

                    let input_table = OutputTableVersion::builder()
                        .name(table.name.clone())
                        .collection_id(table.collection_id)
                        .collection(table.collection.clone())
                        .table_id(table.table_id)
                        .table_version_id(table.table_version_id)
                        .table_data_version_id(table.id)
                        .location(location)
                        .table_pos(table.function_param_pos)
                        .build()?;

                    output_tables_map.insert(*table.function_param_pos, input_table);
                }

                // Build system/user tables
                fn partition_and_sort<T, U>(
                    map: HashMap<i32, T>,
                    transform: impl Fn(T) -> U,
                ) -> (Vec<U>, Vec<U>) {
                    let mut system_tables = Vec::new();
                    let mut user_tables = Vec::new();

                    let mut entries: Vec<_> = map.into_iter().collect();
                    entries.sort_by_key(|(i, _)| *i);

                    for (i, value) in entries {
                        let transformed = transform(value);
                        if i < 0 {
                            system_tables.push(transformed);
                        } else {
                            user_tables.push(transformed);
                        }
                    }

                    (system_tables, user_tables)
                }
                let (system_input, input) = partition_and_sort(input_tables_map, |mut tables| {
                    tables.sort_by_key(|t| *t.version_pos);
                    InputTable::new(tables)
                });
                let (system_output, output) =
                    partition_and_sort(output_tables_map, OutputTable::Table);

                // Build message context
                let function_input_v2 = FunctionInput::builder()
                    .info(info)
                    .system_input(system_input)
                    .input(input)
                    .system_output(system_output)
                    .output(output)
                    .build()?;
                let function_input = WorkerInput::FunctionV2(Box::new(function_input_v2));

                // Build message payload
                // TODO ADD get_states to states
                let message_payload = RequestMessagePayload {
                    class: WorkerClass::EPHEMERAL,
                    worker: WorkerName::FUNCTION.as_ref().to_string(),
                    action: MessageAction::Start,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: Some(function_input),
                };

                // Create worker message
                let message = WorkerDB::builder()
                    .collection_id(f.collection_id)
                    .execution_id(f.execution_id)
                    .transaction_id(f.transaction_id)
                    .function_run_id(f.id)
                    .function_version_id(f.function_version_id)
                    .message_status(WorkerMessageStatus::Locked)
                    .status(WorkerStatus::RunRequested)
                    .worker_type(WorkerType::FunctionRunner)
                    .worker_config(None)
                    .build()?;

                // Add it to the queue
                message_queue
                    .put(message.id.to_string(), message_payload)
                    .await?;
                Ok::<_, TdError>(message)
            }
        })
        .collect();

    // Collect new versions asynchronously
    let res = futures::future::try_join_all(futures).await?;

    Ok(res)
}

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
        _invalid_requests,
        _invalid_messages
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
        rollback_queue(message_queue, w_id, f_id)
            .await
            .unwrap_or_else(|e| errors.push(e));
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
                    rollback_query(queries, conn, w_id, f_id)
                        .await
                        .unwrap_or_else(|e| errors.push(e));
                    rollback_queue(message_queue, w_id, f_id)
                        .await
                        .unwrap_or_else(|e| errors.push(e));
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
        rollback_query(queries, &mut *conn, w_id, f_id)
            .await
            .unwrap_or_else(|e| errors.push(e));
        rollback_queue(message_queue, w_id, f_id)
            .await
            .unwrap_or_else(|e| errors.push(e));
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
    _message_queue: &T,
    _messages: HashMap<WorkerId, CronInput>,
) -> Result<(), TdError> {
    Ok(())
}

pub async fn unlock_watchdog_workers<T: WorkerMessageQueue>(
    _queries: &DaoQueries,
    _conn: &mut SqliteConnection,
    _message_queue: &T,
    _messages: HashMap<WorkerId, WatchdogInput>,
) -> Result<(), TdError> {
    Ok(())
}

async fn rollback_query(
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

async fn rollback_queue<T: WorkerMessageQueue>(
    message_queue: &T,
    message_id: &WorkerId,
    function_run_id: &FunctionRunId,
) -> Result<(), TdError> {
    match message_queue.rollback(&message_id.to_string()).await {
        Ok(_) => {
            error!(
                "Rolled back locked message [{}] for function run [{}] in queue",
                message_id, function_run_id
            );
        }
        Err(e) => {
            error!(
                "Failed to rollback locked message [{}] for function run [{}]: {}",
                message_id, function_run_id, e
            );
        }
    }

    Ok(())
}
