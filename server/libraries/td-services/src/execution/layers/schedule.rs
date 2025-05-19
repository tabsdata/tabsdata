//
// Copyright 2025 Tabs Data Inc.
//

use http::Method;
use itertools::{Either, Itertools};
use sqlx::SqliteConnection;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use td_common::server::WorkerName::FUNCTION;
use td_common::server::{
    Callback, HttpCallbackBuilder, MessageAction, RequestMessagePayload,
    RequestMessagePayloadBuilder, SupervisorMessage, SupervisorMessagePayload, WorkerClass,
    WorkerMessageQueue,
};
use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::location2::StorageLocation;
use td_objects::rest_urls::{BASE_URL, UPDATE_FUNCTION_RUN};
use td_objects::sql::cte::CteQueries;
use td_objects::sql::{DerefQueries, FindBy, SelectBy, UpdateBy};
use td_objects::types::basic::{FunctionRunId, HasData, WorkerMessageId};
use td_objects::types::execution::{
    ExecutableFunctionRunDB, FunctionRequirementDBWithNames, FunctionRunDB, FunctionRunStatus,
    TableDataVersionDBWithNames, UpdateFunctionRunDB, UpdateWorkerMessageDB, WorkerMessageDB,
    WorkerMessageStatus,
};
use td_objects::types::worker::v2::{
    FunctionInfoV2, FunctionInputV2, InputTable, InputTableVersion, OutputTable, OutputTableVersion,
};
use td_objects::types::worker::{EnvPrefix, FunctionInput, Location};
use td_storage::Storage;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use tracing::{error, trace};
use url::Url;

#[td_error]
pub enum ScheduleError {
    #[error("Cannot create execution callback Url: {0}")]
    CallbackUrlParseError(#[from] url::ParseError) = 5000,
    #[error("Invalid supervisor request message payload: {0}")]
    InvalidRequestMessagePayload(String) = 5001,
    #[error("Missing supervisor request message context")]
    MissingRequestContext = 5002,
}

pub async fn create_locked_worker_messages<Q: DerefQueries, T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<Q>,
    SrvCtx(storage): SrvCtx<Storage>,
    SrvCtx(server_url): SrvCtx<SocketAddr>,
    Connection(connection): Connection,
    Input(function_runs): Input<Vec<ExecutableFunctionRunDB>>,
) -> Result<Vec<WorkerMessageDB>, TdError> {
    let futures: Vec<_> = function_runs
        .iter()
        .map(|f| {
            let message_queue = message_queue.clone();
            let queries = queries.clone();
            let connection = connection.clone();
            let storage = storage.clone();
            let server_url = server_url.clone();
            async move {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                // Build callback
                // This is loopback address, because this endpoint is only available to the server.
                let function_run_id = f.id().to_string();
                let endpoint = UPDATE_FUNCTION_RUN.replace("{function_run_id}", &function_run_id);
                let callback_url = format!(
                    "http://127.0.0.1:{}{}{}",
                    server_url.port(),
                    BASE_URL,
                    endpoint
                );
                let callback_url =
                    Url::parse(&callback_url).map_err(ScheduleError::CallbackUrlParseError)?;

                let http_callback = HttpCallbackBuilder::default()
                    .url(callback_url)
                    .method(Method::POST)
                    .headers(HashMap::default())
                    .body(true)
                    .build()
                    .unwrap();
                let callback = Callback::Http(http_callback);

                // Build states map
                let mut get_states = HashSet::new();

                // Build storage location
                let storage_location = StorageLocation::try_from(f.storage_version()).unwrap();

                // Build message info
                let (path, _) = storage_location
                    .builder(f.data_location())
                    .collection(f.collection_id())
                    .function(f.bundle_id())
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(mount_def.id())?;
                get_states.insert(env_prefix.clone());
                let location = Location::builder()
                    .uri(external_path)
                    .env_prefix(env_prefix)
                    .build()?;

                let info = FunctionInfoV2::builder()
                    .collection_id(f.collection_id())
                    .collection(f.collection())
                    .function_version_id(f.function_version_id())
                    .function(f.name())
                    .function_run_id(f.id())
                    .function_bundle(location)
                    .try_triggered_on(f.triggered_on().timestamp_millis())?
                    .transaction_id(f.transaction_id())
                    .execution_id(f.execution_id())
                    .execution_name(f.execution().clone())
                    .build()?;

                // Build input tables
                let requirements: Vec<FunctionRequirementDBWithNames> = queries
                    .select_by::<FunctionRequirementDBWithNames>(&(f.id()))?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                let mut input_tables_map = HashMap::new();
                for req in requirements.iter() {
                    if let (Some(dependency_pos), Some(input_idx)) = (
                        req.requirement_dependency_pos(),
                        req.requirement_input_idx(),
                    ) {
                        let location = match req.requirement_table_data_version_id() {
                            Some(data_version_id) => {
                                let found_data_version: TableDataVersionDBWithNames = queries
                                    .select_by::<TableDataVersionDBWithNames>(&data_version_id)?
                                    .build_query_as()
                                    .fetch_one(&mut *conn)
                                    .await
                                    .map_err(handle_sql_err)?;

                                let data_version = match found_data_version.has_data() {
                                    // We can use the data version if it has data, otherwise we need to
                                    // find the first version with data, in the same table version id, if any.
                                    Some(has_data) if **has_data => Some(found_data_version),
                                    _ => queries
                                        .select_versions_at::<TableDataVersionDBWithNames>(
                                            Some(f.triggered_on()),
                                            Some(&[&HasData::from(true)]),
                                            &(req.requirement_table_version_id()),
                                        )?
                                        .build_query_as()
                                        .fetch_optional(&mut *conn)
                                        .await
                                        .map_err(handle_sql_err)?,
                                };

                                if let Some(data_version) = data_version {
                                    let (path, _) = storage_location
                                        .builder(f.data_location())
                                        .collection(req.collection_id())
                                        .data(data_version.id())
                                        .build();
                                    let (external_path, mount_def) =
                                        storage.to_external_uri(&path)?;
                                    let env_prefix = EnvPrefix::try_from(mount_def.id())?;
                                    get_states.insert(env_prefix.clone());
                                    let location = Location::builder()
                                        .uri(external_path)
                                        .env_prefix(env_prefix)
                                        .build()?;
                                    Some(location)
                                } else {
                                    None
                                }
                            }
                            None => None,
                        };

                        let input_table = InputTableVersion::builder()
                            .name(req.requirement_table())
                            .collection_id(req.collection_id())
                            .collection(req.collection())
                            .table_id(req.requirement_table_id())
                            .table_version_id(req.requirement_table_version_id())
                            .table_data_version_id(*req.requirement_table_data_version_id())
                            .location(location)
                            .input_idx(input_idx)
                            .table_pos(dependency_pos)
                            .version_pos(req.requirement_version_pos())
                            .build()?;

                        input_tables_map
                            .entry(**dependency_pos)
                            .or_insert_with(Vec::new)
                            .push(input_table);
                    }
                }

                // Build output tables
                let tables: Vec<TableDataVersionDBWithNames> = queries
                    .select_by::<TableDataVersionDBWithNames>(&(f.id()))?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                let mut output_tables_map = HashMap::new();
                for table in tables.iter() {
                    let (path, _) = StorageLocation::current()
                        .builder(f.data_location())
                        .collection(table.collection_id())
                        .data(table.id())
                        .build();
                    let (external_path, mount_def) = storage.to_external_uri(&path)?;
                    let env_prefix = EnvPrefix::try_from(mount_def.id())?;
                    get_states.insert(env_prefix.clone());
                    let location = Location::builder()
                        .uri(external_path)
                        .env_prefix(env_prefix)
                        .build()?;

                    let input_table = OutputTableVersion::builder()
                        .name(table.name())
                        .collection_id(table.collection_id())
                        .collection(table.collection())
                        .table_id(table.table_id())
                        .table_version_id(table.table_version_id())
                        .table_data_version_id(table.id())
                        .location(location)
                        .table_pos(table.function_param_pos())
                        .build()?;

                    output_tables_map.insert(**table.function_param_pos(), input_table);
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
                    tables.sort_by_key(|t| **t.version_pos());
                    InputTable::new(tables)
                });
                let (system_output, output) =
                    partition_and_sort(output_tables_map, OutputTable::Table);

                // Build message context
                let function_input_v2 = FunctionInputV2::builder()
                    .info(info)
                    .system_input(system_input)
                    .input(input)
                    .system_output(system_output)
                    .output(output)
                    .build()?;
                let function_input = FunctionInput::V2(function_input_v2);

                // Build message payload
                // TODO ADD get_states to states
                let message_payload: RequestMessagePayload<FunctionInput> =
                    RequestMessagePayloadBuilder::default()
                        .class(WorkerClass::EPHEMERAL)
                        .worker(FUNCTION.as_ref())
                        .action(MessageAction::Start)
                        .arguments(vec![])
                        .callback(callback)
                        .context(function_input)
                        .build()
                        .unwrap();

                // Create worker message
                let message = WorkerMessageDB::builder()
                    .collection_id(f.collection_id())
                    .execution_id(f.execution_id())
                    .transaction_id(f.transaction_id())
                    .function_run_id(f.id())
                    .function_version_id(f.function_version_id())
                    .status(WorkerMessageStatus::Locked)
                    .build()?;

                // Add it to the queue
                message_queue
                    .put(message.id().to_string(), message_payload)
                    .await?;
                Ok::<_, TdError>(message)
            }
        })
        .collect();

    // Collect new versions asynchronously
    let res = futures::future::try_join_all(futures).await?;

    Ok(res)
}

// These layer should not fail for single messages errors, only for wider errors (system, connection, etc.).
// All errors parsing or processing messages should be logged and the message should be removed from the queue.
pub async fn unlock_worker_messages<Q: DerefQueries, T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
) -> Result<(), TdError> {
    let messages = message_queue.locked_messages().await;

    // Filter all invalid messages
    let function_run_ids = messages
        .iter()
        .filter_map(|m| {
            let message =
                if let SupervisorMessagePayload::SupervisorRequestMessagePayload(message) =
                    m.payload()
                {
                    Some(message)
                } else {
                    error!(
                    "Scheduled locked message [{}] is not a SupervisorRequestMessagePayload: {:?}",
                    m.id(),
                    m.payload()
                );
                    None
                }?;

            let function_input = message.context().as_ref().or_else(|| {
                error!(
                    "Scheduled locked message [{}] has no context: {:?}",
                    m.id(),
                    m.payload()
                );
                None
            })?;

            let function_run_id = match function_input {
                FunctionInput::V0(_) => unreachable!(), // TODO
                FunctionInput::V1(_) => unreachable!(), // TODO
                FunctionInput::V2(context) => Some(context.info().function_run_id()),
            }?;

            Some((function_run_id, m))
        })
        .collect::<HashMap<_, _>>();

    // Find all associated function runs
    let found_function_runs: Vec<FunctionRunDB> = {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

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
        found_function_runs.iter().map(|f| (f.id(), f)).collect();

    // Rollback all message for function runs that are not found
    let not_found_runs = function_run_ids
        .iter()
        .filter(|(id, _)| !found_function_runs.contains_key(*id))
        .collect::<Vec<_>>();
    let not_found_runs_futures = not_found_runs.into_iter().map(|(id, _)| {
        let message_queue = message_queue.clone();
        let message = function_run_ids[id];
        async move {
            trace!(
                "Function run [{}] is not found for locked message [{}] in queue",
                message.id(),
                id
            );
            rollback_queue(message_queue.deref(), message, id).await?;

            Ok::<_, TdError>(())
        }
    });

    // Split in valid and invalid function runs
    let (valid_runs, invalid_runs): (Vec<_>, Vec<_>) =
        found_function_runs.into_iter().partition_map(|(id, f)| {
            if matches!(f.status(), FunctionRunStatus::RunRequested) {
                Either::Left(id)
            } else {
                let message = function_run_ids[f.id()];
                error!(
                    "Scheduled locked message [{}] for function run [{}] is not in RR state: {}",
                    message.id(),
                    f.id(),
                    f.status()
                );
                Either::Right(id)
            }
        });

    // Commit all valid function runs
    let valid_runs_futures = valid_runs.into_iter().map(|id| {
        let message_queue = message_queue.clone();
        let queries = queries.clone();
        let connection = connection.clone();
        let message = function_run_ids[id];
        async move {
            let mut conn = connection.lock().await;
            let conn = conn.get_mut_connection()?;

            let update = UpdateWorkerMessageDB::unlocked()?;
            // TODO the queue should use this typed too
            let message_id = WorkerMessageId::try_from(message.id().as_str())?;
            match queries
                .update_by::<_, WorkerMessageDB>(&update, &(&message_id))?
                .build()
                .execute(&mut *conn)
                .await
            {
                Ok(_) => match message_queue.commit(message.id()).await {
                    Ok(_) => {
                        trace!(
                            "Scheduled locked message [{}] for function run [{}] is committed",
                            message.id(),
                            id
                        );
                    }
                    Err(e) => {
                        error!(
                            "Scheduled locked message [{}] for function run [{}] commit failed: {}",
                            message.id(),
                            id,
                            e
                        );
                        rollback_query(queries.deref(), conn, message, id).await?;
                        rollback_queue(message_queue.deref(), message, id).await?;
                    }
                },
                Err(e) => {
                    error!(
                        "Scheduled locked message [{}] for function run [{}] commit failed: {}",
                        message.id(),
                        id,
                        e
                    );
                }
            }

            Ok::<_, TdError>(())
        }
    });

    // Rollback all invalid function runs
    let invalid_runs_futures = invalid_runs.into_iter().map(|id| {
        let message_queue = message_queue.clone();
        let queries = queries.clone();
        let connection = connection.clone();
        let message = function_run_ids[id];
        async move {
            let mut conn = connection.lock().await;
            let conn = conn.get_mut_connection()?;

            rollback_query(queries.deref(), conn, message, id).await?;
            rollback_queue(message_queue.deref(), message, id).await?;

            Ok::<_, TdError>(())
        }
    });

    let not_found_res = futures::future::try_join_all(not_found_runs_futures).await;
    let valid_res = futures::future::try_join_all(valid_runs_futures).await;
    let invalid_res = futures::future::try_join_all(invalid_runs_futures).await;
    // We try to run all the futures and fail at the end.
    let _ = valid_res.and(invalid_res).and(not_found_res)?;

    Ok(())
}

async fn rollback_query<Q: DerefQueries>(
    queries: &Q,
    conn: &mut SqliteConnection,
    message: &SupervisorMessage<FunctionInput>,
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
                "Rolled back status to scheduled for locked message [{}] for function run [{}] in DB",
                message.id(),
                function_run_id
            );
        }
        Err(e) => {
            error!(
                "Failed to rollback status to scheduled for locked message [{}] for function run [{}]: {}",
                message.id(),
                function_run_id,
                e
            );
        }
    }

    Ok(())
}

async fn rollback_queue<T: WorkerMessageQueue>(
    message_queue: &T,
    message: &SupervisorMessage<FunctionInput>,
    function_run_id: &FunctionRunId,
) -> Result<(), TdError> {
    match message_queue.rollback(message.id()).await {
        Ok(_) => {
            error!(
                "Rolled back locked message [{}] for function run [{}] in queue",
                message.id(),
                function_run_id
            );
        }
        Err(e) => {
            error!(
                "Failed to rollback locked message [{}] for function run [{}]: {}",
                message.id(),
                function_run_id,
                e
            );
        }
    }

    Ok(())
}
