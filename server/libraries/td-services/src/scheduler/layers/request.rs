//
// Copyright 2025 Tabs Data Inc.
//

//! These layers should not fail for single messages errors, only for wider errors (system, connection, etc.).
//! All errors parsing or processing messages should be logged and the message should be removed from the queue.

use http::Method;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use td_common::id;
use td_common::server::{
    Callback, HttpCallback, HttpRequest, MessageAction, RequestMessagePayload, WorkerClass,
    WorkerMessageQueue, WorkerName,
};
use td_error::{ApiError, TdError, api_error, td_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function_requirement::FunctionRequirementDBWithNames;
use td_objects::dxo::function_run::FunctionRunToExecuteDB;
use td_objects::dxo::table_data_version::TableDataVersionDBWithNames;
use td_objects::dxo::worker::{WorkerDB, WorkerDBWithNames};
use td_objects::dxo::yaml::cron_trigger::v1::CronTriggerInput;
use td_objects::dxo::yaml::function_runner::v2::{
    FunctionInfo, FunctionInput, InputTable, InputTableVersion, OutputTable, OutputTableVersion,
};
use td_objects::dxo::yaml::location::Location;
use td_objects::dxo::yaml::stage_trigger::v1::StageTriggerInput;
use td_objects::dxo::yaml::{WorkerInput, cron_trigger, stage_trigger};
use td_objects::rest_urls::BASE_URL;
use td_objects::rest_urls::internal::{
    DOWNLOAD_FUNCTION_STATE, FUNCTION_VERSION_EXECUTE, FUNCTION_VERSION_EXECUTION_STATUS,
    UPDATE_FUNCTION_RUN_WORKER, UPDATE_WORKER,
};
use td_objects::sql::cte::TableQueries;
use td_objects::sql::{DaoQueries, SelectBy};
use td_objects::types::addresses::InternalServerAddresses;
use td_objects::types::basic::{
    AtTime, Enabled, EnvPrefix, HasData, WorkerConfig, WorkerId, WorkerMessageStatus, WorkerStatus,
    WorkerType,
};
use td_storage::Storage;
use td_storage::location::StorageLocation;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use url::Url;

#[td_error]
pub enum WorkerRequestError {
    #[error("Error parsing URL: {0}")]
    UrlParse(#[from] url::ParseError) = 5000,
}

pub async fn create_locked_function_runner_workers_start_request<T: WorkerMessageQueue>(
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

                // Worker id
                let worker_id = WorkerId::default();

                // Build callback
                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();
                let http_callback = HttpCallback {
                    url: {
                        let endpoint = UPDATE_FUNCTION_RUN_WORKER
                            .replace("{function_run_id}", &f.id.to_string())
                            .replace("{worker_id}", &worker_id.to_string());
                        let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&callback_url).map_err(WorkerRequestError::UrlParse)?
                    },
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

                // Staging location
                let (path, _) = storage_location
                    .builder(&f.data_location)
                    .collection(&f.collection_id)
                    .stage(&f.function_id)
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                get_states.insert(env_prefix.clone());
                let stage_location = Location::builder()
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
                    .function_version_id(&f.function_version_id)
                    .function_defined_on(f.defined_on)
                    .function(f.name.clone())
                    .function_run_id(f.id)
                    .function_bundle(bundle_location)
                    .function_stage(stage_location)
                    .triggered_on(f.triggered_on)
                    .transaction_id(&f.transaction_id)
                    .execution_id(&f.execution_id)
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
                        let (found_data_version, location) = match &req
                            .requirement_table_data_version_id
                        {
                            Some(data_version_id) => {
                                let found_data_version: TableDataVersionDBWithNames = queries
                                    .select_by::<TableDataVersionDBWithNames>(&data_version_id)?
                                    .build_query_as()
                                    .fetch_one(&mut *conn)
                                    .await
                                    .map_err(handle_sql_err)?;

                                let data_version_with_data =
                                    if matches!(found_data_version.has_data, Some(HasData::TRUE)) {
                                        Some(Cow::Borrowed(&found_data_version))
                                    } else {
                                        let res = queries
                                            .latest_table_data_version_with_data::<TableDataVersionDBWithNames>(
                                                &found_data_version.triggered_on,
                                                &found_data_version.table_id,
                                            )?
                                            .build_query_as()
                                            .fetch_optional(&mut *conn)
                                            .await
                                            .map_err(handle_sql_err)?;

                                        res.map(Cow::Owned)
                                    };

                                if let Some(data_version_with_data) =
                                    data_version_with_data.as_ref()
                                {
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
                            .execution_id(found_data_version.as_ref().map(|v| v.execution_id.clone()))
                            .transaction_id(found_data_version.as_ref().map(|v| v.transaction_id.clone()))
                            .function_run_id(found_data_version.as_ref().map(|v| v.function_run_id))
                            .triggered_on(
                                found_data_version
                                    .as_ref()
                                    .map(|v| v.triggered_on.try_into())
                                    .transpose()?,
                            )
                            .table_data_version_id(found_data_version.as_ref().map(|v| v.id.clone()))
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
                        .table_data_version_id(&table.id)
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
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::Function.as_ref().to_string(),
                    run_id: worker_id.to_string(),
                    action: MessageAction::Start,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: Some(function_input),
                };

                // Create worker message
                let message = WorkerDB {
                    id: worker_id,
                    collection_id: f.collection_id,
                    execution_id: Some(f.execution_id.clone()),
                    transaction_id: Some(f.transaction_id.clone()),
                    function_run_id: Some(f.id),
                    function_version_id: f.function_version_id.clone(),
                    message_status: WorkerMessageStatus::Locked,
                    started_on: None,
                    ended_on: None,
                    status: WorkerStatus::RunRequested,
                    worker_type: WorkerType::FunctionRunner,
                    worker_enabled: Enabled::TRUE,
                    worker_config: None,
                };

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

pub async fn create_locked_cron_trigger_workers_start_request<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(server_addresses): SrvCtx<InternalServerAddresses>,
    Input(cron_trigger_workers): Input<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = cron_trigger_workers
        .iter()
        .map(|cron_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                // Worker id
                let worker_id = cron_trigger_worker.id.to_string();

                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();

                // Build callback
                let http_callback = HttpCallback {
                    url: {
                        let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                        let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&callback_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: HashMap::default(),
                    body: true,
                };
                let callback = Callback::Http(http_callback);

                // Function version id
                let function_version_id = cron_trigger_worker.function_version_id.to_string();

                // Build trigger task
                let task = HttpRequest {
                    url: {
                        let endpoint = FUNCTION_VERSION_EXECUTE
                            .replace("{function_version_id}", &function_version_id);
                        let trigger_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&trigger_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: HashMap::from([(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    body: true,
                };

                // Build info
                let info = cron_trigger::v1::Info {
                    worker_id: cron_trigger_worker.id,
                    collection_id: cron_trigger_worker.collection_id,
                    collection: cron_trigger_worker.collection.clone(),
                    function_version_id: cron_trigger_worker.function_version_id.clone(),
                    function: cron_trigger_worker.function.clone(),
                    scheduled_on: AtTime::now(),
                    task,
                    config: WorkerConfig::serialize(&cron_trigger_worker.worker_config)?,
                };
                // Build message context
                let cron_trigger_input = CronTriggerInput { info };
                let context = WorkerInput::CronTriggerV1(Box::new(cron_trigger_input));

                // Build message payload
                // TODO ADD get_states to states
                let message_payload = RequestMessagePayload {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::CronTrigger.as_ref().to_string(),
                    run_id: worker_id.clone(),
                    action: MessageAction::Start,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: Some(context),
                };

                // Add it to the queue
                message_queue.put(worker_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}

pub async fn create_locked_cron_trigger_workers_stop_request<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(server_addresses): SrvCtx<InternalServerAddresses>,
    Input(cron_trigger_workers): Input<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = cron_trigger_workers
        .iter()
        .map(|cron_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                // Worker id
                let worker_id = cron_trigger_worker.id.to_string();

                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();

                // Build callback
                let http_callback = HttpCallback {
                    url: {
                        let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                        let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&callback_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: HashMap::default(),
                    body: true,
                };
                let callback = Callback::Http(http_callback);

                // we create a diff message ID than the worker ID for STOP
                // as it has to be a different file name for the current messaging impl
                let message_id = id::id().to_string();

                // Build message payload
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::CronTrigger.as_ref().to_string(),
                    run_id: worker_id,
                    action: MessageAction::Stop,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: None,
                };

                // Add it to the queue
                message_queue.put(message_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}

pub async fn create_locked_stage_trigger_workers_start_request<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    SrvCtx(storage): SrvCtx<Storage>,
    SrvCtx(server_addresses): SrvCtx<InternalServerAddresses>,
    Connection(connection): Connection,
    Input(stage_trigger_workers): Input<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = stage_trigger_workers
        .iter()
        .map(|stage_trigger_worker| {
            let message_queue = message_queue.clone();
            let queries = queries.clone();
            let connection = connection.clone();
            let storage = storage.clone();
            let server_addresses = server_addresses.clone();
            async move {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                // Worker id
                let worker_id = stage_trigger_worker.id.to_string();

                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();

                // Build callback
                let http_callback = HttpCallback {
                    url: {
                        let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                        let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&callback_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: HashMap::default(),
                    body: true,
                };
                let callback = Callback::Http(http_callback);

                // Function version id
                let function_version_id = stage_trigger_worker.function_version_id.to_string();

                // Build fn_trigger request
                let fn_trigger = HttpRequest {
                    url: {
                        let endpoint = FUNCTION_VERSION_EXECUTE
                            .replace("{function_version_id}", &function_version_id);
                        let trigger_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&trigger_url).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::POST,
                    headers: Default::default(),
                    body: false,
                };

                // Build fn_state request
                let fn_state = HttpRequest {
                    url: {
                        let endpoint = DOWNLOAD_FUNCTION_STATE
                            .replace("{function_version_id}", &function_version_id);
                        let fn_state = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&fn_state).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::GET,
                    headers: Default::default(),
                    body: false,
                };

                // Build fn_status request
                let fn_status = HttpRequest {
                    url: {
                        let endpoint = FUNCTION_VERSION_EXECUTION_STATUS
                            .replace("{function_version_id}", &function_version_id);
                        let fn_status = format!("http://{server_address}{BASE_URL}{endpoint}");
                        Url::parse(&fn_status).map_err(WorkerRequestError::UrlParse)?
                    },
                    method: Method::GET,
                    headers: Default::default(),
                    body: true,
                };

                // Build function location
                let f: FunctionDB = queries
                    .select_by::<FunctionDB>(&stage_trigger_worker.function_version_id)?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;

                let storage_location = StorageLocation::try_from(&f.storage_version).unwrap();
                let (path, _) = storage_location
                    .builder(&f.data_location)
                    .collection(&f.collection_id)
                    .function(&f.bundle_id)
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                let function_bundle = Location::builder()
                    .uri(external_path)
                    .env_prefix(env_prefix)
                    .build()?;

                let (path, _) = storage_location
                    .builder(&f.data_location)
                    .collection(&f.collection_id)
                    .stage(&f.function_id)
                    .build();
                let (external_path, mount_def) = storage.to_external_uri(&path)?;
                let env_prefix = EnvPrefix::try_from(&mount_def.id)?;
                let function_stage = Location::builder()
                    .uri(external_path)
                    .env_prefix(env_prefix)
                    .build()?;

                // Build info
                // TODO Urls are wrong as of now
                let info = stage_trigger::v1::Info {
                    worker_id: stage_trigger_worker.id,
                    collection_id: stage_trigger_worker.collection_id,
                    collection: stage_trigger_worker.collection.clone(),
                    function_version_id: stage_trigger_worker.function_version_id.clone(),
                    function: stage_trigger_worker.function.clone(),
                    function_defined_on: f.defined_on,
                    function_bundle,
                    function_stage,
                    scheduled_on: AtTime::now(),
                    fn_trigger,
                    fn_state,
                    config: WorkerConfig::serialize(&stage_trigger_worker.worker_config)?,
                    fn_status,
                };
                // Build message context
                let stage_trigger_input = StageTriggerInput { info };
                let context = WorkerInput::StageTriggerV1(Box::new(stage_trigger_input));

                // Build message payload
                // TODO ADD get_states to states
                let message_payload = RequestMessagePayload {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::StageTrigger.as_ref().to_string(),
                    run_id: worker_id.clone(),
                    action: MessageAction::Start,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: Some(context),
                };

                // Add it to the queue
                message_queue.put(worker_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}

pub async fn create_locked_stage_trigger_workers_stop_request<T: WorkerMessageQueue>(
    SrvCtx(message_queue): SrvCtx<T>,
    SrvCtx(server_addresses): SrvCtx<InternalServerAddresses>,
    Input(stage_trigger_workers): Input<Vec<WorkerDBWithNames>>,
) -> Result<(), TdError> {
    let futures: Vec<_> = stage_trigger_workers
        .iter()
        .map(|stage_trigger_worker| {
            let message_queue = message_queue.clone();
            let server_addresses = server_addresses.clone();
            async move {
                // This is loopback address, because this endpoint is only available to the server.
                let server_address = server_addresses.first();

                // Worker id
                let worker_id = stage_trigger_worker.id.to_string();

                // Build callback
                let endpoint = UPDATE_WORKER.replace("{worker_id}", &worker_id);
                let callback_url = format!("http://{server_address}{BASE_URL}{endpoint}");
                let callback_url = Url::parse(&callback_url).map_err(|e| {
                    api_error!(
                        ApiError::InternalError,
                        "Cannot create worker callback Url: {e}"
                    )
                })?;
                let http_callback = HttpCallback {
                    url: callback_url,
                    method: Method::POST,
                    headers: HashMap::default(),
                    body: true,
                };
                let callback = Callback::Http(http_callback);

                // we create a diff message ID than the worker ID for STOP
                // as it has to be a different file name for the current messaging impl
                let message_id = id::id().to_string();

                // Build message payload
                let message_payload = RequestMessagePayload::<serde_yaml::Value> {
                    class: WorkerClass::Ephemeral,
                    worker: WorkerName::StageTrigger.as_ref().to_string(),
                    run_id: worker_id.clone(),
                    action: MessageAction::Stop,
                    arguments: Vec::new(),
                    callback: Some(callback),
                    context: None,
                };

                // Add it to the queue
                message_queue.put(message_id, message_payload).await?;
                Ok::<_, TdError>(())
            }
        })
        .collect();
    futures::future::try_join_all(futures).await?;
    Ok(())
}
