//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use crate::execution::execution_version::{reject_if_external_uid_in_use, reject_if_running};
use crate::execution::plan::{
    assert_function_status, build_execution_plan, build_initial_mint_lfr_pointers,
    build_initial_mint_ltdv_pointers, build_logical_function_run_inputs,
    build_logical_function_runs, build_logical_table_data_versions, build_response,
    build_transaction_map, build_transactions, resolve_execution_plan_versions,
    update_initial_function_run_status, validate_execution_plan, with_recursion_options,
};
use crate::graph::{FunctionOrTable, GraphConfig, generate_td_graph};
use ta_services::ctx_service::ReqCtx;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::TransactionBy;
use td_objects::and;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionRequest, ExecutionResponse};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB,
    LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{ExecutionId, FunctionId, ProjectId, TriggerType, TriggeredOn};
use td_objects::types::json::Json;
use td_queries::{ChunkedQueries, LiveEntry};

pub(crate) mod execution_version;
pub(crate) mod list;
pub(crate) mod plan;
pub(crate) mod read;
pub(crate) mod redirect_at_finish;
pub(crate) mod runtime_info;
pub(crate) mod update_status;

/// Pre-execution trigger gates shared by the function- and version-trigger
/// services. Both gates short-circuit (they never branch to a different
/// success response):
///   1) `skip_if_running` + the function is currently running -> 425 Too Early
///   2) `external_uid` already in use by a live execution     -> 409 Conflict
async fn trigger_guards(
    conn: &mut DomainConnection<Apiserver>,
    request: &ExecutionRequest,
    function_id: &FunctionId,
    function_version_id: &td_objects::types::basic::FunctionVersionId,
    trigger_type: &TriggerType,
) -> Result<(), TdError> {
    // 1) Latest execution of the same trigger type, then the running gate.
    let latest_execution: Option<ExecutionDB> = conn
        .live()
        .fetch_optional(and!(function_version_id, trigger_type))
        .await?;
    let skip_if_running = request.skip_if_running;
    reject_if_running(
        conn,
        function_version_id,
        &skip_if_running,
        &latest_execution,
    )
    .await?;
    // 2) external_uid conflict gate.
    let external_uid = request.external_uid.clone();
    reject_if_external_uid_in_use(conn, function_id, &external_uid).await?;
    Ok(())
}

/// The full trigger flow shared by both trigger services: run the trigger
/// gates (425 if already running, 409 on `external_uid` conflict), then
/// authorize, plan, and insert the execution + its transactions and (logical)
/// function runs. Yields the `ExecutionResponse`; the caller commits and then
/// notifies the scheduler of the new runs.
#[allow(clippy::too_many_arguments)]
/// The caller has already authorized `DevelopProject` on `project_id`, before
/// reading anything: an unauthorized request must not observe the trigger gates.
pub(crate) async fn execute(
    conn: &mut DomainConnection<Apiserver>,
    msgs: &mut ReqCtx,
    transaction_by: &TransactionBy,
    request_context: RequestContext,
    project_id: ProjectId,
    request: ExecutionRequest,
    function: FunctionDBWithNames,
    trigger_type: TriggerType,
) -> Result<ExecutionResponse, TdError> {
    let function_id = function.base.function_id;
    // Trigger gates (short-circuit with 425 / 409).
    trigger_guards(
        conn,
        &request,
        &function_id,
        &function.base.id,
        &trigger_type,
    )
    .await?;
    let override_disabled_trigger = request.override_disabled_trigger;
    assert_function_status(msgs, &function, &override_disabled_trigger).await?;

    let entrypoint = FunctionOrTable::from(&function_id);
    let function_only = request.function_only;
    let recursion_options =
        with_recursion_options(&function_only, &override_disabled_trigger).await?;
    let at = request_context.time;
    let config = GraphConfig::defaults(FunctionDB::executable());
    let graph = generate_td_graph(conn, config, &at, &entrypoint, &recursion_options).await?;

    let execution_plan = build_execution_plan(&graph, &function).await?;
    validate_execution_plan(transaction_by, &execution_plan)?;

    let execution = ExecutionDB {
        id: ExecutionId::new(),
        name: request.name,
        collection_id: function.base.collection_id,
        function_id: function.base.function_id,
        function_version_id: function.base.id,
        triggered_on: TriggeredOn::try_from(&request_context.time)?,
        triggered_by_id: request_context.user_id,
        trigger_type,
        external_uid: request.external_uid,
        recursion_options: Some(Json(recursion_options)),
    };
    conn.live().insert(&execution).await?;

    let transaction_map =
        build_transaction_map(transaction_by, &execution, &execution_plan).await?;
    let transactions = build_transactions(&transaction_map, &execution)?;
    conn.insert_vec::<TransactionDB>(&transactions).await?;

    let logical_function_runs = build_logical_function_runs(
        transaction_by,
        &transaction_map,
        &execution_plan,
        &execution,
        &project_id,
    )
    .await?;
    conn.insert_vec::<LogicalFunctionRunDB>(&logical_function_runs)
        .await?;
    let lfr_pointers = build_initial_mint_lfr_pointers(&logical_function_runs)?;
    conn.insert_vec::<LogicalFunctionRunPointerDB>(&lfr_pointers)
        .await?;

    let logical_table_data_versions = build_logical_table_data_versions(
        transaction_by,
        &transaction_map,
        &execution_plan,
        &execution,
        &logical_function_runs,
    )?;
    conn.insert_vec::<LogicalTableDataVersionDB>(&logical_table_data_versions)
        .await?;
    let ltdv_pointers = build_initial_mint_ltdv_pointers(&logical_table_data_versions)?;
    conn.insert_vec::<LogicalTableDataVersionPointerDB>(&ltdv_pointers)
        .await?;

    let resolved_plan = resolve_execution_plan_versions(conn, &execution, &execution_plan).await?;
    let inputs = build_logical_function_run_inputs(
        conn,
        transaction_by,
        &transaction_map,
        &execution_plan,
        &execution,
        &logical_function_runs,
    )
    .await?;
    conn.insert_vec::<LogicalFunctionRunInputDB>(&inputs)
        .await?;
    // Initial-OnHold: walks the logical chain (LFR_input -> LTDV ->
    // pointer -> TDV -> upstream FR.status) to flip consumers OnHold
    // when upstream is Failed/OnHold.
    update_initial_function_run_status(conn, &execution, &at).await?;

    build_response(
        transaction_by,
        &transaction_map,
        &execution,
        &resolved_plan,
        &logical_function_runs,
        &inputs,
    )
    .await
}
