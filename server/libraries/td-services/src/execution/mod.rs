//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use crate::execution::execution_version::{reject_if_external_uid_in_use, reject_if_running};
use crate::execution::plan::{
    assert_function_status, build_execution_plan, build_logical_function_run_inputs,
    build_logical_function_runs, build_logical_table_data_versions, build_response,
    build_transaction_map, build_transactions, fetch_relations_info,
    resolve_execution_plan_versions, update_initial_function_run_status, validate_execution_plan,
};
use ta_execution::PlanScope;
use ta_services::ctx_service::ReqCtx;
use td_database::access::LockInstant;
use td_database::access::{AccessMode, InProject, SeesProjectRows, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::graph::{GraphConfig, generate_plan_graph};
use td_execution::{ExecutionScope, TransactionBy};
use td_objects::and;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{
    ExecutionDB, ExecutionInsertDB, ExecutionRequest, ExecutionResponse,
};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::types::basic::{ExecutionId, FunctionId, TriggerType, TriggeredOn};
use td_objects::types::json::Json;
use td_queries::Queries;

pub(crate) mod cancellation;
pub(crate) mod execution_version;
pub(crate) mod list;
pub(crate) mod plan;
pub(crate) mod release;
pub(crate) mod settlement;
pub(crate) mod update_status;

/// Pre-execution trigger gates shared by the function- and version-trigger
/// services. Both gates short-circuit (they never branch to a different
/// success response):
///   1) `skip_if_running` + the function is currently running -> 425 Too Early
///   2) `external_uid` already in use by a live execution     -> 409 Conflict
async fn trigger_guards<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    request: &ExecutionRequest,
    function_id: &FunctionId,
    function_version_id: &td_objects::types::basic::FunctionVersionId,
    trigger_type: &TriggerType,
) -> Result<(), TdError> {
    // 1) Latest execution of the same trigger type, then the running gate.
    let latest_execution: Option<ExecutionDB> = conn
        .fetch_first(and!(function_version_id, trigger_type))
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
/// The caller has already authorized `DevelopProject` on the request's project,
/// before reading anything: an unauthorized request must not observe the
/// trigger gates.
pub(crate) async fn execute(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    msgs: &mut ReqCtx,
    transaction_by: &TransactionBy,
    request_context: RequestContext,
    at: LockInstant,
    request: ExecutionRequest,
    function: FunctionDBWithNames,
    trigger_type: TriggerType,
) -> Result<ExecutionResponse, TdError> {
    let function_id = function.base.base.function_id;
    // Trigger gates (short-circuit with 425 / 409).
    trigger_guards(
        conn,
        &request,
        &function_id,
        &function.base.base.id,
        &trigger_type,
    )
    .await?;
    let override_disabled_trigger = request.override_disabled_trigger;
    assert_function_status(msgs, &function, &override_disabled_trigger)?;

    let function_id = function.base.base.function_id;
    let function_only = request.function_only;
    let recursion_options =
        ExecutionScope::execution_recursion_options(function_only, override_disabled_trigger);
    let config = GraphConfig::defaults(FunctionDB::executable());
    let graph = generate_plan_graph(conn, config, None, &function_id, &recursion_options).await?;

    let execution_plan = build_execution_plan(&graph, &function)?;
    validate_execution_plan(transaction_by, &execution_plan)?;

    let execution = ExecutionInsertDB {
        id: ExecutionId::new(),
        name: request.name,
        collection_id: function.base.base.collection_id,
        function_id: function.base.base.function_id,
        function_version_id: function.base.base.id,
        triggered_on: TriggeredOn::from(at),
        triggered_by_id: request_context.user_id,
        trigger_type,
        external_uid: request.external_uid,
        recursion_options: Some(Json(recursion_options)),
    };
    conn.insert(&execution).await?;

    let transaction_map = build_transaction_map(transaction_by, &execution, &execution_plan)?;
    let transactions = build_transactions(&transaction_map, &execution)?;
    conn.chunked().insert(&transactions).await?;

    let logical_function_runs = build_logical_function_runs(
        transaction_by,
        &transaction_map,
        &execution_plan,
        &execution,
    )?;
    conn.chunked().insert(&logical_function_runs).await?;

    let logical_table_data_versions = build_logical_table_data_versions(
        transaction_by,
        &transaction_map,
        &execution_plan,
        &execution,
        &logical_function_runs,
    )?;
    conn.chunked().insert(&logical_table_data_versions).await?;

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
    conn.chunked().insert(&inputs).await?;
    // Initial-OnHold: walks the logical chain (LFR_input -> LTDV ->
    // current TDV -> upstream FR.status) to flip consumers OnHold
    // when upstream is Failed/OnHold.
    update_initial_function_run_status(conn, &execution).await?;
    let relations_info = fetch_relations_info(conn, &resolved_plan).await?;

    build_response(
        transaction_by,
        &transaction_map,
        &execution,
        &resolved_plan,
        &logical_function_runs,
        &inputs,
        relations_info,
    )
    .await
}
