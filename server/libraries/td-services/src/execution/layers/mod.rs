//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::execution::{
    assert_function_status, build_execution_plan, build_initial_mint_lfr_pointers,
    build_initial_mint_ltdv_pointers, build_logical_function_run_inputs,
    build_logical_function_runs, build_logical_table_data_versions, build_response,
    build_transaction_map, build_transactions, resolve_execution_plan_versions,
    update_initial_function_run_status, validate_execution_plan, with_recursion_options,
};
use crate::execution::layers::execution_version::{
    dedup_lock, reject_if_external_uid_in_use, reject_if_running,
};
use crate::graph::layers::{FunctionOrTable, GraphConfig, generate_td_graph};
use crate::scheduler::layers::request::notify_scheduler;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_objects::And;
use td_objects::StateFilter;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionDBBuilder, ExecutionRequest};
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunPointerDB,
    LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
};
use td_objects::dxo::transaction::{TransactionDB, TransactionDBBuilder};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    ExternalUid, FunctionId, FunctionOnly, FunctionVersionId, OverrideDisabledTrigger,
    SkipIfRunning, TriggerType,
};
use td_objects::types::recursion::RecursionOptions;
use td_queries::{By, insert, insert_vec};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub(crate) mod execution;
pub(crate) mod execution_version;
pub(crate) mod list;
pub(crate) mod read;
pub(crate) mod redirect_at_finish;
pub(crate) mod runtime_info;
pub(crate) mod update_status;

pub struct GraphOpts;
impl GraphConfig for GraphOpts {
    fn function_state() -> StateFilter<FunctionDB> {
        FunctionDB::executable()
    }

    fn include_dependency_functions() -> bool {
        false
    }
}

// Pre-execution trigger gates shared by the function- and version-trigger services.
// Both gates short-circuit (they never branch to a different success response):
//   1) `skip_if_running` + the function is currently running -> 425 Too Early
//   2) `external_uid` already in use by a live execution     -> 409 Conflict
// Evaluated in that order; on the happy path the pipeline falls through to `execute()`.
// Requires `ExecutionRequest`, `FunctionVersionId`, `FunctionId` and `TriggerType` in scope.
#[layer]
pub fn trigger_guards() {
    layers!(
        // 1) Latest execution of the same trigger type, then the running gate.
        from_fn(By::<And![FunctionVersionId, TriggerType]>::fetch_optional::<ExecutionDB>),
        from_fn(With::<ExecutionRequest>::extract::<SkipIfRunning>),
        from_fn(reject_if_running),
        // 2) external_uid conflict gate (advisory-locked to close the insert race).
        from_fn(With::<ExecutionRequest>::extract::<Option<ExternalUid>>),
        from_fn(dedup_lock),
        from_fn(reject_if_external_uid_in_use),
    )
}

// The full trigger flow shared by both trigger services: run the trigger gates
// (`trigger_guards`: 425 if already running, 409 on `external_uid` conflict), then authorize,
// plan, and insert the execution + its transactions and (logical) function runs, then notify
// the scheduler of the new runs. Yields the `ExecutionResponse`. Callers supply only the prelude
// that resolves the trigger function and trigger type. Requires:
// - Transaction connection
// - AuthzContext
// - SchedulerNotify
// - ExecutionRequest
// - TransactionBy
// - FunctionDBWithNames (trigger), FunctionId, FunctionVersionId, TriggerType
#[layer]
pub fn execute() {
    layers!(
        // Trigger gates (short-circuit with 425 / 409).
        trigger_guards(),
        from_fn(With::<ExecutionRequest>::extract::<OverrideDisabledTrigger>),
        from_fn(assert_function_status),
        // check requester can develop the current project (manage or develop)
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        // Get recursion options
        from_fn(With::<FunctionDBWithNames>::extract::<FunctionId>),
        from_fn(With::<FunctionId>::convert_to::<FunctionOrTable>),
        from_fn(With::<ExecutionRequest>::extract::<FunctionOnly>),
        from_fn(with_recursion_options),
        // Find td graph
        generate_td_graph(GraphOpts),
        // Build execution plan
        from_fn(build_execution_plan),
        from_fn(validate_execution_plan),
        // Build execution
        from_fn(With::<FunctionDBWithNames>::try_convert_to::<ExecutionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<ExecutionRequest>::update::<ExecutionDBBuilder, _>),
        from_fn(With::<TriggerType>::set::<ExecutionDBBuilder>),
        from_fn(With::<RecursionOptions>::set::<ExecutionDBBuilder>),
        from_fn(With::<ExecutionDBBuilder>::build::<ExecutionDB>),
        from_fn(insert::<ExecutionDB>),
        // Build transactions
        from_fn(build_transaction_map),
        from_fn(With::<ExecutionDB>::try_convert_to::<TransactionDBBuilder, _>),
        from_fn(build_transactions),
        from_fn(insert_vec::<TransactionDB>),
        // Logical LFRs — identity rows + InitialMint pointer events.
        from_fn(build_logical_function_runs),
        from_fn(insert_vec::<LogicalFunctionRunDB>),
        from_fn(build_initial_mint_lfr_pointers),
        from_fn(insert_vec::<LogicalFunctionRunPointerDB>),
        // Logical LTDVs — identity rows + InitialMint pointer events.
        from_fn(build_logical_table_data_versions),
        from_fn(insert_vec::<LogicalTableDataVersionDB>),
        from_fn(build_initial_mint_ltdv_pointers),
        from_fn(insert_vec::<LogicalTableDataVersionPointerDB>),
        // Resolve plan-build response (used by build_response).
        from_fn(resolve_execution_plan_versions),
        // Logical dependency-binding rows.
        from_fn(build_logical_function_run_inputs),
        from_fn(insert_vec::<LogicalFunctionRunInputDB>),
        // Initial-OnHold: walks the logical chain (LFR_input → LTDV →
        // pointer → TDV → upstream FR.status) to flip consumers OnHold
        // when upstream is Failed/OnHold.
        from_fn(update_initial_function_run_status),
        // Execution plan response — the service's `ExecutionResponse`.
        from_fn(build_response),
        // Notify scheduler to pick up the newly created function runs.
        from_fn(notify_scheduler),
    )
}
