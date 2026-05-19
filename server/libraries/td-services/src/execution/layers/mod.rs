//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::DO_AUTHZ;
use crate::execution::layers::execution::{
    assert_function_status, build_execution_plan, build_initial_mint_lfr_pointers,
    build_initial_mint_ltdv_pointers, build_logical_function_run_inputs,
    build_logical_function_runs, build_logical_table_data_versions, build_response,
    build_transaction_map, build_transactions, resolve_execution_plan_versions,
    update_initial_function_run_status, validate_execution_plan, with_recursion_options,
};
use crate::graph::layers::{FunctionOrTable, GraphConfig, generate_td_graph};
use td_authz::Authz;
use td_authz::tower::{AuthzOn, CollAdmin, CollExec};
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
    CollectionId, FunctionId, FunctionOnly, OverrideDisabledTrigger, TriggerType,
};
use td_objects::types::recursion::RecursionOptions;
use td_queries::{insert, insert_vec};
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
    type Authz = DO_AUTHZ;

    fn function_state() -> StateFilter<FunctionDB> {
        FunctionDB::executable()
    }

    fn include_dependency_functions() -> bool {
        false
    }
}

// Requires:
// - Transaction connection
// - AuthzContext
// - ExecutionRequest
// - TransactionBy
// - FunctionDBWithNames (trigger)
#[layer]
pub fn execute() {
    layers!(
        from_fn(With::<ExecutionRequest>::extract::<OverrideDisabledTrigger>),
        from_fn(assert_function_status),
        // check requester is coll_admin or coll_exec for the function's collection
        from_fn(With::<FunctionDBWithNames>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollExec>::check),
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
        // Execution plan response
        from_fn(build_response),
    )
}
