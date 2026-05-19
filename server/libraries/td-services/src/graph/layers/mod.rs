//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::{DoAuthz, inter_collection_authz};
use crate::graph::layers::build::GraphBuilder;
use td_objects::StateFilter;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, TableId};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod build;

/// Knobs for [`GraphBuilder`] / [`generate_td_graph`]. State filters
/// are typed values; defaults cover the values both current callers
/// share. Implementations specify what differs (`function_state`,
/// `include_dependency_functions`) plus their auth strategy.
///
/// `historic()` picks the read mode: `false` (default) reads live
/// state from `_current` views — used by execution and live
/// inspection. `true` rebuilds graph state at the request's `AtTime`
/// via ranked CTEs — used only by plan-reading services that need the
/// graph as it existed when the plan was dispatched.
pub trait GraphConfig: Send + Sync + 'static {
    type Authz: DoAuthz;

    fn trigger_state() -> StateFilter<TriggerDBWithNames> {
        TriggerDBWithNames::active()
    }
    fn dependency_state() -> StateFilter<DependencyDBWithNames> {
        DependencyDBWithNames::active()
    }
    fn table_state() -> StateFilter<TableDB> {
        TableDB::available()
    }

    fn function_state() -> StateFilter<FunctionDB>;
    fn include_dependency_functions() -> bool;

    fn historic() -> bool {
        false
    }
}

#[derive(Debug)]
pub enum FunctionOrTable {
    Function(FunctionId),
    Table(TableId),
}

impl From<&FunctionId> for FunctionOrTable {
    fn from(function_id: &FunctionId) -> Self {
        Self::Function(*function_id)
    }
}

impl From<&TableId> for FunctionOrTable {
    fn from(table_id: &TableId) -> Self {
        Self::Table(*table_id)
    }
}

// Requires:
// - Connection (read only is enough)
// - FunctionOrTable: entrypoint for the trigger/dependency graph finder
// - Recursion options: recursion options to find the graphs
//
// `_config` is a marker argument that pins the `C: GraphConfig` choice
// at the call site without an explicit turbofish; it is otherwise
// unused (the trait's methods are static).
#[layer]
pub fn generate_td_graph<C: GraphConfig>(_config: C) {
    macro_rules! gb {
        ($func: ident) => {
            GraphBuilder::<C>::$func
        };
    }

    layers!(
        // Find triggers
        from_fn(gb!(find_recursive_trigger_graph)),
        inter_collection_authz::<_, C::Authz, TriggerDBWithNames, _>(),
        // Find dependencies. For clarity, we always show the function, even if
        // generating a table centered graph, the same way we show the tables of
        // a function centered graph.
        from_fn(gb!(main_function_id)),
        if C::include_dependency_functions() {
            layers!(
                from_fn(gb!(find_recursive_dependency_graph)),
                from_fn(gb!(unique_function_ids)),
            )
        } else {
            layers!(
                from_fn(gb!(unique_triggered_function_ids)),
                from_fn(gb!(find_triggered_functions_dependency_graph))
            )
        },
        inter_collection_authz::<_, C::Authz, DependencyDBWithNames, _>(),
        // Find output tables
        from_fn(gb!(find_functions_output_tables)),
        // Find all tables and functions
        from_fn(gb!(find_all_tables_map)),
        from_fn(gb!(find_all_functions_map)),
        // Create td graph and validate it
        from_fn(gb!(build_td_graph)),
    )
}
