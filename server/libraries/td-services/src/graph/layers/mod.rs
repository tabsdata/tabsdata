//
// Copyright 2025 Tabs Data Inc.
//

#![allow(private_bounds, non_camel_case_types)]

use crate::do_authz::{DoAuthz, inter_collection_authz};
use crate::graph::layers::build::{
    build_td_graph, find_all_functions_map, find_all_tables_map, find_functions_output_tables,
    find_recursive_dependency_graph, find_recursive_trigger_graph,
    find_triggered_functions_dependency_graph, main_function_id, unique_function_ids,
    unique_triggered_function_ids,
};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, TableId};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod build;

pub trait WithDependencies {
    fn all_deps() -> bool;
}

pub struct ALL_DEPS;
impl WithDependencies for ALL_DEPS {
    fn all_deps() -> bool {
        true
    }
}

pub struct TRIGGER_ONLY_DEPS;
impl WithDependencies for TRIGGER_ONLY_DEPS {
    fn all_deps() -> bool {
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
// - DaoQueries
// - FunctionOrTable: entrypoint for the trigger/dependency graph finder
// - Recursion options: recursion options to find the graphs
// Allows inter-collection authz checks is A = DO_AUTHZ
// Generates all dependencies if D = ALL_DEPS, or only triggered functions dependencies if D = TRIGGER_ONLY_DEPS
#[layer]
pub fn generate_td_graph<A: DoAuthz, D: WithDependencies>() {
    layers!(
        // Find triggers
        from_fn(find_recursive_trigger_graph),
        inter_collection_authz::<_, A, TriggerDBWithNames, _>(),
        // Find dependencies. For clarity, we always show the function, even if
        // generating a table centered graph, the same way we show the tables of
        // a function centered graph.
        from_fn(main_function_id),
        if D::all_deps() {
            layers!(
                from_fn(find_recursive_dependency_graph),
                from_fn(unique_function_ids),
            )
        } else {
            layers!(
                from_fn(unique_triggered_function_ids),
                from_fn(find_triggered_functions_dependency_graph)
            )
        },
        inter_collection_authz::<_, A, DependencyDBWithNames, _>(),
        // Find output tables
        from_fn(find_functions_output_tables),
        // Find all tables and functions
        from_fn(find_all_tables_map),
        from_fn(find_all_functions_map),
        // Create td graph and validate it
        from_fn(build_td_graph),
    )
}
