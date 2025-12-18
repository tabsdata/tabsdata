//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::{DoAuthz, inter_collection_authz};
use crate::graph::layers::build::GraphBuilder;
use td_objects::dxo::States;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, TableId};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod build;

pub trait GraphConfig<
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
>
{
    type Authz: DoAuthz;

    fn include_dependency_functions(&self) -> bool;
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
// Generics: define graph options and states.
#[layer]
pub fn generate_td_graph<
    C,
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
>(
    config: C,
) where
    for<'a> C: GraphConfig<TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE> + 'a,
    TriggerDBWithNames: States<TRIGGER_STATE>,
    DependencyDBWithNames: States<DEPENDENCY_STATE>,
    FunctionDB: States<FUNCTION_STATE>,
    TableDB: States<TABLE_STATE>,
{
    // Macro helper to reduce type name.
    macro_rules! gb {
        ($func: ident) => {
            GraphBuilder::<C, TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>::$func
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
        if config.include_dependency_functions() {
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
