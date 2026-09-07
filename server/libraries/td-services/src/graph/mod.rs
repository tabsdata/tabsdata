//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::build::GraphBuilder;
use std::collections::HashMap;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::StateFilter;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::graph::TdGraph;
use td_objects::types::basic::AtTime;
use td_objects::types::basic::{FunctionId, TableId};
use td_objects::types::recursion::RecursionOptions;

pub mod build;

/// Knobs for [`GraphBuilder`] / [`generate_td_graph`]. Build one with
/// [`GraphConfig::defaults`] and override what differs via struct update:
///
/// ```ignore
/// GraphConfig { historic: true, ..GraphConfig::defaults(FunctionDB::active()) }
/// ```
///
/// Authorization is owned by the calling service: under the
/// project-oriented RBAC model projects are silos with no
/// inter-project visibility, so the caller gates access with its own
/// project-scoped `Rbac` check before invoking graph generation, and
/// the graph module itself carries no authz.
///
/// `historic` picks the read mode: `false` (default) reads live
/// state from `_current` views — used by execution and live
/// inspection. `true` rebuilds graph state at the request's `AtTime`
/// via ranked CTEs — used only by plan-reading services that need the
/// graph as it existed when the plan was dispatched.
pub struct GraphConfig {
    pub trigger_state: StateFilter<TriggerDBWithNames>,
    pub dependency_state: StateFilter<DependencyDBWithNames>,
    pub table_state: StateFilter<TableDB>,
    pub function_state: StateFilter<FunctionDB>,
    pub include_dependency_functions: bool,
    pub historic: bool,
}

impl GraphConfig {
    pub fn defaults(function_state: StateFilter<FunctionDB>) -> Self {
        Self {
            trigger_state: TriggerDBWithNames::active(),
            dependency_state: DependencyDBWithNames::active(),
            table_state: TableDB::available(),
            function_state,
            include_dependency_functions: false,
            historic: false,
        }
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

/// Generate the trigger/dependency graph around `function_or_table`, per the
/// [`GraphConfig`] knobs. Authorization is owned by the calling service (see
/// [`GraphConfig`]).
pub async fn generate_td_graph(
    conn: &mut DomainConnection<Apiserver>,
    config: GraphConfig,
    at_time: &AtTime,
    function_or_table: &FunctionOrTable,
    recursion_options: &RecursionOptions,
) -> Result<TdGraph, TdError> {
    let builder = GraphBuilder { config };
    let trigger_graph = builder
        .find_recursive_trigger_graph(conn, at_time, function_or_table, recursion_options)
        .await?;
    // Find dependencies. For clarity, we always show the function, even if
    // generating a table centered graph, the same way we show the tables of
    // a function centered graph.
    let function_id = builder
        .main_function_id(conn, at_time, function_or_table)
        .await?;
    let (dep_graph, function_ids) = if builder.config.include_dependency_functions {
        let dep_graph = builder
            .find_recursive_dependency_graph(conn, at_time, function_or_table, recursion_options)
            .await?;
        let ids = builder.unique_function_ids(&function_id, &trigger_graph, &dep_graph)?;
        (dep_graph, ids)
    } else {
        let ids = builder.unique_triggered_function_ids(&function_id, &trigger_graph)?;
        let dep_graph = builder
            .find_triggered_functions_dependency_graph(conn, at_time, &ids)
            .await?;
        (dep_graph, ids)
    };
    let output_tables = builder
        .find_functions_output_tables(conn, at_time, &function_ids)
        .await?;
    let all_tables_map: HashMap<TableId, _> = builder
        .find_all_tables_map(conn, at_time, &trigger_graph, &dep_graph, &output_tables)
        .await?;
    let all_functions_map: HashMap<FunctionId, FunctionDBWithNames> = builder
        .find_all_functions_map(conn, at_time, &trigger_graph, &dep_graph, &output_tables)
        .await?;
    builder.build_td_graph(
        &trigger_graph,
        &dep_graph,
        &output_tables,
        &all_tables_map,
        &all_functions_map,
    )
}
