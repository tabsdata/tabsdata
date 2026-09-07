//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::build::GraphBuilder;
use std::collections::HashMap;
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::graph::TdGraph;
use td_objects::sql_condition::SqlCondition;
use td_objects::types::basic::AtTime;
use td_objects::types::basic::{FunctionId, TableId};
use td_objects::types::recursion::RecursionOptions;

pub mod build;

/// Knobs for [`GraphBuilder`] / [`generate_td_graph`]. Build one with
/// [`GraphConfig::defaults`] and override what differs via struct update:
///
/// ```ignore
/// GraphConfig { include_dependency_functions: true, ..GraphConfig::defaults(FunctionDB::active()) }
/// ```
///
/// Authorization is owned by the calling service: projects are silos, so
/// the caller gates access with its own project-scoped check and the graph
/// module carries no authz.
///
pub struct GraphConfig {
    pub trigger_state: Box<dyn SqlCondition<TriggerDBWithNames>>,
    pub function_state: Box<dyn SqlCondition<FunctionDB>>,
    pub include_dependency_functions: bool,
}

impl GraphConfig {
    pub fn defaults(function_state: impl SqlCondition<FunctionDB> + 'static) -> Self {
        Self {
            trigger_state: Box::new(TriggerDBWithNames::active()),
            function_state: Box::new(function_state),
            include_dependency_functions: false,
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
/// [`GraphConfig`] knobs, live (`None`) or as it existed at `at_time`.
/// Authorization is owned by the calling service (see [`GraphConfig`]).
pub async fn generate_td_graph<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    config: GraphConfig,
    at_time: Option<&AtTime>,
    function_or_table: &FunctionOrTable,
    recursion_options: &RecursionOptions,
) -> Result<TdGraph, TdError> {
    let builder = GraphBuilder { config };
    let trigger_graph = builder
        .find_recursive_trigger_graph(conn, at_time, function_or_table, recursion_options)
        .await?;
    // The main function is always shown, even in a table-centered graph.
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
