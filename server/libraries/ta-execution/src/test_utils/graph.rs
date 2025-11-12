//
// Copyright 2025 Tabs Data Inc.
//

use crate::graphs::{ExecutionGraph, GraphBuilder};
use td_objects::execution::graph::FunctionNode;
use td_objects::table_ref::Versions;
use td_objects::test_utils::graph::{
    FUNCTION_NAMES, TABLE_NAMES, dependency, function_node, table, trigger,
};

pub async fn test_graph() -> (ExecutionGraph<Versions>, FunctionNode) {
    let output_tables = vec![
        table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[2]).await,
    ];
    let input_tables = [dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];
    let input_tables = input_tables.iter().map(|t| (&t.0, &t.1, &t.2)).collect();
    let trigger_graph = [trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];
    let trigger_graph = trigger_graph.iter().map(|t| (&t.0, &t.1, &t.2)).collect();

    let builder = GraphBuilder::new(&output_tables, &trigger_graph, &input_tables);
    let trigger_function = function_node(&FUNCTION_NAMES[0]);

    let graph = builder.build(trigger_function.clone()).unwrap();
    (graph, trigger_function)
}
