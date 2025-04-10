//
// Copyright 2025 Tabs Data Inc.
//

use crate::graphs::{ExecutionGraph, GraphBuilder};
use td_objects::types::execution::FunctionVersionNode;
use td_objects::types::table_ref::Versions;
use td_objects::types::test_utils::execution::{dependency, function_node, table, trigger};

pub async fn test_graph() -> (ExecutionGraph<Versions>, FunctionVersionNode) {
    let output_tables = vec![
        table("function_1", "table_1").await,
        table("function_2", "table_2").await,
    ];
    let input_tables = vec![dependency("table_1", "function_2").await];
    let trigger_graph = vec![trigger("table_1", "function_2").await];

    let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
    let trigger_function = function_node("function_1");

    let graph = builder.build(trigger_function.clone()).unwrap();
    (graph, trigger_function)
}
