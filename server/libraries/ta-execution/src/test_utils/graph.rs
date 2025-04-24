//
// Copyright 2025 Tabs Data Inc.
//

use crate::graphs::{ExecutionGraph, GraphBuilder};
use td_objects::types::execution::FunctionVersionNode;
use td_objects::types::table_ref::Versions;
use td_objects::types::test_utils::execution::{
    dependency, function_node, table, trigger, FUNCTION_NAMES, TABLE_NAMES,
};

pub async fn test_graph() -> (ExecutionGraph<Versions>, FunctionVersionNode) {
    let output_tables = vec![
        table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        table(&FUNCTION_NAMES[1], &TABLE_NAMES[2]).await,
    ];
    let input_tables = vec![dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];
    let trigger_graph = vec![trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];

    let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
    let trigger_function = function_node(&FUNCTION_NAMES[0]);

    let graph = builder.build(trigger_function.clone()).unwrap();
    (graph, trigger_function)
}
