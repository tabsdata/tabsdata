//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use petgraph::prelude::NodeIndex;
use std::collections::HashSet;
use ta_execution::graphs::ExecutionGraph;

/// Trait for planning triggered functions in an execution graph. It should only return node indices
/// that are triggered except the main trigger, which is always triggered.
#[async_trait]
pub trait TriggerPlanner<V> {
    fn triggered_functions_index(&self) -> HashSet<NodeIndex>;
}

#[async_trait]
impl<V> TriggerPlanner<V> for ExecutionGraph<V> {
    fn triggered_functions_index(&self) -> HashSet<NodeIndex> {
        HashSet::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::{DiGraph, Graph};
    use ta_execution::graphs::ExecutionGraph;
    use td_error::TdError;
    use td_objects::types::execution::{GraphDependency, GraphEdge, GraphNode};
    use td_objects::types::test_utils::execution::{function_node, table_node};

    #[test]
    fn test_triggered_functions_index() -> Result<(), TdError> {
        let mut graph: Graph<GraphNode, GraphEdge<u64>> = DiGraph::new();
        let fn_1 = graph.add_node(GraphNode::Function(function_node("fn_1")));
        let table_1 = graph.add_node(GraphNode::Table(table_node("table_1")));
        graph.add_edge(fn_1, table_1, GraphEdge::output(0));

        let fn_2 = graph.add_node(GraphNode::Function(function_node("fn_2")));
        let table_2 = graph.add_node(GraphNode::Table(table_node("table_2")));
        graph.add_edge(fn_2, table_2, GraphEdge::output(0));

        let fn_3 = graph.add_node(GraphNode::Function(function_node("fn_2")));
        let table_3 = graph.add_node(GraphNode::Table(table_node("table_3")));
        graph.add_edge(fn_3, table_3, GraphEdge::output(0));

        graph.add_edge(table_1, fn_2, GraphEdge::trigger(0));
        graph.add_edge(
            table_1,
            fn_2,
            GraphEdge::dependency(
                0,
                GraphDependency::builder()
                    .try_dep_pos(0)?
                    .self_dependency(false)
                    .build()?,
            ),
        );
        graph.add_edge(table_2, fn_3, GraphEdge::trigger(0));

        let execution_graph = ExecutionGraph::new(graph.clone(), fn_1);
        let triggered_functions = execution_graph.triggered_functions_index();
        assert_eq!(triggered_functions.len(), 0);

        let execution_graph = ExecutionGraph::new(graph, fn_2);
        let triggered_functions = execution_graph.triggered_functions_index();
        assert_eq!(triggered_functions.len(), 0);
        Ok(())
    }
}
