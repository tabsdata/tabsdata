//
// Copyright 2025 Tabs Data Inc.
//

use petgraph::prelude::{EdgeRef, NodeIndex};
use petgraph::Direction;
use td_execution::dataset::{Dataset, RelativeVersions, TdVersions};
use td_execution::execution_planner::ExecutionTemplate;
use td_execution::graphs::DatasetGraph;
use td_interceptor_api::execution::from_graph::FromDatasetGraph;

pub struct ExecutionTemplateBuilder;

impl FromDatasetGraph for ExecutionTemplateBuilder {
    fn from_graph(dgraph: DatasetGraph) -> ExecutionTemplate {
        let trigger_node = *dgraph.trigger_index();
        let mut execution_template =
            ExecutionTemplate::with_trigger(dgraph.dataset_for_index(trigger_node));

        let current_dataset = dgraph.dataset_for_index(trigger_node);

        // Discover incoming data edges.
        let incoming_data_edges = discover_incoming_data_edges(&dgraph, trigger_node);

        // Add requirements for incoming data edges.
        for (source_dataset, version) in incoming_data_edges {
            execution_template.add_data_requirement(current_dataset, source_dataset, version);
        }

        execution_template
    }
}

/// Discovers incoming data edges for a given node.
pub fn discover_incoming_data_edges(
    dgraph: &DatasetGraph,
    current_node: NodeIndex,
) -> Vec<(&Dataset, RelativeVersions)> {
    let mut data_edges = Vec::new();

    for edge in dgraph
        .graph()
        .edges_directed(current_node, Direction::Incoming)
    {
        let link = edge.weight();
        if let TdVersions::Table { .. } = link {
            let link = link.clone();

            let source_node = edge.source();
            let version = if current_node == source_node {
                RelativeVersions::Same(link)
            } else {
                RelativeVersions::Plan(link)
            };

            let source_dataset = &dgraph.graph()[source_node];
            data_edges.push((source_dataset, version))
        };
    }

    data_edges
}
