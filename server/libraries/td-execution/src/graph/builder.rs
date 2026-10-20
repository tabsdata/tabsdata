//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::TdGraph;
use petgraph::Graph;
use petgraph::prelude::NodeIndex;
use std::collections::HashMap;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::graph::{GraphDependency, GraphEdge, GraphNode, GraphOutput};
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, SelfDependency, TableId};
use td_objects::types::table_ref::Versions;

#[td_error]
enum GraphError {
    #[error("Missing table information for table with ID '{0}' in contextual map")]
    MissingTable(TableId) = 5000,
    #[error("Missing function information for function with ID '{0}' in contextual map")]
    MissingFunction(FunctionId) = 5001,
}

#[derive(Debug)]
pub struct TdGraphBuilder<'a> {
    graph: Graph<GraphNode, GraphEdge<Versions>>,
    node_map: HashMap<GraphNode, NodeIndex>,
    all_tables_map: &'a HashMap<TableId, TableDBWithNames>,
    all_functions_map: &'a HashMap<FunctionId, FunctionDBWithNames>,
}

impl<'a> TdGraphBuilder<'a> {
    pub fn new(
        all_tables_map: &'a HashMap<TableId, TableDBWithNames>,
        all_functions_map: &'a HashMap<FunctionId, FunctionDBWithNames>,
    ) -> Self {
        Self {
            graph: Graph::new(),
            node_map: HashMap::new(),
            all_tables_map,
            all_functions_map,
        }
    }

    fn add_if_absent(&mut self, node: GraphNode) -> NodeIndex {
        *self
            .node_map
            .entry(node.clone())
            .or_insert_with(|| self.graph.add_node(node))
    }

    pub fn with_triggers<'b>(
        mut self,
        triggers: impl IntoIterator<Item = &'b TriggerDBWithNames>,
    ) -> Result<Self, TdError> {
        for trigger in triggers {
            let table = self
                .all_tables_map
                .get(&trigger.base.base.trigger_by_table_id)
                .ok_or(GraphError::MissingTable(
                    trigger.base.base.trigger_by_table_id,
                ))?;
            let function = self
                .all_functions_map
                .get(&trigger.base.base.function_id)
                .ok_or(GraphError::MissingFunction(trigger.base.base.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            // note it is inverted in output edges
            self.graph
                .add_edge(table_idx, function_idx, GraphEdge::trigger(Versions::None));
        }
        Ok(self)
    }

    pub fn with_outputs<'b>(
        mut self,
        outputs: impl IntoIterator<Item = &'b TableDBWithNames>,
    ) -> Result<Self, TdError> {
        for table in outputs {
            let function = self
                .all_functions_map
                .get(&table.base.base.function_id)
                .ok_or(GraphError::MissingFunction(table.base.base.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            let graph_output = GraphOutput {
                output_pos: table.base.base.function_param_pos,
            };

            // note it is inverted in dep and trigger edges
            self.graph.add_edge(
                function_idx,
                table_idx,
                GraphEdge::output(Versions::None, graph_output),
            );
        }
        Ok(self)
    }

    pub fn with_dependencies<'b>(
        mut self,
        deps: impl IntoIterator<Item = &'b DependencyDBWithNames>,
    ) -> Result<Self, TdError> {
        for dep in deps {
            let table = self
                .all_tables_map
                .get(&dep.base.base.table_id)
                .ok_or(GraphError::MissingTable(dep.base.base.table_id))?;
            let function = self
                .all_functions_map
                .get(&dep.base.base.function_id)
                .ok_or(GraphError::MissingFunction(dep.base.base.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            // A table keeps the version that defined it: producer identity is the function.
            let graph_dep = GraphDependency {
                dep_pos: dep.base.base.dep_pos,
                self_dependency: SelfDependency::from(
                    function.base.base.function_id == table.base.base.function_id,
                ),
            };

            // note it is inverted in output edges
            self.graph.add_edge(
                table_idx,
                function_idx,
                GraphEdge::dependency(dep.base.base.table_versions.deref().clone(), graph_dep),
            );
        }
        Ok(self)
    }

    pub fn build(self) -> TdGraph {
        TdGraph { inner: self.graph }
    }
}
