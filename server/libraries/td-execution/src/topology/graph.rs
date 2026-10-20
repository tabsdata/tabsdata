//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::TopologyGraph;
use crate::topology::ProjectTopology;
use crate::topology::walk::Admitted;
use petgraph::Graph;
use petgraph::prelude::NodeIndex;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::collection::ResolvedCollectionDB;
use td_objects::dxo::graph::{
    FunctionNode, GraphDependency, GraphEdge, GraphNode, GraphOutput, TableNode,
};
use td_objects::dxo::topology::{TopologyDependency, TopologyTable, TopologyTrigger};
use td_objects::types::basic::{CollectionId, FunctionId, SelfDependency, TableId};
use td_objects::types::recursion::RecursionOptions;
use td_objects::types::table_ref::Versions;

#[td_error]
enum Missing {
    #[error("Missing table information for table with ID '{0}' in contextual map")]
    Table(TableId) = 5000,
    #[error("Missing function information for function with ID '{0}' in contextual map")]
    Function(FunctionId) = 5001,
    #[error("Missing collection information for collection with ID '{0}' in contextual map")]
    Collection(CollectionId) = 5002,
}

impl ProjectTopology {
    pub fn graph(&self) -> Result<TopologyGraph, TdError> {
        // A frozen table outlives the function that wrote it: no output edge.
        let outputs = self
            .ordered_tables()
            .filter(|t| self.functions.contains_key(&t.function_id));
        self.assemble(&self.triggers, &self.dependencies, outputs)
    }

    /// Everything flowing into and out of `table`, whatever the state of the
    /// functions on the way.
    pub fn flow_graph(&self, table: &TableId) -> Result<TopologyGraph, TdError> {
        let start = self.table(table)?.function_id;
        let admitted = Admitted::flow(self, start);
        let unbounded = RecursionOptions::new(None, None);
        let mut triggers = self.trigger_walk(&admitted, &unbounded);
        let children = self.children_triggers(&admitted, &triggers);
        triggers.extend(children);
        let dependencies = self.dependency_walk(&admitted, &unbounded);
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .flat_map(|t| [*t.trigger_by_function_id, t.function_id])
            .chain(
                dependencies
                    .iter()
                    .flat_map(|d| [*d.table_function_id, d.function_id]),
            )
            .chain([start])
            .collect();
        self.subgraph(&triggers, dependencies, &functions)
    }

    /// `triggers` and `dependencies`, plus the active tables `functions` write.
    pub(crate) fn subgraph(
        &self,
        triggers: &[&TopologyTrigger],
        dependencies: Vec<&TopologyDependency>,
        functions: &HashSet<FunctionId>,
    ) -> Result<TopologyGraph, TdError> {
        self.assemble(
            triggers.iter().copied(),
            dependencies,
            self.outputs_of(functions),
        )
    }

    pub(crate) fn function_node(&self, function: &FunctionId) -> Result<FunctionNode, TdError> {
        let function = self
            .functions
            .get(function)
            .ok_or(Missing::Function(*function))?;
        let collection = self.collection(&function.collection_id)?;
        Ok(FunctionNode {
            collection_id: function.collection_id,
            collection: collection.base.name.clone(),
            function_id: function.function_id,
            function_version_id: function.id.clone(),
            name: function.name.clone(),
            enabled: function.enabled,
            group_id: collection.base.group_id,
            parent_function_id: function.parent_function_id,
        })
    }

    fn table_node(&self, table: &TopologyTable) -> Result<TableNode, TdError> {
        let collection = self.collection(&table.collection_id)?;
        Ok(TableNode {
            collection_id: table.collection_id,
            collection: collection.base.name.clone(),
            function_version_id: table.function_version_id.clone(),
            table_id: table.table_id,
            table_version_id: table.id,
            name: table.name.clone(),
            system: table.system(),
            private: table.private,
        })
    }

    fn table(&self, table: &TableId) -> Result<&TopologyTable, TdError> {
        Ok(self.tables.get(table).ok_or(Missing::Table(*table))?)
    }

    fn collection(&self, collection: &CollectionId) -> Result<&ResolvedCollectionDB, TdError> {
        Ok(self
            .collections
            .get(collection)
            .ok_or(Missing::Collection(*collection))?)
    }

    /// The graph of these rows: a trigger and a dependency are edges from a
    /// table into a function, an output one from a function into its table.
    fn assemble<'r>(
        &self,
        triggers: impl IntoIterator<Item = &'r TopologyTrigger>,
        dependencies: impl IntoIterator<Item = &'r TopologyDependency>,
        outputs: impl IntoIterator<Item = &'r TopologyTable>,
    ) -> Result<TopologyGraph, TdError> {
        let mut graph = Assembly {
            topology: self,
            graph: Graph::new(),
            nodes: HashMap::new(),
        };
        for trigger in triggers {
            let table = self.table(&trigger.trigger_by_table_id)?;
            let (function, table) = graph.pair(&trigger.function_id, table)?;
            graph
                .graph
                .add_edge(table, function, GraphEdge::trigger(Versions::None));
        }
        for dependency in dependencies {
            let table = self.table(&dependency.table_id)?;
            // A table keeps the version that defined it: producer identity is the function.
            let graph_dependency = GraphDependency {
                dep_pos: dependency.dep_pos,
                self_dependency: SelfDependency::from(dependency.function_id == table.function_id),
            };
            let (function, table) = graph.pair(&dependency.function_id, table)?;
            graph.graph.add_edge(
                table,
                function,
                GraphEdge::dependency(dependency.table_versions.deref().clone(), graph_dependency),
            );
        }
        for table in outputs {
            let graph_output = GraphOutput {
                output_pos: table.function_param_pos,
            };
            let (function, table) = graph.pair(&table.function_id, table)?;
            graph.graph.add_edge(
                function,
                table,
                GraphEdge::output(Versions::None, graph_output),
            );
        }
        Ok(TopologyGraph { inner: graph.graph })
    }
}

struct Assembly<'t> {
    topology: &'t ProjectTopology,
    graph: Graph<GraphNode, GraphEdge<Versions>>,
    nodes: HashMap<GraphNode, NodeIndex>,
}

impl Assembly<'_> {
    fn pair(
        &mut self,
        function: &FunctionId,
        table: &TopologyTable,
    ) -> Result<(NodeIndex, NodeIndex), TdError> {
        let function = GraphNode::Function(self.topology.function_node(function)?);
        let table = GraphNode::Table(self.topology.table_node(table)?);
        Ok((self.node(function), self.node(table)))
    }

    fn node(&mut self, node: GraphNode) -> NodeIndex {
        *self
            .nodes
            .entry(node.clone())
            .or_insert_with(|| self.graph.add_node(node))
    }
}
