//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::dependency::DependencyDBWithNames;
use crate::dxo::function::FunctionDBWithNames;
use crate::dxo::graph::{
    FunctionNode, GraphDependency, GraphEdge, GraphNode, GraphOutput, TableNode,
};
use crate::dxo::table::TableDBWithNames;
use crate::dxo::trigger::TriggerDBWithNames;
use crate::transaction::map::TransactionMapper;
use crate::types::basic::{FunctionId, InputIdx, TableId, TransactionKey};
use crate::types::table_ref::Versions;
use petgraph::dot::{Config, Dot};
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use petgraph::visit::IntoNodeReferences;
use petgraph::{Direction, Graph};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use ta_objects::Buildable;
use td_error::{td_error, TdError};

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

    /// Adds a node to the graph if it does not exist.
    fn add_if_absent(&mut self, node: GraphNode) -> NodeIndex {
        *self
            .node_map
            .entry(node.clone())
            .or_insert_with(|| self.graph.add_node(node))
    }

    /// Adds trigger edges between table and function nodes.
    pub fn with_triggers(mut self, triggers: &[TriggerDBWithNames]) -> Result<Self, TdError> {
        for trigger in triggers {
            let table = self
                .all_tables_map
                .get(&trigger.trigger_by_table_id)
                .ok_or(GraphError::MissingTable(trigger.trigger_by_table_id))?;
            let function = self
                .all_functions_map
                .get(&trigger.function_id)
                .ok_or(GraphError::MissingFunction(trigger.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            // note it is inverted in output edges
            self.graph
                .add_edge(table_idx, function_idx, GraphEdge::trigger(Versions::None));
        }
        Ok(self)
    }

    /// Adds output edges from functions to output tables.
    pub fn with_outputs(mut self, outputs: &[TableDBWithNames]) -> Result<Self, TdError> {
        for table in outputs {
            let function = self
                .all_functions_map
                .get(&table.function_id)
                .ok_or(GraphError::MissingFunction(table.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            let graph_output = GraphOutput::builder()
                .output_pos(table.function_param_pos)
                .build()?;

            // note it is inverted in dep and trigger edges
            self.graph.add_edge(
                function_idx,
                table_idx,
                GraphEdge::output(Versions::None, graph_output),
            );
        }
        Ok(self)
    }

    /// Adds dependency edges between functions.
    pub fn with_dependencies(mut self, deps: &[DependencyDBWithNames]) -> Result<Self, TdError> {
        // Provenance `input_idx`: a single global index across the plan's
        // dependency edges. Building the edges in a deterministic
        // `(function_id, dep_pos)` order lets the index fall out of the
        // enumeration in one pass — no lookup table — and makes it reproducible
        // whenever the plan is rebuilt.
        let mut deps: Vec<&DependencyDBWithNames> = deps.iter().collect();
        deps.sort_by_key(|dep| (dep.function_id, dep.dep_pos));

        for (input_idx, dep) in deps.into_iter().enumerate() {
            let table = self
                .all_tables_map
                .get(&dep.table_id)
                .ok_or(GraphError::MissingTable(dep.table_id))?;
            let function = self
                .all_functions_map
                .get(&dep.function_id)
                .ok_or(GraphError::MissingFunction(dep.function_id))?;

            let (function_node, table_node) = GraphNode::pair(function, table)?;
            let function_idx = self.add_if_absent(function_node);
            let table_idx = self.add_if_absent(table_node);

            let graph_dep = GraphDependency::builder()
                .dep_pos(dep.dep_pos)
                .self_dependency(function.id == table.function_version_id)
                .input_idx(InputIdx::try_from(input_idx as i32)?)
                .build()?;

            // note it is inverted in output edges
            self.graph.add_edge(
                table_idx,
                function_idx,
                GraphEdge::dependency(dep.table_versions.deref().clone(), graph_dep),
            );
        }
        Ok(self)
    }

    /// Consumes the builder and returns the final owned graph.
    pub fn build(self) -> TdGraph {
        TdGraph { inner: self.graph }
    }
}

/// Tabsdata Graph representation, consisting in nodes and edges.
/// There are two types of nodes: functions and tables.
/// The edges are of three types:
/// 1. Output: from function to table, which marks the tables created.
/// 2. Trigger: from table to function, which marks the functions triggered.
/// 3. Dependency: from table to function, which marks which functions depend on the table.
///
/// For 1, 2 and 3, it contains the versions of the table going in or out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TdGraph<F = FunctionNode, T = TableNode, V = Versions> {
    #[serde(flatten)]
    pub inner: Graph<GraphNode<F, T>, GraphEdge<V>>,
}

impl<F, T, V> TdGraph<F, T, V>
where
    F: Eq + Hash,
    T: Eq + Hash,
{
    /// Returns all the nodes(functions) present (including the main function).
    pub fn functions(&self) -> HashSet<&F> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Function(function) => Some(function),
                _ => None,
            })
            .collect()
    }

    /// Returns all the edges(tables) present.
    pub fn tables(&self) -> HashSet<&T> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Table(table) => Some(table),
                _ => None,
            })
            .collect()
    }

    /// Returns all the edges and the nodes connecting them.
    pub fn relations(&self) -> HashSet<(&F, &T, &GraphEdge<V>)>
    where
        V: Eq + Hash,
    {
        self.inner
            .edge_references()
            .map(|e| {
                let source = &self.inner[e.source()];
                let target = &self.inner[e.target()];
                let weight = e.weight();
                let (function, table) = match (source, target) {
                    (GraphNode::Function(f), GraphNode::Table(t)) => (f, t),
                    (GraphNode::Table(t), GraphNode::Function(f)) => (f, t),
                    _ => unreachable!(),
                };
                (function, table, weight)
            })
            .collect()
    }
}

impl<F: Display, T: Display, V: Display> TdGraph<F, T, V> {
    /// Generates a DOT representation of the graph.
    pub fn dot(&self) -> Dot<'_, &Graph<GraphNode<F, T>, GraphEdge<V>>> {
        Dot::with_attr_getters(
            &self.inner,
            &[Config::EdgeNoLabel],
            &|_, edge| match edge.weight() {
                GraphEdge::Output { .. } => String::new(),
                GraphEdge::Trigger { .. } => String::new(),
                GraphEdge::Dependency { versions, .. } => {
                    format!("label=\"{versions}\"")
                }
            },
            &|_, (_, node)| match node {
                GraphNode::Function(_) => "shape=circle".to_string(),
                GraphNode::Table(_) => "shape=box".to_string(),
            },
        )
    }
}

/// Represents a partial graph used for validation.
pub struct PartialGraph<F> {
    pub inner: Graph<F, ()>,
}

impl<F> PartialGraph<F> {
    /// Creates a partial graph containing only trigger edges.
    pub fn trigger_graph<T, V>(original: &TdGraph<F, T, V>) -> Result<PartialGraph<&F>, TdError> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Add nodes we want to keep (function nodes)
        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Function(node) = node {
                let new_idx = new_graph.add_node(node);
                node_map.insert(node_idx, new_idx);
            }
        }

        // Rewire edges
        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Table(_) = node {
                // For each ignored node, connect its predecessors to successors
                let preds: Vec<_> = original
                    .inner
                    .edges_directed(node_idx, Direction::Incoming)
                    .filter(|edge| !matches!(edge.weight(), GraphEdge::Dependency { .. }))
                    .map(|edge| edge.source())
                    .collect();

                let succs: Vec<_> = original
                    .inner
                    .edges_directed(node_idx, Direction::Outgoing)
                    .filter(|edge| !matches!(edge.weight(), GraphEdge::Dependency { .. }))
                    .map(|edge| edge.target())
                    .collect();

                for pred in preds {
                    if let Some(&new_pred) = node_map.get(&pred) {
                        for succ in &succs {
                            if let Some(&new_succ) = node_map.get(succ) {
                                new_graph.add_edge(new_pred, new_succ, ());
                            }
                        }
                    }
                }
            }
        }

        Ok(PartialGraph { inner: new_graph })
    }

    /// Creates a partial graph containing only dependency edges.
    pub fn dependency_graph<T, V>(
        original: &TdGraph<F, T, V>,
        drop_self_dependency: bool,
    ) -> Result<PartialGraph<&T>, TdError> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Add nodes we want to keep (table nodes)
        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Table(node) = node {
                let new_idx = new_graph.add_node(node);
                node_map.insert(node_idx, new_idx);
            }
        }

        // Rewire edges
        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Function(_) = node {
                // For each ignored node, connect its predecessors to successors
                let preds: Vec<_> = original
                    .inner
                    .edges_directed(node_idx, Direction::Incoming)
                    .filter(|edge| match edge.weight() {
                        GraphEdge::Output { .. } => true,
                        GraphEdge::Trigger { .. } => false,
                        GraphEdge::Dependency { dependency, .. } => {
                            !(*dependency.self_dependency && drop_self_dependency)
                        }
                    })
                    .map(|edge| edge.source())
                    .collect();

                let succs: Vec<_> = original
                    .inner
                    .edges_directed(node_idx, Direction::Outgoing)
                    .filter(|edge| match edge.weight() {
                        GraphEdge::Output { .. } => true,
                        GraphEdge::Trigger { .. } => false,
                        GraphEdge::Dependency { dependency, .. } => {
                            !(*dependency.self_dependency && drop_self_dependency)
                        }
                    })
                    .map(|edge| edge.target())
                    .collect();

                for pred in preds {
                    if let Some(&new_pred) = node_map.get(&pred) {
                        for succ in &succs {
                            if let Some(&new_succ) = node_map.get(succ) {
                                new_graph.add_edge(new_pred, new_succ, ());
                            }
                        }
                    }
                }
            }
        }

        Ok(PartialGraph { inner: new_graph })
    }

    /// Generates a DOT representation of the partial graph.
    #[allow(dead_code)]
    pub fn dot(&self) -> Dot<'_, &Graph<F, ()>> {
        Dot::with_attr_getters(
            &self.inner,
            &[Config::EdgeNoLabel],
            &|_, _| String::new(),
            &|_, _| String::new(),
        )
    }
}

impl PartialGraph<FunctionNode> {
    /// Creates a trigger transaction graph.
    pub fn transaction_graph<T, V>(
        dgraph: &TdGraph<FunctionNode, T, V>,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<PartialGraph<TransactionKey>, TdError> {
        let trigger_graph = PartialGraph::trigger_graph(dgraph)?;

        let mut new_graph = DiGraph::new();

        let mut node_map = HashMap::new();
        for node in trigger_graph.inner.node_indices() {
            let key = transaction_mapper.key(trigger_graph.inner[node])?;
            node_map
                .entry(key.clone())
                .or_insert_with(|| new_graph.add_node(key));
        }

        let mut edge_map = HashMap::new();
        for edge in trigger_graph.inner.edge_references() {
            let source_key = transaction_mapper.key(trigger_graph.inner[edge.source()])?;
            let target_key = transaction_mapper.key(trigger_graph.inner[edge.target()])?;
            let source = node_map[&source_key];
            let target = node_map[&target_key];
            if source != target && !edge_map.contains_key(&(source, target)) {
                new_graph.add_edge(source, target, ());
                edge_map.insert((source, target), ());
            }
        }

        Ok(PartialGraph { inner: new_graph })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::MapGraph;
    use crate::test_utils::graph::{
        function_node, table_node, test_graph, FUNCTION_NAMES, TABLE_NAMES,
    };
    use crate::test_utils::transaction::TestTransactionBy;
    use itertools::Itertools;
    use std::collections::HashSet;

    fn expected_function_nodes() -> HashSet<FunctionNode> {
        FUNCTION_NAMES.iter().map(function_node).collect()
    }

    fn expected_table_nodes() -> HashSet<TableNode> {
        TABLE_NAMES.iter().map(table_node).collect()
    }

    #[tokio::test]
    async fn test_functions() -> Result<(), TdError> {
        let graph = test_graph().await;
        let actual = graph
            .functions()
            .into_iter()
            .cloned()
            .collect::<HashSet<_>>();
        let expected = expected_function_nodes();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_tables() -> Result<(), TdError> {
        let graph = test_graph().await;
        let actual = graph.tables().into_iter().cloned().collect::<HashSet<_>>();
        let expected = expected_table_nodes();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_edges_types() {
        let graph = test_graph().await;

        for node in graph.inner.node_indices() {
            let edges = graph.inner.edges(node);
            for edge in edges {
                let source = &graph.inner[edge.source()];
                let target = &graph.inner[edge.target()];
                match edge.weight() {
                    GraphEdge::Output { .. } => {
                        assert!(matches!(source, GraphNode::Function(_)));
                        assert!(matches!(target, GraphNode::Table(_)));
                    }
                    GraphEdge::Trigger { .. } => {
                        assert!(matches!(source, GraphNode::Table(_)));
                        assert!(matches!(target, GraphNode::Function(_)));
                    }
                    GraphEdge::Dependency { .. } => {
                        assert!(matches!(source, GraphNode::Table(_)));
                        assert!(matches!(target, GraphNode::Function(_)));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_graph_edges() {
        let graph = test_graph().await;

        for node in graph.inner.node_indices() {
            let edges = graph.inner.edges(node);
            for edge in edges {
                let source = &graph.inner[edge.source()];
                let target = &graph.inner[edge.target()];
                match (source, target) {
                    (GraphNode::Function(f), GraphNode::Table(t)) => match (&f.name, &t.name) {
                        (function, table)
                            if function == &FUNCTION_NAMES[0] && table == &TABLE_NAMES[0] =>
                        {
                            assert!(matches!(edge.weight(), GraphEdge::Output { .. }));
                        }
                        (function, table)
                            if function == &FUNCTION_NAMES[1] && table == &TABLE_NAMES[1] =>
                        {
                            assert!(matches!(edge.weight(), GraphEdge::Output { .. }));
                        }
                        (function, table)
                            if function == &FUNCTION_NAMES[1] && table == &TABLE_NAMES[2] =>
                        {
                            assert!(matches!(edge.weight(), GraphEdge::Output { .. }));
                        }
                        (function, table)
                            if function == &FUNCTION_NAMES[2] && table == &TABLE_NAMES[3] =>
                        {
                            assert!(matches!(edge.weight(), GraphEdge::Output { .. }));
                        }
                        _ => unreachable!(),
                    },
                    (GraphNode::Table(t), GraphNode::Function(f)) => match (&t.name, &f.name) {
                        (table, function)
                            if table == &TABLE_NAMES[0] && function == &FUNCTION_NAMES[0] =>
                        {
                            assert!(matches!(
                                edge.weight(),
                                GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                            ));
                        }
                        (table, function)
                            if table == &TABLE_NAMES[0] && function == &FUNCTION_NAMES[1] =>
                        {
                            assert!(matches!(
                                edge.weight(),
                                GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                            ));
                        }
                        (table, function)
                            if table == &TABLE_NAMES[1] && function == &FUNCTION_NAMES[2] =>
                        {
                            assert!(matches!(edge.weight(), GraphEdge::Dependency { .. }));
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_graph_relations() {
        let graph = test_graph().await;
        let relations = graph.relations();

        assert_eq!(relations.len(), 8);
        for (function, table, edge) in relations {
            if function.name == FUNCTION_NAMES[0] && table.name == TABLE_NAMES[0] {
                assert!(matches!(
                    edge,
                    GraphEdge::Output { .. } | GraphEdge::Dependency { .. }
                ));
            }
            if function.name == FUNCTION_NAMES[1] && table.name == TABLE_NAMES[1] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
            if function.name == FUNCTION_NAMES[1] && table.name == TABLE_NAMES[2] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
            if function.name == FUNCTION_NAMES[2] && table.name == TABLE_NAMES[3] {
                assert!(matches!(edge, GraphEdge::Output { .. }));
            }
            if table.name == TABLE_NAMES[0] && function.name == FUNCTION_NAMES[1] {
                assert!(matches!(
                    edge,
                    GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                ));
            }
            if table.name == TABLE_NAMES[1] && function.name == FUNCTION_NAMES[2] {
                assert!(matches!(edge, GraphEdge::Dependency { .. }));
            }
        }
    }

    #[tokio::test]
    async fn test_trigger_graph() {
        let graph = test_graph().await;
        let trigger_graph = PartialGraph::trigger_graph(&graph).unwrap();
        assert_eq!(trigger_graph.inner.node_count(), 3);
        assert_eq!(trigger_graph.inner.edge_count(), 1);

        let function_nodes = trigger_graph
            .inner
            .node_indices()
            .map(|node| trigger_graph.inner[node].clone())
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        let expected_function_nodes = expected_function_nodes()
            .into_iter()
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(function_nodes, expected_function_nodes);

        let edges: Vec<_> = trigger_graph.inner.edge_references().collect();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        let source = trigger_graph.inner[edge.source()];
        assert_eq!(source.name, FUNCTION_NAMES[0]);
        let target = trigger_graph.inner[edge.target()];
        assert_eq!(target.name, FUNCTION_NAMES[1]);
    }

    #[tokio::test]
    async fn test_dependency_graph() {
        let graph = test_graph().await;
        let dependency_graph = PartialGraph::dependency_graph(&graph, true).unwrap();
        assert_eq!(dependency_graph.inner.node_count(), TABLE_NAMES.len());
        assert_eq!(dependency_graph.inner.edge_count(), 3);

        let table_nodes = dependency_graph
            .inner
            .node_indices()
            .map(|node| dependency_graph.inner[node].clone())
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        let expected_table_nodes = expected_table_nodes()
            .into_iter()
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(table_nodes, expected_table_nodes);

        let edges: Vec<_> = dependency_graph.inner.edge_references().collect();
        assert_eq!(edges.len(), 3);

        let edge = edges[0];
        let source = dependency_graph.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[0]);
        let target = dependency_graph.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[2]);

        let edge = edges[1];
        let source = dependency_graph.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[0]);
        let target = dependency_graph.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[1]);

        let edge = edges[2];
        let source = dependency_graph.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[1]);
        let target = dependency_graph.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[3]);
    }

    #[tokio::test]
    async fn test_transaction_graph() {
        let graph = test_graph().await;

        let transaction_by = TestTransactionBy::Name;
        let transaction_graph = PartialGraph::transaction_graph(&graph, &transaction_by).unwrap();
        assert_eq!(transaction_graph.inner.node_count(), 3);
        assert_eq!(transaction_graph.inner.edge_count(), 1);

        let expected_trx_nodes = vec![
            TransactionKey::try_from(FUNCTION_NAMES[0].to_string()).unwrap(),
            TransactionKey::try_from(FUNCTION_NAMES[1].to_string()).unwrap(),
            TransactionKey::try_from(FUNCTION_NAMES[2].to_string()).unwrap(),
        ];
        let trx_nodes = transaction_graph
            .inner
            .node_indices()
            .map(|node| transaction_graph.inner[node].clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(expected_trx_nodes, trx_nodes);

        let edges: Vec<_> = transaction_graph.inner.edge_references().collect();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        let source = &transaction_graph.inner[edge.source()];
        assert_eq!(
            *source,
            TransactionKey::try_from(FUNCTION_NAMES[0].to_string()).unwrap()
        );
        let target = &transaction_graph.inner[edge.target()];
        assert_eq!(
            *target,
            TransactionKey::try_from(FUNCTION_NAMES[1].to_string()).unwrap()
        );

        let transaction_by = TestTransactionBy::Single;
        let transaction_graph = PartialGraph::transaction_graph(&graph, &transaction_by).unwrap();
        assert_eq!(transaction_graph.inner.node_count(), 1);
        assert_eq!(transaction_graph.inner.edge_count(), 0);

        let node = transaction_graph.inner.node_indices().next().unwrap();
        let node = &transaction_graph.inner[node];
        assert_eq!(*node, TransactionKey::try_from("S").unwrap());
    }

    #[tokio::test]
    async fn test_map_graph_versioned_structure() -> Result<(), TdError> {
        let graph = test_graph().await;
        let functions = graph.functions();
        let tables = graph.tables();

        let new_graph = graph
            .map(
                |node| async move {
                    Ok::<_, TdError>(match node {
                        GraphNode::Function(f) => GraphNode::Function(f.name.clone()),
                        GraphNode::Table(t) => GraphNode::Table(t.name.clone()),
                    })
                },
                |_, _, _| async { Ok::<_, TdError>(1) },
            )
            .await?;

        assert_eq!(new_graph.functions().len(), functions.len());
        assert_eq!(new_graph.tables().len(), tables.len());
        Ok(())
    }
}
