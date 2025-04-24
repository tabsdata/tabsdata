//
// Copyright 2024 Tabs Data Inc.
//

use crate::transaction::TransactionMapper;
use getset::Getters;
use petgraph::algo::toposort;
use petgraph::dot::{Config, Dot};
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use petgraph::Graph;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::types::basic::{FunctionName, TransactionByStr, TransactionKey};
use td_objects::types::dependency::DependencyVersionDBWithNames;
use td_objects::types::execution::{
    FunctionVersionNode, GraphDependency, GraphEdge, GraphNode, GraphOutput,
};
use td_objects::types::table::TableVersionDBWithNames;
use td_objects::types::table_ref::Versions;
use td_objects::types::trigger::TriggerVersionDBWithNames;

#[td_error]
pub enum GraphError {
    #[error("Graph is not a Direct Acyclic Graph. Cycle found in: {0:?}")]
    Cyclic(FunctionName) = 0,
    #[error("Graph is not a transactional Direct Acyclic Graph with transaction mode: {0:?}. Cycle found in: {1:?}"
    )]
    CyclicTransaction(TransactionByStr, TransactionKey) = 1,
}

/// Builder for creating am `ExecutionGraph`.
pub struct GraphBuilder<'a> {
    trigger_graph: &'a Vec<TriggerVersionDBWithNames>,
    output_tables: &'a Vec<TableVersionDBWithNames>,
    input_tables: &'a Vec<DependencyVersionDBWithNames>,
}

/// Adds a node to the graph if it does not exist.
fn add_if_absent<N, E>(
    graph: &mut Graph<N, E>,
    node_map: &mut HashMap<N, NodeIndex>,
    node: N,
) -> NodeIndex
where
    N: Eq + Hash + Clone,
{
    *node_map
        .entry(node.clone())
        .or_insert_with(|| graph.add_node(node))
}

impl<'a> GraphBuilder<'a> {
    /// Creates a new `GraphBuilder` from a data and trigger graph.
    pub fn new(
        trigger_graph: &'a Vec<TriggerVersionDBWithNames>,
        output_tables: &'a Vec<TableVersionDBWithNames>,
        input_tables: &'a Vec<DependencyVersionDBWithNames>,
    ) -> Self {
        Self {
            trigger_graph,
            output_tables,
            input_tables,
        }
    }

    /// Builds a `Graph` from the data and trigger graphs, starting from a trigger function.
    /// Nodes and edges are added to the graph based on the table inputs, table outputs and triggers.
    pub fn build(self, trigger: FunctionVersionNode) -> Result<ExecutionGraph<Versions>, TdError> {
        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Add trigger function node
        let trigger_index = add_if_absent(&mut graph, &mut node_map, GraphNode::Function(trigger));

        // Add triggers, both nodes and edges
        for trigger in self.trigger_graph.deref() {
            let (source, target) = GraphNode::from_trigger(trigger)?;
            let source_index = add_if_absent(&mut graph, &mut node_map, source);
            let target_index = add_if_absent(&mut graph, &mut node_map, target);

            graph.add_edge(
                source_index,
                target_index,
                GraphEdge::trigger(Versions::None),
            );
        }

        // Add the output tables for each function
        for table in self.output_tables {
            let (source, target) = GraphNode::from_table(table)?;
            let source_index = add_if_absent(&mut graph, &mut node_map, source);
            let target_index = add_if_absent(&mut graph, &mut node_map, target);

            let graph_output = GraphOutput::builder()
                .output_pos(table.function_param_pos().clone())
                .build()?;
            graph.add_edge(
                source_index,
                target_index,
                GraphEdge::output(Versions::None, graph_output),
            );
        }

        // Add the input tables for each function
        for dependency in self.input_tables {
            let (source, target) = GraphNode::from_dependency(dependency)?;
            let source_index = add_if_absent(&mut graph, &mut node_map, source);
            let target_index = add_if_absent(&mut graph, &mut node_map, target);

            let graph_dependency = GraphDependency::builder()
                .dep_pos(dependency.dep_pos())
                .self_dependency(
                    dependency.function_version_id() == dependency.table_function_version_id(),
                )
                .build()?;
            let versions = dependency.table_versions().deref();
            let edge = GraphEdge::dependency(versions.clone(), graph_dependency);

            graph.add_edge(source_index, target_index, edge);
        }

        // And finally, build the graph
        Ok(ExecutionGraph::new(graph, trigger_index))
    }
}

/// Graph representation of the execution.
/// There are two types of nodes: functions and tables.
/// The edges are of three types:
/// 1. Output: from function to table, which marks the tables created.
/// 2. Trigger: from table to function, which marks the functions triggered.
/// 3. Dependency: from table to function, which marks which functions depend on the table.
///
/// For 1, 2 and 3, it contains the versions of the table going in or out. This version will always
/// be `Versions::None` for the output and trigger edges (which is the version being planned).
/// And any Versions for the dependency nodes.
/// For 3, it also contains further information to resolve the dependency later on.
#[derive(Getters)]
#[getset(get = "pub")]
pub struct ExecutionGraph<V> {
    inner: Graph<GraphNode, GraphEdge<V>>,
    trigger_index: NodeIndex,
}

impl<V> ExecutionGraph<V> {
    pub fn new(inner: Graph<GraphNode, GraphEdge<V>>, trigger_index: NodeIndex) -> Self {
        Self {
            inner,
            trigger_index,
        }
    }

    /// Validates the graph to ensure it is a Direct Acyclic Graph (DAG) function wise.
    pub fn validate_dag(&self) -> Result<(), TdError> {
        let partial = PartialGraph::trigger_graph(self)?;
        toposort(&partial.inner, None).map_err(|err| {
            let function = &partial.inner[err.node_id()];
            GraphError::Cyclic(function.name().clone())
        })?;
        Ok(())
    }

    /// Validates the graph to ensure it is a Direct Acyclic Graph (DAG) transaction wise.
    pub fn validate_transaction(
        &self,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<(), TdError> {
        let partial = PartialGraph::transaction_graph(self, transaction_mapper)?;
        toposort(&partial.inner, None).map_err(|err| {
            let key = &partial.inner[err.node_id()];
            match transaction_mapper.transaction_by() {
                Ok(transaction_by) => {
                    GraphError::CyclicTransaction(transaction_by, key.into()).into()
                }
                Err(err) => err,
            }
        })?;
        Ok(())
    }
}

impl<V: Display> ExecutionGraph<V> {
    /// Generates a DOT representation of the graph.
    pub fn dot(&self) -> Dot<&Graph<GraphNode, GraphEdge<V>>> {
        Dot::with_attr_getters(
            &self.inner,
            &[Config::EdgeNoLabel],
            &|_, edge| match edge.weight() {
                GraphEdge::Output { .. } => String::new(),
                GraphEdge::Trigger { .. } => String::new(),
                GraphEdge::Dependency { versions, .. } => {
                    format!("label=\"{}\"", versions)
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
#[derive(Getters)]
#[getset(get = "pub")]
pub struct PartialGraph<F> {
    inner: Graph<F, ()>,
}

impl<F> PartialGraph<F> {
    /// Creates a partial graph containing only trigger edges.
    pub fn trigger_graph(
        original: &ExecutionGraph<F>,
    ) -> Result<PartialGraph<&FunctionVersionNode>, TdError> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Add nodes we want to keep (function nodes)
        for node_idx in original.inner().node_indices() {
            let node = &original.inner()[node_idx];
            if let GraphNode::Function(node) = node {
                let new_idx = new_graph.add_node(node);
                node_map.insert(node_idx, new_idx);
            }
        }

        // Rewire edges
        for node_idx in original.inner().node_indices() {
            let node = &original.inner()[node_idx];
            if let GraphNode::Table(_) = node {
                // For each ignored node, connect its predecessors to successors
                let preds: Vec<_> = original
                    .inner()
                    .edges_directed(node_idx, petgraph::Direction::Incoming)
                    .filter(|edge| !matches!(edge.weight(), GraphEdge::Dependency { .. }))
                    .map(|edge| edge.source())
                    .collect();

                let succs: Vec<_> = original
                    .inner()
                    .edges_directed(node_idx, petgraph::Direction::Outgoing)
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

    /// Creates a trigger transaction graph.
    pub fn transaction_graph(
        dgraph: &ExecutionGraph<F>,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<PartialGraph<TransactionKey>, TdError> {
        let trigger_graph = PartialGraph::trigger_graph(dgraph)?;

        let mut new_graph = DiGraph::new();

        let mut node_map = HashMap::new();
        for node in trigger_graph.inner().node_indices() {
            let key = transaction_mapper.key(trigger_graph.inner()[node])?;
            node_map
                .entry(key.clone())
                .or_insert_with(|| new_graph.add_node(key));
        }

        let mut edge_map = HashMap::new();
        for edge in trigger_graph.inner().edge_references() {
            let source_key = transaction_mapper.key(trigger_graph.inner()[edge.source()])?;
            let target_key = transaction_mapper.key(trigger_graph.inner()[edge.target()])?;
            let source = node_map[&source_key];
            let target = node_map[&target_key];
            if source != target && !edge_map.contains_key(&(source, target)) {
                new_graph.add_edge(source, target, ());
                edge_map.insert((source, target), ());
            }
        }

        Ok(PartialGraph { inner: new_graph })
    }

    /// Generates a DOT representation of the partial graph.
    #[allow(dead_code)]
    pub fn dot(&self) -> Dot<&Graph<F, ()>> {
        Dot::with_attr_getters(
            &self.inner,
            &[Config::EdgeNoLabel],
            &|_, _| String::new(),
            &|_, _| String::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::graphs::GraphBuilder;
    use crate::graphs::{GraphEdge, GraphNode, PartialGraph, TransactionKey};
    use crate::test_utils::graph::test_graph;
    use crate::test_utils::transaction::TestTransactionBy;
    use petgraph::visit::EdgeRef;
    use std::collections::HashSet;
    use td_objects::types::test_utils::execution::{
        dependency, function_node, table, table_node, trigger, FUNCTION_NAMES, TABLE_NAMES,
    };

    #[test]
    fn test_graph_builder_new() {
        let output_tables = vec![];
        let input_tables = vec![];
        let trigger_graph = vec![];

        let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
        assert_eq!(builder.trigger_graph, &trigger_graph);
        assert_eq!(builder.output_tables, &output_tables);
        assert_eq!(builder.input_tables, &input_tables);
    }

    #[tokio::test]
    async fn test_graph_builder_build() {
        let (graph, trigger_function) = test_graph().await;
        assert_eq!(
            GraphNode::Function(trigger_function),
            graph.inner()[graph.trigger_index]
        );
        assert_eq!(
            graph.inner.node_count(),
            FUNCTION_NAMES.len() + TABLE_NAMES.len()
        );
        // There is 1 trigger edge, 1 dependency edge and 3 output edges
        assert_eq!(graph.inner.edge_count(), 5);
    }

    #[tokio::test]
    async fn test_graph_nodes() {
        let (graph, _) = test_graph().await;

        let mut table_nodes = TABLE_NAMES.iter().fold(HashSet::new(), |mut acc, table| {
            acc.insert(GraphNode::Table(table_node(table)));
            acc
        });

        let mut function_nodes = FUNCTION_NAMES
            .iter()
            .fold(HashSet::new(), |mut acc, function| {
                acc.insert(GraphNode::Function(function_node(function)));
                acc
            });

        for node in graph.inner.node_indices() {
            let node = &graph.inner()[node];
            match node {
                GraphNode::Function(_) => assert!(function_nodes.remove(node)),
                GraphNode::Table(_) => assert!(table_nodes.remove(node)),
            }
        }

        assert!(function_nodes.is_empty());
        assert!(table_nodes.is_empty());
    }

    #[tokio::test]
    async fn test_graph_edges_types() {
        let (graph, _) = test_graph().await;

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
        let (graph, _) = test_graph().await;

        for node in graph.inner.node_indices() {
            let edges = graph.inner.edges(node);
            for edge in edges {
                let source = &graph.inner[edge.source()];
                let target = &graph.inner[edge.target()];
                match (source, target) {
                    (GraphNode::Function(f), GraphNode::Table(t)) => match (f.name(), t.name()) {
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
                        _ => unreachable!(),
                    },
                    (GraphNode::Table(t), GraphNode::Function(f)) => match (t.name(), f.name()) {
                        (table, function)
                            if table == &TABLE_NAMES[0] && function == &FUNCTION_NAMES[1] =>
                        {
                            assert!(matches!(
                                edge.weight(),
                                GraphEdge::Dependency { .. } | GraphEdge::Trigger { .. }
                            ));
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_trigger_graph() {
        let (graph, _) = test_graph().await;
        let trigger_graph = PartialGraph::trigger_graph(&graph).unwrap();
        assert_eq!(trigger_graph.inner.node_count(), 2);
        assert_eq!(trigger_graph.inner.edge_count(), 1);

        let mut function_nodes = HashSet::from([
            function_node(&FUNCTION_NAMES[0]),
            function_node(&FUNCTION_NAMES[1]),
        ]);

        for node in trigger_graph.inner.node_indices() {
            let node = &trigger_graph.inner()[node];
            assert!(function_nodes.remove(node));
        }
        assert!(function_nodes.is_empty());

        let edges: Vec<_> = trigger_graph.inner.edge_references().collect();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        let source = trigger_graph.inner[edge.source()];
        assert_eq!(*source.name(), FUNCTION_NAMES[0]);
        let target = trigger_graph.inner[edge.target()];
        assert_eq!(*target.name(), FUNCTION_NAMES[1]);
    }

    #[tokio::test]
    async fn test_graph_validate_dag_ok() {
        let (graph, _) = test_graph().await;
        assert!(graph.validate_dag().is_ok());

        let output_tables = vec![
            table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
            table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        ];
        let input_tables = vec![
            // this creates cycles dependency wise (which is ok)
            dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await,
            dependency(&TABLE_NAMES[1], &FUNCTION_NAMES[0]).await,
            dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await,
            dependency(&TABLE_NAMES[1], &FUNCTION_NAMES[1]).await,
        ];
        let trigger_graph = vec![trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await];

        let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
        let trigger_function = function_node(&FUNCTION_NAMES[0]);
        let graph = builder.build(trigger_function.clone()).unwrap();
        assert!(graph.validate_dag().is_ok());
    }

    #[tokio::test]
    async fn test_graph_validate_dag_err() {
        let output_tables = vec![
            table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
            table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        ];
        let input_tables = vec![];
        let trigger_graph = vec![
            // This creates a cycle trigger wise (which is not ok)
            trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await,
            trigger(&TABLE_NAMES[1], &FUNCTION_NAMES[0]).await,
        ];

        let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
        let trigger_function = function_node(&FUNCTION_NAMES[0]);

        let graph = builder.build(trigger_function.clone()).unwrap();
        assert!(graph.validate_dag().is_err());
    }

    #[tokio::test]
    async fn test_transaction_graph() {
        let (graph, _) = test_graph().await;

        let transaction_by = TestTransactionBy::Name;
        let transaction_graph = PartialGraph::transaction_graph(&graph, &transaction_by).unwrap();
        assert_eq!(transaction_graph.inner.node_count(), 2);
        assert_eq!(transaction_graph.inner.edge_count(), 1);

        let mut function_nodes = HashSet::from([
            TransactionKey::try_from(FUNCTION_NAMES[0].to_string()).unwrap(),
            TransactionKey::try_from(FUNCTION_NAMES[1].to_string()).unwrap(),
        ]);

        for node in transaction_graph.inner.node_indices() {
            let node = &transaction_graph.inner()[node];
            assert!(function_nodes.remove(node));
        }
        assert!(function_nodes.is_empty());

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
        let node = &transaction_graph.inner()[node];
        assert_eq!(*node, TransactionKey::try_from("S").unwrap());
    }

    #[tokio::test]
    async fn test_graph_validate_transaction() {
        // Valid transaction graph, both function and single transaction
        let (graph, _) = test_graph().await;
        assert!(graph.validate_transaction(&TestTransactionBy::Name).is_ok());
        assert!(graph
            .validate_transaction(&TestTransactionBy::Single)
            .is_ok());

        let output_tables = vec![
            table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await,
            table(&FUNCTION_NAMES[1], &TABLE_NAMES[1]).await,
        ];
        let input_tables = vec![];
        let trigger_graph = vec![
            // This created a cycle if transaction is at function level, but not at a graph level
            trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[1]).await,
            trigger(&TABLE_NAMES[1], &FUNCTION_NAMES[0]).await,
        ];

        let builder = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables);
        let trigger_function = function_node(&FUNCTION_NAMES[0]);
        let graph = builder.build(trigger_function.clone()).unwrap();
        assert!(graph
            .validate_transaction(&TestTransactionBy::Name)
            .is_err());
        assert!(graph
            .validate_transaction(&TestTransactionBy::Single)
            .is_ok());
    }
}
