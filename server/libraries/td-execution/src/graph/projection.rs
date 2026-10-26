//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::TopologyGraph;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::prelude::{DiGraph, EdgeRef};
use std::collections::HashMap;
use ta_execution::TransactionMapper;
use td_error::TdError;
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode};
use td_objects::types::basic::TransactionKey;

/// One kind of node of a graph, with an edge wherever the graph connects two
/// of them through the other kind; the plan checks cycles on these.
pub struct Projection<F> {
    pub inner: Graph<F, ()>,
}

impl<F> Projection<F> {
    pub fn triggers<T, V>(original: &TopologyGraph<F, T, V>) -> Result<Projection<&F>, TdError> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Function(node) = node {
                let new_idx = new_graph.add_node(node);
                node_map.insert(node_idx, new_idx);
            }
        }

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

        Ok(Projection { inner: new_graph })
    }

    pub fn dependencies<T, V>(
        original: &TopologyGraph<F, T, V>,
        drop_self_dependency: bool,
    ) -> Result<Projection<&T>, TdError> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        for node_idx in original.inner.node_indices() {
            let node = &original.inner[node_idx];
            if let GraphNode::Table(node) = node {
                let new_idx = new_graph.add_node(node);
                node_map.insert(node_idx, new_idx);
            }
        }

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

        Ok(Projection { inner: new_graph })
    }
}

impl Projection<FunctionNode> {
    pub fn transactions<T, V>(
        dgraph: &TopologyGraph<FunctionNode, T, V>,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<Projection<TransactionKey>, TdError> {
        let triggers = Projection::triggers(dgraph)?;
        let roots = dgraph.transaction_roots();

        let mut new_graph = DiGraph::new();

        let mut node_map = HashMap::new();
        for node in triggers.inner.node_indices() {
            let key = roots.key(transaction_mapper, triggers.inner[node])?;
            node_map
                .entry(key.clone())
                .or_insert_with(|| new_graph.add_node(key));
        }

        let mut edge_map = HashMap::new();
        for edge in triggers.inner.edge_references() {
            let source_key = roots.key(transaction_mapper, triggers.inner[edge.source()])?;
            let target_key = roots.key(transaction_mapper, triggers.inner[edge.target()])?;
            let source = node_map[&source_key];
            let target = node_map[&target_key];
            if source != target && !edge_map.contains_key(&(source, target)) {
                new_graph.add_edge(source, target, ());
                edge_map.insert((source, target), ());
            }
        }

        Ok(Projection { inner: new_graph })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        TestTransactionBy, expected_function_nodes, expected_table_nodes, test_graph,
    };
    use crate::topology::ProjectTopology;
    use itertools::Itertools;
    use td_objects::test_utils::graph::{
        FUNCTION_NAMES, TABLE_NAMES, collection, dependency, function, table, version,
    };
    use td_objects::types::basic::FunctionVersionId;

    #[tokio::test]
    async fn test_triggers() {
        let graph = test_graph();
        let triggers = Projection::triggers(&graph).unwrap();
        assert_eq!(triggers.inner.node_count(), 3);
        assert_eq!(triggers.inner.edge_count(), 1);

        let function_nodes = triggers
            .inner
            .node_indices()
            .map(|node| triggers.inner[node].clone())
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        let expected_function_nodes = expected_function_nodes()
            .into_iter()
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(function_nodes, expected_function_nodes);

        let edges: Vec<_> = triggers.inner.edge_references().collect();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        let source = triggers.inner[edge.source()];
        assert_eq!(source.name, FUNCTION_NAMES[0]);
        let target = triggers.inner[edge.target()];
        assert_eq!(target.name, FUNCTION_NAMES[1]);
    }

    #[tokio::test]
    async fn test_dependencies() {
        let graph = test_graph();
        let dependencies = Projection::dependencies(&graph, true).unwrap();
        assert_eq!(dependencies.inner.node_count(), TABLE_NAMES.len());
        assert_eq!(dependencies.inner.edge_count(), 3);

        let table_nodes = dependencies
            .inner
            .node_indices()
            .map(|node| dependencies.inner[node].clone())
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        let expected_table_nodes = expected_table_nodes()
            .into_iter()
            .sorted_by_key(|k| k.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(table_nodes, expected_table_nodes);

        let edges: Vec<_> = dependencies.inner.edge_references().collect();
        assert_eq!(edges.len(), 3);

        let edge = edges[0];
        let source = dependencies.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[0]);
        let target = dependencies.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[2]);

        let edge = edges[1];
        let source = dependencies.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[0]);
        let target = dependencies.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[1]);

        let edge = edges[2];
        let source = dependencies.inner[edge.source()];
        assert_eq!(source.name, TABLE_NAMES[1]);
        let target = dependencies.inner[edge.target()];
        assert_eq!(target.name, TABLE_NAMES[3]);
    }

    /// An updated function carries a new version id while its tables keep the
    /// version that defined them; its dependency on its own output is still a
    /// self dependency, so the dependency DAG stays acyclic.
    #[tokio::test]
    async fn test_dependencies_self_dependency_across_versions() {
        let mut updated = function(&FUNCTION_NAMES[0]);
        updated.id = FunctionVersionId::new();
        let graph = ProjectTopology::new(
            version(),
            vec![collection()],
            vec![],
            vec![updated],
            HashMap::new(),
            vec![table(&FUNCTION_NAMES[0], &TABLE_NAMES[0])],
            vec![],
            vec![dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0])],
        )
        .graph()
        .unwrap();

        let self_dependencies = graph
            .inner
            .edge_references()
            .filter(|e| e.weight().is_self_dependency())
            .count();
        assert_eq!(self_dependencies, 1);

        let dependencies = Projection::dependencies(&graph, true).unwrap();
        assert_eq!(dependencies.inner.node_count(), 1);
        assert_eq!(dependencies.inner.edge_count(), 0);
    }

    #[tokio::test]
    async fn test_transactions() {
        let graph = test_graph();

        let transaction_by = TestTransactionBy::Name;
        let transactions = Projection::transactions(&graph, &transaction_by).unwrap();
        assert_eq!(transactions.inner.node_count(), 3);
        assert_eq!(transactions.inner.edge_count(), 1);

        let expected_trx_nodes = vec![
            TransactionKey::try_from(FUNCTION_NAMES[0].to_string()).unwrap(),
            TransactionKey::try_from(FUNCTION_NAMES[1].to_string()).unwrap(),
            TransactionKey::try_from(FUNCTION_NAMES[2].to_string()).unwrap(),
        ];
        let trx_nodes = transactions
            .inner
            .node_indices()
            .map(|node| transactions.inner[node].clone())
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(expected_trx_nodes, trx_nodes);

        let edges: Vec<_> = transactions.inner.edge_references().collect();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        let source = &transactions.inner[edge.source()];
        assert_eq!(
            *source,
            TransactionKey::try_from(FUNCTION_NAMES[0].to_string()).unwrap()
        );
        let target = &transactions.inner[edge.target()];
        assert_eq!(
            *target,
            TransactionKey::try_from(FUNCTION_NAMES[1].to_string()).unwrap()
        );

        let transaction_by = TestTransactionBy::Single;
        let transactions = Projection::transactions(&graph, &transaction_by).unwrap();
        assert_eq!(transactions.inner.node_count(), 1);
        assert_eq!(transactions.inner.edge_count(), 0);

        let node = transactions.inner.node_indices().next().unwrap();
        let node = &transactions.inner[node];
        assert_eq!(*node, TransactionKey::try_from("S").unwrap());
    }
}
