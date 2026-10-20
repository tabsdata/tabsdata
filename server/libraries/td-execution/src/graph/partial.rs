//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::TdGraph;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::prelude::{DiGraph, EdgeRef};
use std::collections::HashMap;
use ta_execution::TransactionMapper;
use td_error::TdError;
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode};
use td_objects::types::basic::TransactionKey;

/// Represents a partial graph used for validation.
pub struct PartialGraph<F> {
    pub inner: Graph<F, ()>,
}

impl<F> PartialGraph<F> {
    pub fn trigger_graph<T, V>(original: &TdGraph<F, T, V>) -> Result<PartialGraph<&F>, TdError> {
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

        Ok(PartialGraph { inner: new_graph })
    }

    pub fn dependency_graph<T, V>(
        original: &TdGraph<F, T, V>,
        drop_self_dependency: bool,
    ) -> Result<PartialGraph<&T>, TdError> {
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

        Ok(PartialGraph { inner: new_graph })
    }
}

impl PartialGraph<FunctionNode> {
    pub fn transaction_graph<T, V>(
        dgraph: &TdGraph<FunctionNode, T, V>,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<PartialGraph<TransactionKey>, TdError> {
        let trigger_graph = PartialGraph::trigger_graph(dgraph)?;
        let roots = dgraph.transaction_roots();

        let mut new_graph = DiGraph::new();

        let mut node_map = HashMap::new();
        for node in trigger_graph.inner.node_indices() {
            let key = roots.key(transaction_mapper, trigger_graph.inner[node])?;
            node_map
                .entry(key.clone())
                .or_insert_with(|| new_graph.add_node(key));
        }

        let mut edge_map = HashMap::new();
        for edge in trigger_graph.inner.edge_references() {
            let source_key = roots.key(transaction_mapper, trigger_graph.inner[edge.source()])?;
            let target_key = roots.key(transaction_mapper, trigger_graph.inner[edge.target()])?;
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
    use crate::graph::TdGraphBuilder;
    use crate::test_utils::{
        TestTransactionBy, expected_function_nodes, expected_table_nodes, test_graph,
    };
    use itertools::Itertools;
    use td_objects::test_utils::graph::{FUNCTION_NAMES, TABLE_NAMES, dependency, function, table};
    use td_objects::types::basic::FunctionVersionId;

    #[tokio::test]
    async fn test_trigger_graph() {
        let graph = test_graph();
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
        let graph = test_graph();
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

    /// An updated function carries a new version id while its tables keep the
    /// version that defined them; its dependency on its own output is still a
    /// self dependency, so the dependency DAG stays acyclic.
    #[tokio::test]
    async fn test_dependency_graph_self_dependency_across_versions() {
        let mut updated = function(&FUNCTION_NAMES[0]);
        updated.base.base.id = FunctionVersionId::new();
        let all_tables = vec![table(&FUNCTION_NAMES[0], &TABLE_NAMES[0])];
        let all_tables_map = all_tables
            .iter()
            .map(|t| (t.base.base.table_id, t.clone()))
            .collect();
        let all_functions_map = [(updated.base.base.function_id, updated.clone())]
            .into_iter()
            .collect();
        let dependencies = vec![dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0])];
        let graph = TdGraphBuilder::new(&all_tables_map, &all_functions_map)
            .with_dependencies(&dependencies)
            .unwrap()
            .with_outputs(&all_tables)
            .unwrap()
            .build();

        let self_dependencies = graph
            .inner
            .edge_references()
            .filter(|e| e.weight().is_self_dependency())
            .count();
        assert_eq!(self_dependencies, 1);

        let dependency_graph = PartialGraph::dependency_graph(&graph, true).unwrap();
        assert_eq!(dependency_graph.inner.node_count(), 1);
        assert_eq!(dependency_graph.inner.edge_count(), 0);
    }

    #[tokio::test]
    async fn test_transaction_graph() {
        let graph = test_graph();

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
}
