//
// Copyright 2025 Tabsdata Inc.
//

use futures::future::try_join_all;
use petgraph::Graph;
use petgraph::dot::{Config, Dot};
use petgraph::prelude::EdgeRef;
use petgraph::visit::IntoNodeReferences;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use ta_execution::TransactionMapper;
use td_error::TdError;
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode, TableNode};
use td_objects::types::basic::{FunctionId, TransactionKey};
use td_objects::types::table_ref::Versions;

/// Catalog rows of a topology version as a graph, the whole project or a part
/// of it. There are two types of nodes: functions and tables.
/// The edges are of three types:
/// 1. Output: from function to table, which marks the tables created.
/// 2. Trigger: from table to function, which marks the functions triggered.
/// 3. Dependency: from table to function, which marks which functions depend on the table.
///
/// For 1, 2 and 3, it contains the versions of the table going in or out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyGraph<F = FunctionNode, T = TableNode, V = Versions> {
    #[serde(flatten)]
    pub inner: Graph<GraphNode<F, T>, GraphEdge<V>>,
}

impl<F, T, V> TopologyGraph<F, T, V>
where
    F: Eq + Hash,
    T: Eq + Hash,
{
    pub fn functions(&self) -> HashSet<&F> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Function(function) => Some(function),
                _ => None,
            })
            .collect()
    }

    pub fn tables(&self) -> HashSet<&T> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Table(table) => Some(table),
                _ => None,
            })
            .collect()
    }

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

impl<T, V> TopologyGraph<FunctionNode, T, V> {
    pub fn transaction_roots(&self) -> TransactionRoots<'_> {
        TransactionRoots {
            functions: self
                .inner
                .node_weights()
                .filter_map(|n| match n {
                    GraphNode::Function(f) => Some((f.function_id, f)),
                    _ => None,
                })
                .collect(),
        }
    }
}

/// A function runs in its parent's transaction when the parent is in the
/// graph, and in its own otherwise.
pub struct TransactionRoots<'a> {
    functions: HashMap<FunctionId, &'a FunctionNode>,
}

impl TransactionRoots<'_> {
    pub fn root<'b>(&'b self, node: &'b FunctionNode) -> &'b FunctionNode {
        node.parent_function_id
            .as_ref()
            .and_then(|parent| self.functions.get(&**parent).copied())
            .unwrap_or(node)
    }

    pub fn key(
        &self,
        mapper: &impl TransactionMapper,
        node: &FunctionNode,
    ) -> Result<TransactionKey, TdError> {
        mapper.key(self.root(node))
    }
}

impl<F: Display, T: Display, V: Display> TopologyGraph<F, T, V> {
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

impl<F, T, V> TopologyGraph<F, T, V> {
    pub async fn map<'a, CN, RN, CE, RE, FF, TT, VV>(
        &'a self,
        mut transform_nodes: CN,
        mut transform_edges: CE,
    ) -> Result<TopologyGraph<FF, TT, VV>, TdError>
    where
        CN: FnMut(&'a GraphNode<F, T>) -> RN,
        RN: Future<Output = Result<GraphNode<FF, TT>, TdError>>,
        CE: FnMut(&'a GraphEdge<V>, &'a GraphNode<F, T>, &'a GraphNode<F, T>) -> RE,
        RE: Future<Output = Result<VV, TdError>>,
    {
        let mut node_futures = Vec::new();
        for (idx, node) in self.inner.node_references() {
            let fut = transform_nodes(node);
            node_futures.push(async move {
                let new_node = fut.await?;
                Ok::<_, TdError>((idx, new_node))
            });
        }
        let mut updated_nodes = try_join_all(node_futures)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let mut edge_futures = Vec::new();
        for edge in self.inner.edge_references() {
            let source = &self.inner[edge.source()];
            let target = &self.inner[edge.target()];
            let fut = transform_edges(edge.weight(), source, target);
            edge_futures.push(async move {
                let new_version = fut.await?;
                Ok::<_, TdError>((edge.id(), new_version))
            });
        }
        let mut updated_versions = try_join_all(edge_futures)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let new_graph = self.inner.map(
            |node_idx, _| updated_nodes.remove(&node_idx).unwrap(),
            |idx, edge_weight| {
                let new_version = updated_versions.remove(&idx).unwrap();
                edge_weight.versioned(new_version)
            },
        );

        Ok(TopologyGraph { inner: new_graph })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{expected_function_nodes, expected_table_nodes, test_graph};
    use td_objects::test_utils::graph::{FUNCTION_NAMES, TABLE_NAMES, function_node};
    use td_objects::types::basic::{FunctionId, Parent};

    #[tokio::test]
    async fn test_functions() -> Result<(), TdError> {
        let graph = test_graph();
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
        let graph = test_graph();
        let actual = graph.tables().into_iter().cloned().collect::<HashSet<_>>();
        let expected = expected_table_nodes();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_edges_types() {
        let graph = test_graph();

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
        let graph = test_graph();

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
        let graph = test_graph();
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
    async fn test_map_graph_versioned_structure() -> Result<(), TdError> {
        let graph = test_graph();
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

    #[test]
    fn test_transaction_root_is_the_parent_when_in_the_graph() {
        let graph = test_graph();
        let parent = function_node(&FUNCTION_NAMES[0]);
        let mut child = function_node(&FUNCTION_NAMES[1]);
        child.parent_function_id = Some(Parent::from(parent.function_id));

        assert_eq!(graph.transaction_roots().root(&child), &parent);
    }

    #[test]
    fn test_transaction_root_is_itself_without_a_parent_in_the_graph() {
        let graph = test_graph();
        let mut child = function_node(&FUNCTION_NAMES[1]);
        child.parent_function_id = Some(Parent::from(FunctionId::default()));
        assert_eq!(graph.transaction_roots().root(&child), &child);

        let orphan = function_node(&FUNCTION_NAMES[1]);
        assert_eq!(graph.transaction_roots().root(&orphan), &orphan);
    }
}
