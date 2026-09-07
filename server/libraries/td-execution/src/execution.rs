//
// Copyright 2025 Tabsdata Inc.
//

use petgraph::algo::toposort;
use petgraph::prelude::{Direction, EdgeRef, NodeIndex};
use petgraph::visit::{IntoNodeReferences, VisitMap, Visitable};
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use td_error::{TdError, td_error};
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode, ResolvedVersion, TableNode};
use td_objects::graph::{MapGraph, PartialGraph, TdGraph};
use td_objects::transaction::map::TransactionMapper;
use td_objects::types::basic::{FunctionName, TableName, TransactionByStr, TransactionKey};
use td_objects::types::table_ref::Versions;

#[td_error]
enum ExecutionPlannerError {
    #[error("Graph is not a Directed Acyclic Graph trigger wise. Cycle found in function: {0}")]
    CyclicTrigger(FunctionName) = 0,
    #[error(
        "Graph is not a Directed Acyclic Graph dependency wise. A dependency cycle is only allowed by its own function. Cycle found in table: {0}"
    )]
    CyclicDependency(TableName) = 1,
    #[error(
        "Graph is not a transactional Directed Acyclic Graph with transaction mode: {0:?}. Cycle found in transaction: {1}"
    )]
    CyclicTransaction(TransactionByStr, TransactionKey) = 2,
    #[error("Trigger function '{0}' not found in execution plan.")]
    InvalidTrigger(String) = 5000,
}

/// The `ExecutionPlanner` trait is used to define the execution plan for the type implementing it.
pub trait ExecutionPlanner<F, T, V> {
    fn with_trigger(&self, trigger: &F) -> Result<ExecutionPlan<F, T, V>, TdError>;
}

impl<F, T, V> ExecutionPlanner<F, T, V> for TdGraph<F, T, V>
where
    F: Clone + Eq + Display,
    T: Clone,
    V: Clone,
{
    fn with_trigger(&self, trigger: &F) -> Result<ExecutionPlan<F, T, V>, TdError> {
        // Find trigger index in the graph
        let trigger_index = self
            .inner
            .node_references()
            .find_map(|(index, node)| {
                if let GraphNode::Function(function) = node
                    && function == trigger
                {
                    Some(index)
                } else {
                    None
                }
            })
            .ok_or_else(|| ExecutionPlannerError::InvalidTrigger(trigger.to_string()))?;

        // Find all nodes in the trigger graph
        let mut visited = self.inner.visit_map();
        let mut nodes_in_trigger_path = HashSet::new();

        let mut stack = vec![trigger_index];
        while let Some(node) = stack.pop() {
            if visited.is_visited(&node) {
                continue;
            }

            visited.visit(node);
            nodes_in_trigger_path.insert(node);
            for edge in self.inner.edges(node) {
                if let GraphEdge::Output { .. } | GraphEdge::Trigger { .. } = edge.weight() {
                    stack.push(edge.target());
                }
            }
        }

        Ok(ExecutionPlan {
            graph: self.clone(),
            trigger_index,
            nodes_in_trigger_path,
        })
    }
}

#[derive(Debug)]
pub struct ExecutionPlan<F = FunctionNode, T = TableNode, V = Versions> {
    pub graph: TdGraph<F, T, V>,
    pub trigger_index: NodeIndex,
    pub nodes_in_trigger_path: HashSet<NodeIndex>,
}

pub type ResolvedExecutionPlan = ExecutionPlan<FunctionNode, TableNode, ResolvedVersion>;

impl<F, T, V> Deref for ExecutionPlan<F, T, V> {
    type Target = TdGraph<F, T, V>;
    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl<F, T, V> DerefMut for ExecutionPlan<F, T, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}

impl<T, V> ExecutionPlan<FunctionNode, T, V> {
    pub fn validate_dag_trigger(&self) -> Result<(), TdError> {
        let partial = PartialGraph::trigger_graph(self)?;
        toposort(&partial.inner, None).map_err(|err| {
            let function = &partial.inner[err.node_id()];
            ExecutionPlannerError::CyclicTrigger(function.name.clone())
        })?;
        Ok(())
    }

    pub fn validate_dag_transaction(
        &self,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<(), TdError> {
        let partial = PartialGraph::transaction_graph(self, transaction_mapper)?;
        toposort(&partial.inner, None).map_err(|err| {
            let key = &partial.inner[err.node_id()];
            match transaction_mapper.transaction_by() {
                Ok(transaction_by) => {
                    ExecutionPlannerError::CyclicTransaction(transaction_by, key.clone()).into()
                }
                Err(err) => err,
            }
        })?;
        Ok(())
    }
}

impl<F, V> ExecutionPlan<F, TableNode, V> {
    pub fn validate_dag_dependency(&self) -> Result<(), TdError> {
        // drop self dependencies as those cycles are allowed, and toposort errors if present
        let partial = PartialGraph::dependency_graph(self, true)?;
        toposort(&partial.inner, None).map_err(|err| {
            let table = &partial.inner[err.node_id()];
            ExecutionPlannerError::CyclicDependency(table.name.clone())
        })?;
        Ok(())
    }
}

impl<F, T, V> ExecutionPlan<F, T, V>
where
    F: Eq + Hash,
    T: Eq + Hash,
{
    pub fn main_trigger(&self) -> &F {
        match &self.graph.inner[self.trigger_index] {
            GraphNode::Function(f) => f,
            GraphNode::Table(_) => unreachable!("trigger_index must point to a Function node"),
        }
    }

    pub fn triggered_functions(&self) -> HashSet<&F> {
        self.inner
            .node_references()
            .filter(|(idx, _)| self.nodes_in_trigger_path.contains(idx))
            .filter_map(|(_, node)| {
                if let GraphNode::Function(function) = node {
                    Some(function)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn created_tables(&self) -> HashSet<&T> {
        self.nodes_in_trigger_path
            .iter()
            .filter_map(|node| {
                let node = &self.inner[*node];
                if let GraphNode::Table(table) = node {
                    Some(table)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn function_table_input_versions(&self) -> HashSet<(&F, &T, &GraphEdge<V>)>
    where
        V: Eq + Hash,
    {
        self.nodes_in_trigger_path
            .iter()
            .flat_map(|index| {
                self.inner
                    .edges_directed(*index, Direction::Incoming)
                    .filter_map(|edge| {
                        let source = &self.inner[edge.source()];
                        let target = &self.inner[edge.target()];
                        match (source, target) {
                            (GraphNode::Table(table), GraphNode::Function(function)) => {
                                Some((function, table, edge.weight()))
                            }
                            _ => None,
                        }
                    })
            })
            .collect()
    }

    pub fn table_function_output_versions(&self) -> HashSet<(&T, &F, &GraphEdge<V>)>
    where
        V: Eq + Hash,
    {
        self.nodes_in_trigger_path
            .iter()
            .flat_map(|index| {
                self.inner
                    .edges_directed(*index, Direction::Outgoing)
                    .filter_map(|edge| {
                        let source = &self.inner[edge.source()];
                        let target = &self.inner[edge.target()];
                        match (source, target) {
                            (GraphNode::Function(function), GraphNode::Table(table)) => {
                                Some((table, function, edge.weight()))
                            }
                            _ => None,
                        }
                    })
            })
            .collect()
    }
}

impl<F, T, V> MapGraph<F, T, V> for ExecutionPlan<F, T, V> {
    type Output<FF, TT, VV> = ExecutionPlan<FF, TT, VV>;

    async fn map<'a, CN, RN, CE, RE, FF, TT, VV>(
        &'a self,
        transform_nodes: CN,
        transform_edges: CE,
    ) -> Result<Self::Output<FF, TT, VV>, TdError>
    where
        CN: FnMut(&'a GraphNode<F, T>) -> RN,
        RN: Future<Output = Result<GraphNode<FF, TT>, TdError>>,
        CE: FnMut(&'a GraphEdge<V>, &'a GraphNode<F, T>, &'a GraphNode<F, T>) -> RE,
        RE: Future<Output = Result<VV, TdError>>,
        F: 'a,
        T: 'a,
        V: 'a,
    {
        let new_graph = self.graph.map(transform_nodes, transform_edges).await?;
        Ok(ExecutionPlan {
            graph: new_graph,
            trigger_index: self.trigger_index,
            nodes_in_trigger_path: self.nodes_in_trigger_path.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_error::TdError;
    use td_objects::test_utils::graph::{
        FUNCTION_NAMES, TABLE_NAMES, function_node, table_node, test_graph,
    };
    use td_objects::test_utils::transaction::TestTransactionBy;

    #[tokio::test]
    async fn test_execution_plan_map_async() {
        let graph = test_graph();
        let graph = graph
            .with_trigger(&function_node(&FUNCTION_NAMES[0]))
            .unwrap();

        let mapped = graph
            .map(
                |node| async move {
                    match node {
                        GraphNode::Function(f) => Ok(GraphNode::Function(f.clone())),
                        GraphNode::Table(t) => Ok(GraphNode::Table(t.clone())),
                    }
                },
                |edge, _src, _dst| async move { Ok(edge.versions().clone()) },
            )
            .await
            .expect("map should succeed");

        // Check that the structure is preserved
        assert_eq!(mapped.trigger_index, graph.trigger_index);
        assert_eq!(mapped.nodes_in_trigger_path, graph.nodes_in_trigger_path);

        // Check that all nodes are present and have the same names
        for (idx, node) in mapped.inner.node_references() {
            let orig_node = &graph.inner[idx];
            match (orig_node, node) {
                (GraphNode::Function(f1), GraphNode::Function(f2)) => assert_eq!(f1.name, f2.name),
                (GraphNode::Table(t1), GraphNode::Table(t2)) => assert_eq!(t1.name, t2.name),
                _ => panic!("Node type mismatch"),
            }
        }
        // Check that all edges are present and have the same weights (multigraph-safe)
        let mut mapped_edges: Vec<_> = mapped
            .inner
            .edge_references()
            .map(|e| (e.source(), e.target(), format!("{:?}", e.weight())))
            .collect();
        let mut orig_edges: Vec<_> = graph
            .inner
            .edge_references()
            .map(|e| (e.source(), e.target(), format!("{:?}", e.weight())))
            .collect();
        mapped_edges.sort();
        orig_edges.sort();
        assert_eq!(
            mapped_edges, orig_edges,
            "Edge sets (with weights) do not match"
        );
        // Assert that the number of edges is the same
        assert_eq!(
            mapped.inner.edge_count(),
            graph.inner.edge_count(),
            "Edge count mismatch"
        );
        // Assert that the number of nodes is the same
        assert_eq!(
            mapped.inner.node_count(),
            graph.inner.node_count(),
            "Node count mismatch"
        );
        // Assert that all node indices in the mapped graph exist in the original graph
        for idx in mapped.inner.node_indices() {
            assert!(
                graph.inner.node_indices().any(|i| i == idx),
                "Node index {:?} missing in original graph",
                idx
            );
        }
    }

    #[tokio::test]
    async fn test_graph_validate_dag_trigger_ok() {
        let graph = test_graph();
        let graph = graph
            .with_trigger(&function_node(&FUNCTION_NAMES[0]))
            .unwrap();
        assert!(graph.validate_dag_trigger().is_ok());
    }

    #[tokio::test]
    async fn test_graph_validate_dag_dependency_ok() {
        let graph = test_graph();
        let graph = graph
            .with_trigger(&function_node(&FUNCTION_NAMES[0]))
            .unwrap();
        assert!(graph.validate_dag_dependency().is_ok());
    }

    #[tokio::test]
    async fn test_graph_validate_dag_transaction_ok() {
        let graph = test_graph();
        let graph = graph
            .with_trigger(&function_node(&FUNCTION_NAMES[0]))
            .unwrap();
        assert!(
            graph
                .validate_dag_transaction(&TestTransactionBy::Name)
                .is_ok()
        );
        assert!(
            graph
                .validate_dag_transaction(&TestTransactionBy::Single)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_manual_trigger_function_matches() -> Result<(), TdError> {
        let graph = test_graph();
        let expected_trigger_fn = function_node(&FUNCTION_NAMES[0]);
        let graph = graph.with_trigger(&expected_trigger_fn)?;
        assert_eq!(graph.main_trigger().name, expected_trigger_fn.name);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_table_input_versions_and_table_function_output_versions() {
        let graph = test_graph();
        let graph = graph
            .with_trigger(&function_node(&FUNCTION_NAMES[0]))
            .unwrap();
        let input_versions = graph.function_table_input_versions();
        let output_versions = graph.table_function_output_versions();
        // Just check that these return non-empty sets for a non-trivial graph
        assert!(!input_versions.is_empty());
        assert!(!output_versions.is_empty());
    }

    #[tokio::test]
    async fn test_disconnected_nodes() {
        // Create a graph with a function and a table, but no edges
        let f = function_node(&FUNCTION_NAMES[0]);
        let t = table_node(&TABLE_NAMES[0]);
        let mut graph = TdGraph::<FunctionNode, TableNode, Versions> {
            inner: petgraph::Graph::new(),
        };
        // Add nodes manually
        let f_idx = graph.inner.add_node(GraphNode::Function(f.clone()));
        let _t_idx = graph.inner.add_node(GraphNode::Table(t.clone()));
        let exec_plan = ExecutionPlan {
            graph,
            trigger_index: f_idx,
            nodes_in_trigger_path: [f_idx].iter().cloned().collect(),
        };
        // Should only have the origin function in the trigger path
        let triggered_functions = exec_plan.triggered_functions();
        assert_eq!(triggered_functions.len(), 1);
        assert!(triggered_functions.iter().any(|fnc| fnc.name == f.name));
        let output_tables = exec_plan.created_tables();
        assert!(output_tables.is_empty());
    }
}
