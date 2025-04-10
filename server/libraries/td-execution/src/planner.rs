//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use petgraph::prelude::EdgeRef;
use petgraph::visit::IntoNodeReferences;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use ta_execution::graphs::ExecutionGraph;
use td_objects::types::execution::{FunctionVersionNode, GraphEdge, GraphNode, TableVersionNode};
use te_execution::planner::TriggerPlanner;

/// The `ExecutionPlanner` trait is used to define the execution plan for the type implementing it.
/// It does require TriggerPlanner as a super-trait, which will define which nodes are triggered,
/// excluding the manual trigger node (the main trigger).
#[async_trait]
pub trait ExecutionPlanner<V: Eq + Hash>: TriggerPlanner<V> {
    /// Versioned Planner type implementing this trait (used to transform using versioned).
    type Planner<T: Eq + Hash>: ExecutionPlanner<T>;

    /// Transforms the execution planner by applying a transformation function to each version.
    /// This is useful to convert relative versions to absolute versions.
    async fn versioned<'a, VV, T, E, Fut>(&'a self, transform: T) -> Result<Self::Planner<VV>, E>
    where
        T: Fn(&'a TableVersionNode, &'a V, bool) -> Fut + Send + Clone + 'a,
        V: Sync + 'a,
        Fut: Future<Output = Result<VV, E>> + Send,
        VV: Eq + Hash + Send + Clone,
        E: Send;

    /// Returns all the nodes(functions) present in the plan (including the main function).
    fn functions(&self) -> HashSet<&FunctionVersionNode>;

    /// Returns all the edges(tables) present in the plan.
    fn tables(&self) -> HashSet<&TableVersionNode>;

    /// Returns the output tables that are output of triggered functions. Output tables are tables
    /// with a `GraphEdge::Output` edge. Not all tables are output tables, as some of them are
    /// not getting created (if the function is not triggered).
    fn output_tables(&self) -> HashSet<(&FunctionVersionNode, &TableVersionNode)>;

    /// Returns the manual trigger function. This is the main function that triggers the execution.
    fn manual_trigger_function(&self) -> &FunctionVersionNode;

    /// Returns all the functions that are triggered by the planner (it does NOT include the main function).
    fn triggered_functions(&self) -> HashSet<&FunctionVersionNode>;

    /// Returns the functions that are triggered with the versions required before being able to
    /// execute them (it includes the main function, if it had any dependency). Requirements
    /// are the edges with `GraphEdge::Dependency` or `GraphEdge::Trigger`.
    fn function_version_requirements(&self) -> HashSet<(&FunctionVersionNode, &V)>;
}

#[async_trait]
impl<V: Eq + Hash> ExecutionPlanner<V> for ExecutionGraph<V> {
    type Planner<T: Eq + Hash> = ExecutionGraph<T>;

    async fn versioned<'a, VV, T, E, Fut>(&'a self, transform: T) -> Result<Self::Planner<VV>, E>
    where
        T: Fn(&'a TableVersionNode, &'a V, bool) -> Fut + Send + Clone + 'a,
        V: Sync + 'a,
        Fut: Future<Output = Result<VV, E>> + Send,
        VV: Eq + Hash + Send,
        E: Send,
    {
        // TODO this does a query per edge, we can optimize if table + version is the same (add a transform cache, dont touch this probably)
        // TODO we could probably also skip non triggered nodes, setting just None
        let futures: Vec<_> = self
            .inner()
            .edge_references()
            .map(|edge| {
                let (node, self_dependency, versions) = match edge.weight() {
                    GraphEdge::Output { versions } => {
                        (&self.inner()[edge.target()], false, versions)
                    }
                    GraphEdge::Trigger { versions } => {
                        (&self.inner()[edge.source()], false, versions)
                    }
                    GraphEdge::Dependency {
                        versions,
                        dependency,
                    } => (
                        &self.inner()[edge.source()],
                        **dependency.self_dependency(),
                        versions,
                    ),
                };
                let table = match node {
                    GraphNode::Table(table) => table,
                    _ => unreachable!(),
                };
                (edge, table, versions, self_dependency)
            })
            .map(|(edge, table, versions, self_dependency)| {
                let transform = transform.clone();
                async move {
                    let new_version = transform(table, versions, self_dependency).await?;
                    Ok::<_, E>((edge.id(), new_version))
                }
            })
            .collect();

        // Collect new versions asynchronously
        let mut updated_versions: HashMap<_, _> = futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .collect();

        // Build a new graph with transformed weights
        let new_graph = self.inner().map(
            |_, node_weight| node_weight.clone(),
            |edge_idx, edge_weight| {
                let new_version = updated_versions.remove(&edge_idx).unwrap();
                edge_weight.versioned(new_version)
            },
        );

        // Create the new ExecutionGraph with VV
        Ok(ExecutionGraph::new(new_graph, *self.trigger_index()))
    }

    fn functions(&self) -> HashSet<&FunctionVersionNode> {
        self.inner()
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Function(function) => Some(function),
                _ => None,
            })
            .collect()
    }

    fn tables(&self) -> HashSet<&TableVersionNode> {
        self.inner()
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Table(table) => Some(table),
                _ => None,
            })
            .collect()
    }

    fn output_tables(&self) -> HashSet<(&FunctionVersionNode, &TableVersionNode)> {
        self.triggered_functions_index()
            .iter()
            .chain(std::iter::once(self.trigger_index()))
            .flat_map(|index| {
                self.inner()
                    .edges_directed(*index, petgraph::Direction::Outgoing)
                    .filter_map(|edge| {
                        let source = &self.inner()[edge.source()];
                        let target = &self.inner()[edge.target()];
                        match (source, target) {
                            (GraphNode::Function(function), GraphNode::Table(table)) => {
                                Some((function, table))
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn manual_trigger_function(&self) -> &FunctionVersionNode {
        match &self.inner()[*self.trigger_index()] {
            GraphNode::Function(function) => function,
            _ => unreachable!(),
        }
    }

    fn triggered_functions(&self) -> HashSet<&FunctionVersionNode> {
        self.triggered_functions_index()
            .iter()
            .filter(|index| *index != self.trigger_index())
            .filter_map(|index| {
                let node = &self.inner()[*index];
                match node {
                    GraphNode::Function(function) => Some(function),
                    _ => None,
                }
            })
            .collect()
    }

    fn function_version_requirements(&self) -> HashSet<(&FunctionVersionNode, &V)> {
        self.triggered_functions_index()
            .iter()
            .flat_map(|index| {
                self.inner()
                    .edges_directed(*index, petgraph::Direction::Incoming)
                    .filter_map(|edge| {
                        let source = &self.inner()[edge.source()];
                        let target = &self.inner()[edge.target()];
                        match (source, target) {
                            (GraphNode::Table(_), GraphNode::Function(function)) => {
                                Some((function, edge.weight().versions()))
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ta_execution::test_utils::graph::test_graph;
    use td_error::TdError;
    use td_objects::types::test_utils::execution::{function_node, table_node};

    #[tokio::test]
    async fn test_functions() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let functions = graph.functions();
        assert_eq!(functions.len(), 2);

        let mut expected =
            HashSet::from([function_node("function_1"), function_node("function_2")]);

        for function in functions {
            assert!(expected.remove(function));
        }
        assert_eq!(expected.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_tables() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let tables = graph.tables();
        assert_eq!(tables.len(), 2);

        let mut expected = HashSet::from([table_node("table_1"), table_node("table_2")]);

        for table in tables {
            assert!(expected.remove(table));
        }
        assert_eq!(expected.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_output_tables() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let output_tables = graph.output_tables();
        assert_eq!(output_tables.len(), 1);

        let mut expected = HashSet::from([(function_node("function_1"), table_node("table_1"))]);

        for (function, table) in output_tables {
            assert!(expected.remove(&(function.clone(), table.clone())));
        }
        assert_eq!(expected.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_manual_trigger_function() -> Result<(), TdError> {
        let (graph, trigger_function) = test_graph().await;

        let manual_trigger_function = graph.manual_trigger_function();
        assert_eq!(manual_trigger_function, &trigger_function);
        Ok(())
    }

    #[tokio::test]
    async fn test_triggered_functions() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let triggered_functions = graph.triggered_functions();
        assert_eq!(triggered_functions.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_version_requirements() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let function_version_requirements = graph.function_version_requirements();
        assert_eq!(function_version_requirements.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_versioned() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        let new_graph = graph
            .versioned(|_, _, _| async { Ok::<_, TdError>(1) })
            .await?;
        assert_eq!(new_graph.functions().len(), 2);
        assert_eq!(new_graph.tables().len(), 2);
        Ok(())
    }
}
