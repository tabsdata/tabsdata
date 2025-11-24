//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use petgraph::prelude::{Direction, EdgeRef};
use petgraph::visit::IntoNodeReferences;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use ta_execution::graphs::ExecutionGraph;
use td_objects::execution::graph::{FunctionNode, GraphEdge, GraphNode, TableNode};

/// The `ExecutionPlanner` trait is used to define the execution plan for the type implementing it.
#[async_trait]
pub trait ExecutionPlanner<V: Eq + Hash> {
    /// Transforms the execution planner by applying a transformation function to each version.
    /// This is useful to convert relative versions to absolute versions.
    async fn versioned<'a, VV, T, E, Fut>(&'a self, transform: T) -> Result<ExecutionGraph<VV>, E>
    where
        T: Fn(&'a TableNode, &'a V, bool) -> Fut + Send + Clone + 'a,
        V: Sync + 'a,
        Fut: Future<Output = Result<VV, E>> + Send,
        VV: Eq + Hash + Send + Clone,
        E: Send;

    /// Returns all the nodes(functions) present in the plan (including the main function).
    fn functions(&self) -> HashSet<&FunctionNode>;

    /// Returns all the edges(tables) present in the plan.
    fn tables(&self) -> HashSet<&TableNode>;

    /// Returns the output tables that are output of triggered functions. Output tables are tables
    /// with a `GraphEdge::Output` edge. Not all tables are output tables, as some of them are
    /// not getting created (if the function is not triggered).
    fn output_tables(&self) -> HashSet<(&FunctionNode, &TableNode, &GraphEdge<V>)>;

    /// Returns the manual trigger function. This is the main function that triggers the execution.
    fn manual_trigger_function(&self) -> &FunctionNode;

    /// Returns all the functions that are triggered by the planner (it does NOT include the main function).
    fn triggered_functions(&self) -> HashSet<&FunctionNode>;

    /// Returns the functions that are triggered with the versions required before being able to
    /// execute them (it includes the main function, if it had any dependency). Requirements
    /// are the edges with `GraphEdge::Dependency` or `GraphEdge::Trigger`.
    fn function_version_requirements(&self) -> HashSet<(&FunctionNode, &TableNode, &GraphEdge<V>)>;
}

#[async_trait]
impl<V: Eq + Hash> ExecutionPlanner<V> for ExecutionGraph<V> {
    async fn versioned<'a, VV, T, E, Fut>(&'a self, transform: T) -> Result<ExecutionGraph<VV>, E>
    where
        T: Fn(&'a TableNode, &'a V, bool) -> Fut + Send + Clone + 'a,
        V: Sync + 'a,
        Fut: Future<Output = Result<VV, E>> + Send,
        VV: Eq + Hash + Send,
        E: Send,
    {
        // TODO this does a query per edge, we can optimize if table + version is the same (add a transform cache, dont touch this probably)
        // TODO we could probably also skip non triggered nodes, setting just None
        let futures: Vec<_> = self
            .inner
            .edge_references()
            .map(|edge| {
                let (node, self_dependency, versions) = match edge.weight() {
                    GraphEdge::Output { versions, .. } => {
                        (&self.inner[edge.target()], false, versions)
                    }
                    GraphEdge::Trigger { versions } => {
                        (&self.inner[edge.source()], false, versions)
                    }
                    GraphEdge::Dependency {
                        versions,
                        dependency,
                    } => (
                        &self.inner[edge.source()],
                        *dependency.self_dependency,
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
        let new_graph = self.inner.map(
            |_, node_weight| node_weight.clone(),
            |edge_idx, edge_weight| {
                let new_version = updated_versions.remove(&edge_idx).unwrap();
                edge_weight.versioned(new_version)
            },
        );

        // Create the new ExecutionGraph with VV
        Ok(ExecutionGraph::new(new_graph, self.trigger_index))
    }

    fn functions(&self) -> HashSet<&FunctionNode> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Function(function) => Some(function),
                _ => None,
            })
            .collect()
    }

    fn tables(&self) -> HashSet<&TableNode> {
        self.inner
            .node_references()
            .filter_map(|(_, node)| match node {
                GraphNode::Table(table) => Some(table),
                _ => None,
            })
            .collect()
    }

    fn output_tables(&self) -> HashSet<(&FunctionNode, &TableNode, &GraphEdge<V>)> {
        self.inner
            .edge_references()
            .filter_map(|edge_ref| match edge_ref.weight() {
                GraphEdge::Trigger { .. } => Some(edge_ref.target()),
                _ => None,
            })
            .chain(std::iter::once(self.trigger_index))
            .flat_map(|index| {
                self.inner
                    .edges_directed(index, Direction::Outgoing)
                    .filter_map(|edge| {
                        let source = &self.inner[edge.source()];
                        let target = &self.inner[edge.target()];
                        match (source, target) {
                            (GraphNode::Function(function), GraphNode::Table(table)) => {
                                Some((function, table, edge.weight()))
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn manual_trigger_function(&self) -> &FunctionNode {
        match &self.inner[self.trigger_index] {
            GraphNode::Function(function) => function,
            _ => unreachable!(),
        }
    }

    fn triggered_functions(&self) -> HashSet<&FunctionNode> {
        self.inner
            .edge_references()
            .filter_map(|edge_ref| match edge_ref.weight() {
                GraphEdge::Trigger { .. } => Some(edge_ref.target()),
                _ => None,
            })
            .filter(|index| *index != self.trigger_index)
            .filter_map(|index| {
                let node = &self.inner[index];
                match node {
                    GraphNode::Function(function) => Some(function),
                    _ => None,
                }
            })
            .collect()
    }

    fn function_version_requirements(&self) -> HashSet<(&FunctionNode, &TableNode, &GraphEdge<V>)> {
        self.inner
            .edge_references()
            .filter_map(|edge_ref| match edge_ref.weight() {
                GraphEdge::Trigger { .. } => Some(edge_ref.target()),
                _ => None,
            })
            .chain(std::iter::once(self.trigger_index))
            .flat_map(|index| {
                self.inner
                    .edges_directed(index, Direction::Incoming)
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
    use td_objects::test_utils::graph::{FUNCTION_NAMES, TABLE_NAMES, function_node, table_node};

    fn expected_function_nodes() -> HashSet<FunctionNode> {
        FUNCTION_NAMES.iter().map(function_node).collect()
    }

    fn expected_table_nodes() -> HashSet<TableNode> {
        TABLE_NAMES.iter().map(table_node).collect()
    }

    #[tokio::test]
    async fn test_functions() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;
        let actual: HashSet<_> = graph.functions().into_iter().cloned().collect();
        let expected = expected_function_nodes();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_tables() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;
        let actual: HashSet<_> = graph.tables().into_iter().cloned().collect();
        let expected = expected_table_nodes();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_output_tables_are_from_triggered_or_main_functions() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;

        for (function, table, edge) in graph.output_tables() {
            // Each output table must come from a function node.
            assert!(
                FUNCTION_NAMES.contains(&function.name),
                "Unexpected function in output tables: {:?}",
                function
            );

            // Each output table must belong to known table nodes.
            assert!(
                TABLE_NAMES.contains(&table.name),
                "Unexpected table in output tables: {:?}",
                table
            );

            // Each edge must represent an output edge variant.
            assert!(
                matches!(edge, GraphEdge::Output { .. }),
                "Expected only output edges in output_tables(), got: {:?}",
                edge
            );
        }

        // Sanity: all known functions should produce their defined outputs
        let output_fn_names: HashSet<_> = graph
            .output_tables()
            .iter()
            .map(|(f, _, _)| &f.name)
            .collect();
        assert!(
            output_fn_names.contains(&FUNCTION_NAMES[0])
                || output_fn_names.contains(&FUNCTION_NAMES[1]),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_manual_trigger_function_matches() -> Result<(), TdError> {
        let (graph, expected_trigger_fn) = test_graph().await;
        assert_eq!(graph.manual_trigger_function(), &expected_trigger_fn);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_version_requirements_include_dependency_edges() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;
        let requirements = graph.function_version_requirements();

        for (function, table, edge) in &requirements {
            assert!(
                FUNCTION_NAMES.contains(&function.name),
                "Unexpected function in version requirements: {:?}",
                function
            );
            assert!(
                TABLE_NAMES.contains(&table.name),
                "Unexpected table in version requirements: {:?}",
                table
            );
            assert!(
                matches!(
                    edge,
                    GraphEdge::Trigger { .. } | GraphEdge::Dependency { .. }
                ),
                "Invalid edge type in version requirements: {:?}",
                edge
            );
        }

        // Sanity check: at least one dependency per triggered function
        let triggered_fn_names: HashSet<_> = graph
            .triggered_functions()
            .into_iter()
            .map(|f| f.name.clone())
            .collect();

        let functions_with_requirements: HashSet<_> = requirements
            .iter()
            .map(|(f, _, _)| f.name.clone())
            .collect();

        assert!(
            triggered_fn_names.is_subset(&functions_with_requirements),
            "Not all triggered functions have version requirements"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_versioned_graph_versioned_structure() -> Result<(), TdError> {
        let (graph, _) = test_graph().await;
        let new_graph = graph
            .versioned(|_, _, _| async { Ok::<_, TdError>(1) })
            .await?;

        assert_eq!(new_graph.functions().len(), FUNCTION_NAMES.len());
        assert_eq!(new_graph.tables().len(), TABLE_NAMES.len());
        Ok(())
    }
}
