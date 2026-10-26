//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::{Projection, TopologyGraph};
use petgraph::algo::toposort;
use petgraph::prelude::{Dfs, Direction, EdgeIndex, EdgeRef, NodeIndex};
use petgraph::visit::{EdgeFiltered, IntoNodeReferences, VisitMap, Visitable, Walker};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use ta_execution::TransactionMapper;
use td_error::{TdError, td_error};
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode, ResolvedVersion, TableNode};
use td_objects::types::basic::{FunctionName, TableName, TransactionByStr, TransactionKey};
use td_objects::types::table_ref::Versions;

#[td_error]
enum ExecutionPlanError {
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

#[derive(Debug)]
pub struct ExecutionPlan<F = FunctionNode, T = TableNode, V = Versions> {
    pub graph: TopologyGraph<F, T, V>,
    pub trigger_index: NodeIndex,
    pub nodes_in_trigger_path: HashSet<NodeIndex>,
}

pub type ResolvedExecutionPlan = ExecutionPlan<FunctionNode, TableNode, ResolvedVersion>;

impl<F, T, V> ExecutionPlan<F, T, V>
where
    F: Eq + Display,
{
    pub fn new(graph: TopologyGraph<F, T, V>, trigger: &F) -> Result<Self, TdError> {
        let trigger_index = graph
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
            .ok_or_else(|| ExecutionPlanError::InvalidTrigger(trigger.to_string()))?;

        let mut visited = graph.inner.visit_map();
        let mut nodes_in_trigger_path = HashSet::new();

        let mut stack = vec![trigger_index];
        while let Some(node) = stack.pop() {
            if visited.is_visited(&node) {
                continue;
            }

            visited.visit(node);
            nodes_in_trigger_path.insert(node);
            for edge in graph.inner.edges(node) {
                if let GraphEdge::Output { .. } | GraphEdge::Trigger { .. } = edge.weight() {
                    stack.push(edge.target());
                }
            }
        }

        Ok(ExecutionPlan {
            graph,
            trigger_index,
            nodes_in_trigger_path,
        })
    }
}

impl<F, T, V> Deref for ExecutionPlan<F, T, V> {
    type Target = TopologyGraph<F, T, V>;
    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl<F, T, V> DerefMut for ExecutionPlan<F, T, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}

impl<V> ExecutionPlan<FunctionNode, TableNode, V> {
    pub fn validate(&self, transaction_mapper: &impl TransactionMapper) -> Result<(), TdError> {
        self.validate_dag_trigger()?;
        self.validate_dag_dependency()?;
        self.validate_dag_transaction(transaction_mapper)?;
        Ok(())
    }
}

impl<T, V> ExecutionPlan<FunctionNode, T, V> {
    fn validate_dag_trigger(&self) -> Result<(), TdError> {
        let projection = Projection::triggers(self)?;
        toposort(&projection.inner, None).map_err(|err| {
            let function = &projection.inner[err.node_id()];
            ExecutionPlanError::CyclicTrigger(function.name.clone())
        })?;
        Ok(())
    }

    fn validate_dag_transaction(
        &self,
        transaction_mapper: &impl TransactionMapper,
    ) -> Result<(), TdError> {
        let projection = Projection::transactions(self, transaction_mapper)?;
        toposort(&projection.inner, None).map_err(|err| {
            let key = &projection.inner[err.node_id()];
            match transaction_mapper.transaction_by() {
                Ok(transaction_by) => {
                    ExecutionPlanError::CyclicTransaction(transaction_by, key.clone()).into()
                }
                Err(err) => err,
            }
        })?;
        Ok(())
    }
}

impl<F, V> ExecutionPlan<F, TableNode, V> {
    fn validate_dag_dependency(&self) -> Result<(), TdError> {
        // drop self dependencies as those cycles are allowed, and toposort errors if present
        let projection = Projection::dependencies(self, true)?;
        toposort(&projection.inner, None).map_err(|err| {
            let table = &projection.inner[err.node_id()];
            ExecutionPlanError::CyclicDependency(table.name.clone())
        })?;
        Ok(())
    }
}

impl<F, T, V> ExecutionPlan<F, T, V>
where
    F: Eq + Hash,
    T: Eq + Hash,
{
    pub fn trigger(&self) -> &F {
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

    pub fn inputs(&self) -> HashSet<(&F, &T, &GraphEdge<V>)>
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

    pub fn outputs(&self) -> HashSet<(&T, &F, &GraphEdge<V>)>
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

impl<F, T, V> ExecutionPlan<F, T, V> {
    /// Dependencies on tables this plan writes, but not before their reader
    /// runs: written by the reader itself, after it, or alongside it. Only
    /// triggers order runs, and only that order is validated acyclic, so a
    /// reader must not wait for such a write.
    pub fn dependencies_read_before_written(&self) -> HashSet<EdgeIndex> {
        let run_order = EdgeFiltered::from_fn(&self.inner, |edge| {
            !matches!(edge.weight(), GraphEdge::Dependency { .. })
        });
        let mut downstream_of: HashMap<NodeIndex, HashSet<NodeIndex>> = HashMap::new();
        let mut read_before_written = HashSet::new();
        for edge in self.inner.edge_references() {
            if !matches!(edge.weight(), GraphEdge::Dependency { .. }) {
                continue;
            }
            let (table, reader) = (edge.source(), edge.target());
            let Some(writer) = self.writer_in_plan(table) else {
                continue;
            };
            let downstream = downstream_of
                .entry(writer)
                .or_insert_with(|| Dfs::new(&run_order, writer).iter(&run_order).collect());
            let written_before = writer != reader && downstream.contains(&reader);
            if !written_before {
                read_before_written.insert(edge.id());
            }
        }
        read_before_written
    }

    fn writer_in_plan(&self, table: NodeIndex) -> Option<NodeIndex> {
        self.inner
            .edges_directed(table, Direction::Incoming)
            .find(|edge| matches!(edge.weight(), GraphEdge::Output { .. }))
            .map(|edge| edge.source())
            .filter(|writer| self.nodes_in_trigger_path.contains(writer))
    }

    pub async fn map<'a, CN, RN, CE, RE, FF, TT, VV>(
        &'a self,
        transform_nodes: CN,
        transform_edges: CE,
    ) -> Result<ExecutionPlan<FF, TT, VV>, TdError>
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
    use crate::test_utils::{TestTransactionBy, test_graph};
    use crate::topology::ProjectTopology;
    use std::collections::BTreeSet;
    use td_error::TdError;
    use td_objects::test_utils::graph::{
        FUNCTION_NAMES, TABLE_NAMES, collection, dependency, function, function_node, table,
        table_node, trigger, version,
    };

    #[tokio::test]
    async fn test_execution_plan_map_async() {
        let graph = test_graph();
        let graph = ExecutionPlan::new(graph, &function_node(&FUNCTION_NAMES[0])).unwrap();

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

        assert_eq!(mapped.trigger_index, graph.trigger_index);
        assert_eq!(mapped.nodes_in_trigger_path, graph.nodes_in_trigger_path);

        for (idx, node) in mapped.inner.node_references() {
            let orig_node = &graph.inner[idx];
            match (orig_node, node) {
                (GraphNode::Function(f1), GraphNode::Function(f2)) => assert_eq!(f1.name, f2.name),
                (GraphNode::Table(t1), GraphNode::Table(t2)) => assert_eq!(t1.name, t2.name),
                _ => panic!("Node type mismatch"),
            }
        }
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
        assert_eq!(
            mapped.inner.edge_count(),
            graph.inner.edge_count(),
            "Edge count mismatch"
        );
        assert_eq!(
            mapped.inner.node_count(),
            graph.inner.node_count(),
            "Node count mismatch"
        );
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
        let graph = ExecutionPlan::new(graph, &function_node(&FUNCTION_NAMES[0])).unwrap();
        assert!(graph.validate_dag_trigger().is_ok());
    }

    #[tokio::test]
    async fn test_graph_validate_dag_dependency_ok() {
        let graph = test_graph();
        let graph = ExecutionPlan::new(graph, &function_node(&FUNCTION_NAMES[0])).unwrap();
        assert!(graph.validate_dag_dependency().is_ok());
    }

    #[tokio::test]
    async fn test_graph_validate_dag_transaction_ok() {
        let graph = test_graph();
        let graph = ExecutionPlan::new(graph, &function_node(&FUNCTION_NAMES[0])).unwrap();
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
        let graph = ExecutionPlan::new(graph, &expected_trigger_fn)?;
        assert_eq!(graph.trigger().name, expected_trigger_fn.name);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_table_input_versions_and_table_function_output_versions() {
        let graph = test_graph();
        let graph = ExecutionPlan::new(graph, &function_node(&FUNCTION_NAMES[0])).unwrap();
        let input_versions = graph.inputs();
        let output_versions = graph.outputs();
        assert!(!input_versions.is_empty());
        assert!(!output_versions.is_empty());
    }

    /// function_0 writes and reads table_0, which triggers function_1 and,
    /// through function_1's table_1 or directly, function_2. function_1 reads
    /// table_0 and function_2's table_3.
    #[test]
    fn test_dependencies_read_before_written() -> Result<(), TdError> {
        let [f0, f1, f2] = [0, 1, 2].map(|i| &FUNCTION_NAMES[i]);
        let [t0, t1, t3] = [0, 1, 3].map(|i| &TABLE_NAMES[i]);
        let plan = |f2_trigger: &TableName, start: &FunctionName| {
            let graph = ProjectTopology::new(
                version(),
                vec![collection()],
                vec![],
                vec![function(f0), function(f1), function(f2)],
                vec![table(f0, t0), table(f1, t1), table(f2, t3)],
                vec![trigger(t0, f1), trigger(f2_trigger, f2)],
                vec![dependency(t0, f0), dependency(t0, f1), dependency(t3, f1)],
            )
            .graph()?;
            let plan = ExecutionPlan::new(graph, &function_node(start))?;
            let read_before_written: BTreeSet<_> = plan
                .dependencies_read_before_written()
                .into_iter()
                .map(|edge| {
                    let (table, function) = plan.inner.edge_endpoints(edge).unwrap();
                    match (&plan.inner[table], &plan.inner[function]) {
                        (GraphNode::Table(t), GraphNode::Function(f)) => {
                            format!("{}→{}", t.name, f.name)
                        }
                        _ => unreachable!(),
                    }
                })
                .collect();
            Ok::<_, TdError>(read_before_written)
        };
        let edges = |edges: &[&str]| edges.iter().map(|e| e.to_string()).collect();

        assert_eq!(
            plan(t1, f0)?,
            edges(&["table_0→function_0", "table_3→function_1"]),
            "its own table, and one written after it; not one written before"
        );
        assert_eq!(
            plan(t0, f0)?,
            edges(&["table_0→function_0", "table_3→function_1"]),
            "one written alongside it"
        );
        assert_eq!(
            plan(t1, f1)?,
            edges(&["table_3→function_1"]),
            "not a table the plan does not write"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_disconnected_nodes() {
        let f = function_node(&FUNCTION_NAMES[0]);
        let t = table_node(&TABLE_NAMES[0]);
        let mut graph = TopologyGraph::<FunctionNode, TableNode, Versions> {
            inner: petgraph::Graph::new(),
        };
        let f_idx = graph.inner.add_node(GraphNode::Function(f.clone()));
        let _t_idx = graph.inner.add_node(GraphNode::Table(t.clone()));
        let exec_plan = ExecutionPlan {
            graph,
            trigger_index: f_idx,
            nodes_in_trigger_path: [f_idx].iter().cloned().collect(),
        };
        let triggered_functions = exec_plan.triggered_functions();
        assert_eq!(triggered_functions.len(), 1);
        assert!(triggered_functions.iter().any(|fnc| fnc.name == f.name));
        let output_tables = exec_plan.created_tables();
        assert!(output_tables.is_empty());
    }
}
