//
// Copyright 2024 Tabs Data Inc.
//

use crate::dataset::{
    Dataset, DatasetWithUris, ExecutableDataset, RelativeVersions, ResolvedVersion, TdVersions,
};
use crate::execution_planner::ExecutionPlanner;
use crate::link::{DataGraph, TriggerGraph};
use getset::Getters;
use petgraph::algo::toposort;
use petgraph::dot::{Config, Dot};
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use petgraph::Graph;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use td_common::dataset::{DatasetRef, TableRef, VersionRef};
use td_common::uri::TdUri;
use td_error::td_error;
use td_transaction::{TransactionBy, TransactionKey};

#[td_error]
pub enum GraphError {
    #[error("Graph is not a Direct Acyclic Graph. Cycle found in: {0:?}")]
    Cyclic(String) = 0,
    #[error("Graph is not a transactional Direct Acyclic Graph with transaction mode: {0:?}. Cycle found in: {1:?}")]
    CyclicTransaction(TransactionBy, TransactionKey) = 1,
}

/// Builder for creating a `DatasetGraph`.
pub struct DatasetGraphBuilder<'a> {
    data_graph: &'a DataGraph,
    trigger_graph: &'a TriggerGraph,
}

/// Adds a node to the graph if it does not exist.
fn add_if_absent<D, V>(
    graph: &mut Graph<D, V>,
    node_map: &mut HashMap<D, NodeIndex>,
    dataset: D,
) -> NodeIndex
where
    D: Eq + Hash + Clone,
{
    *node_map
        .entry(dataset.clone())
        .or_insert_with(|| graph.add_node(dataset))
}

impl<'a> DatasetGraphBuilder<'a> {
    /// Creates a new `DatasetGraphBuilder` from a data and trigger graph.
    pub fn new(data_graph: &'a DataGraph, trigger_graph: &'a TriggerGraph) -> Self {
        Self {
            data_graph,
            trigger_graph,
        }
    }

    /// Builds a `DatasetGraph` from the data and trigger graphs, starting from a trigger dataset.
    pub fn build(
        self,
        trigger_dataset: Dataset,
    ) -> Result<DatasetGraph<Dataset, TdVersions>, GraphError> {
        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        let trigger_index = add_if_absent(&mut graph, &mut node_map, trigger_dataset);

        for link in self.data_graph.links() {
            let (source, target) = Dataset::from_link(link);
            let source_index = add_if_absent(&mut graph, &mut node_map, source);
            let target_index = add_if_absent(&mut graph, &mut node_map, target);

            let versions = TdUri::parse_versions("", link.source_versions()).unwrap();
            graph.add_edge(
                source_index,
                target_index,
                TdVersions::from_table(
                    versions,
                    link.source_table().to_string(),
                    *link.source_pos(),
                ),
            );
        }

        for link in self.trigger_graph.links() {
            let (source, target) = Dataset::from_link(link);
            let source_index = add_if_absent(&mut graph, &mut node_map, source);
            let target_index = add_if_absent(&mut graph, &mut node_map, target);
            graph.add_edge(source_index, target_index, TdVersions::trigger());
        }

        let dataset_graph = DatasetGraph {
            graph,
            trigger_index,
        };

        Ok(dataset_graph)
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum NodeOrEdge<N, E> {
    Node(N),
    Edge(E),
}

impl<N, E> Debug for NodeOrEdge<N, E>
where
    N: Debug,
    E: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeOrEdge::Node(n) => write!(f, "{:?}", n),
            NodeOrEdge::Edge(v) => write!(f, "{:?}", v),
        }
    }
}

impl<N, E> Display for NodeOrEdge<N, E>
where
    N: Display,
    E: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeOrEdge::Node(n) => write!(f, "{}", n),
            NodeOrEdge::Edge(v) => write!(f, "{}", v),
        }
    }
}

/// Represents a graph of datasets.
#[derive(Getters)]
#[getset(get = "pub")]
pub struct DatasetGraph<D = Dataset, V = TdVersions>
where
    D: DatasetRef,
    V: VersionRef,
{
    graph: Graph<D, V>,
    trigger_index: NodeIndex,
}

impl<D, V> DatasetGraph<D, V>
where
    D: DatasetRef,
    V: VersionRef,
{
    pub fn dataset_for_index(&self, index: NodeIndex) -> &D {
        &self.graph[index]
    }

    /// Generates a DOT representation of the graph.
    pub fn dot(&self) -> Dot<&Graph<D, V>> {
        Dot::new(&self.graph)
    }

    /// Returns a new graph with edges as intermediate nodes.
    pub fn convert_edges_to_nodes(&self) -> Graph<NodeOrEdge<D, V>, usize> {
        let mut new_graph = DiGraph::new();
        let mut node_map = HashMap::new();

        // Copy nodes to the new graph
        for node in self.graph.node_indices() {
            let new_node = new_graph.add_node(NodeOrEdge::Node(self.graph[node].clone()));
            node_map.insert(node, new_node);
        }

        // Convert edges to intermediate nodes
        let mut edge_map = HashMap::new();
        for edge in self.graph.edge_references() {
            // Create an intermediate node
            let weight = NodeOrEdge::Edge(edge.weight().clone());
            let intermediate_node = *edge_map.entry(weight.clone()).or_insert_with(|| {
                let intermediate_node = new_graph.add_node(weight);
                new_graph.add_edge(node_map[&edge.source()], intermediate_node, 0);
                intermediate_node
            });

            // Add new edges
            new_graph.add_edge(intermediate_node, node_map[&edge.target()], 0);
        }

        new_graph
    }
}

impl<D, V> DatasetGraph<ExecutableDataset<D>, V>
where
    D: DatasetRef,
    V: VersionRef,
{
    /// Creates a `DatasetGraph` from an `ExecutionPlanner` versioned on any reference.
    pub fn from_execution_planner(
        execution_planner: &ExecutionPlanner<D, V>,
    ) -> Result<Self, GraphError> {
        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        for dataset in execution_planner.datasets() {
            let execute = execution_planner.is_trigger(dataset);
            let node_index = graph.add_node(ExecutableDataset::new(dataset.clone(), execute));
            node_map.insert(dataset, node_index);
        }

        let (requirements, _) = execution_planner.requirements();
        for requirement in requirements {
            let target_index = *node_map.get(requirement.target()).unwrap();
            let source_index = *node_map.get(requirement.source()).unwrap();
            let source_version = requirement.source_version();
            graph.add_edge(source_index, target_index, source_version.clone());
        }

        let (trigger, _) = execution_planner.manual_trigger();
        let trigger_index = *node_map.get(trigger).unwrap();

        let dataset_graph = DatasetGraph {
            graph,
            trigger_index,
        };

        Ok(dataset_graph)
    }
}

impl<D> DatasetGraph<ExecutableDataset<D>, RelativeVersions>
where
    D: DatasetRef,
{
    /// Generates a DOT representation of the execution template.
    pub fn template_dot(&self) -> Dot<&Graph<ExecutableDataset<D>, RelativeVersions>> {
        Dot::with_attr_getters(
            &self.graph,
            &[Config::EdgeNoLabel],
            &|_, edge| {
                let weight = edge.weight().versions();
                match weight {
                    TdVersions::Dataset { .. } => "label=\"\", color=red ".to_string(),
                    TdVersions::Table {
                        table, versions, ..
                    } => {
                        format!("label=\"{}/{}\"", table, versions)
                    }
                }
            },
            // Using Tabsdata brand orange for nodes background.
            &|_, (_, dataset)| match dataset.execute() {
                true => "fillcolor=\"#FBAF4F\",style=filled ".to_string(),
                false => "color=black ".to_string(),
            },
        )
    }
}

impl<D, V, T> DatasetGraph<D, TdVersions<V, T>>
where
    D: DatasetRef,
    V: VersionRef,
    T: TableRef,
{
    /// Validates the graph to ensure it is a Direct Acyclic Graph (DAG) trigger wise.
    pub fn validate_dag(&self) -> Result<(), GraphError> {
        let partial = PartialGraph::trigger_graph(self);
        toposort(&partial.graph, None).map_err(|err| {
            let dataset = &self.graph[err.node_id()];
            GraphError::Cyclic(dataset.dataset().to_string())
        })?;
        Ok(())
    }

    /// Validates the graph to ensure it is a Direct Acyclic Graph (DAG) transaction wise.
    pub fn validate_transaction(&self, transaction_by: &TransactionBy) -> Result<(), GraphError> {
        let partial = PartialGraph::transaction_graph(self, transaction_by);
        toposort(&partial.graph, None).map_err(|err| {
            let key = &partial.graph[err.node_id()];
            GraphError::CyclicTransaction(transaction_by.clone(), key.to_string())
        })?;
        Ok(())
    }
}

/// Represents a partial graph used for validation.
pub struct PartialGraph<D> {
    graph: Graph<D, ()>,
}

impl<D> PartialGraph<D>
where
    D: DatasetRef,
{
    /// Creates a partial graph containing only data edges.
    #[allow(dead_code)]
    pub fn data_graph<V, T>(dgraph: &DatasetGraph<D, TdVersions<V, T>>) -> PartialGraph<&D>
    where
        V: VersionRef,
        T: TableRef,
    {
        let mut new_graph = DiGraph::new();

        for node in dgraph.graph().node_indices() {
            new_graph.add_node(&dgraph.graph()[node]);
        }

        for edge in dgraph.graph().edge_references() {
            if let TdVersions::Table { .. } = edge.weight() {
                let source = new_graph
                    .node_indices()
                    .find(|&i| dgraph.graph()[i] == dgraph.graph()[edge.source()])
                    .unwrap();
                let target = new_graph
                    .node_indices()
                    .find(|&i| dgraph.graph()[i] == dgraph.graph()[edge.target()])
                    .unwrap();
                new_graph.add_edge(source, target, ());
            }
        }

        PartialGraph { graph: new_graph }
    }

    /// Creates a partial graph containing only trigger edges.
    pub fn trigger_graph<V, T>(dgraph: &DatasetGraph<D, TdVersions<V, T>>) -> PartialGraph<&D>
    where
        V: VersionRef,
        T: TableRef,
    {
        let mut new_graph = DiGraph::new();

        for node in dgraph.graph().node_indices() {
            new_graph.add_node(&dgraph.graph()[node]);
        }

        for edge in dgraph.graph().edge_references() {
            if let TdVersions::Dataset { .. } = edge.weight() {
                let source = new_graph
                    .node_indices()
                    .find(|&i| dgraph.graph()[i] == dgraph.graph()[edge.source()])
                    .unwrap();
                let target = new_graph
                    .node_indices()
                    .find(|&i| dgraph.graph()[i] == dgraph.graph()[edge.target()])
                    .unwrap();
                new_graph.add_edge(source, target, ());
            }
        }

        PartialGraph { graph: new_graph }
    }

    /// Creates a trigger transaction graph.
    pub fn transaction_graph<V, T>(
        dgraph: &DatasetGraph<D, TdVersions<V, T>>,
        transaction_by: &TransactionBy,
    ) -> PartialGraph<TransactionKey>
    where
        D: DatasetRef,
        V: VersionRef,
        T: TableRef,
    {
        let mut new_graph = DiGraph::new();

        let mut node_map = HashMap::new();
        for node in dgraph.graph().node_indices() {
            let key = transaction_by.key(&dgraph.graph()[node]);
            node_map
                .entry(key.clone())
                .or_insert_with(|| new_graph.add_node(key));
        }

        let mut edge_map = HashMap::new();
        for edge in dgraph.graph().edge_references() {
            if let TdVersions::Dataset { .. } = edge.weight() {
                let source_key = transaction_by.key(&dgraph.graph()[edge.source()]);
                let target_key = transaction_by.key(&dgraph.graph()[edge.target()]);
                let source = node_map[&source_key];
                let target = node_map[&target_key];
                if source != target && !edge_map.contains_key(&(source, target)) {
                    new_graph.add_edge(source, target, ());
                    edge_map.insert((source, target), ());
                }
            }
        }

        PartialGraph { graph: new_graph }
    }

    /// Generates a DOT representation of the partial graph.
    #[allow(dead_code)]
    pub fn dot(&self) -> Dot<&Graph<D, ()>> {
        Dot::with_attr_getters(
            &self.graph,
            &[Config::EdgeNoLabel],
            &|_, _| String::new(),
            &|_, _| String::new(),
        )
    }
}

pub type ExecutionGraphWithNames =
    DatasetGraph<ExecutableDataset<DatasetWithUris>, ResolvedVersion>;

impl ExecutionGraphWithNames {
    /// Creates a `DatasetGraph` from an `ExecutionPlanWithNames`, to visualize it.
    pub fn executable_with_names(
        execution_planner: &ExecutionPlanner<DatasetWithUris, ResolvedVersion>,
        transaction_by: &TransactionBy,
    ) -> Result<Self, GraphError> {
        let mut graph = DiGraph::new();
        let mut node_map = HashMap::new();

        for dataset in execution_planner.datasets() {
            let execute = execution_planner.is_trigger(dataset);
            let node_index = graph.add_node(ExecutableDataset::with_transaction(
                dataset.clone(),
                execute,
                transaction_by.clone(),
            ));
            node_map.insert(dataset, node_index);
        }

        // Hiding trigger_requirements.
        let (requirements, _) = execution_planner.data_requirements();
        for requirement in requirements {
            let version_pos = requirement
                .source_version()
                .relative_versions()
                .versions()
                .position();
            match version_pos {
                Some(pos) if pos >= 0 => {
                    let target_index = *node_map.get(requirement.target()).unwrap();
                    let source_index = *node_map.get(requirement.source()).unwrap();
                    let source_version = requirement.source_version();
                    graph.add_edge(source_index, target_index, source_version.clone());
                }
                _ => {
                    // We don't want to show system deps.
                    continue;
                }
            }
        }

        let (trigger, _) = execution_planner.manual_trigger();
        let trigger_index = *node_map.get(trigger).unwrap();

        let dataset_graph = DatasetGraph {
            graph,
            trigger_index,
        };

        Ok(dataset_graph)
    }
}

pub trait ExecutionDot<D, V>
where
    D: DatasetRef,
    V: VersionRef,
{
    fn dot(&self) -> Dot<&Graph<ExecutableDataset<D>, V>>;
}

impl<D, V> ExecutionDot<D, V> for Graph<ExecutableDataset<D>, V>
where
    D: DatasetRef,
    V: VersionRef + Display,
{
    fn dot(&self) -> Dot<&Graph<ExecutableDataset<D>, V>> {
        Dot::with_attr_getters(
            self,
            &[],
            // Adding 3 spaces before and after edges label for a nicer graph visualization.
            &|_, edge| format!("label=\"   {}   \"", edge.weight()),
            &|_, (_, dataset)| {
                let group = dataset.transaction_by().key(dataset);
                match dataset.execute() {
                    // Using Tabsdata brand orange for nodes background.
                    true => {
                        format!("fillcolor=\"#FBAF4F\",style=filled,group=\"{}\" ", group)
                    }
                    false => format!("group=\"{}\" ", group),
                }
            },
        )
    }
}

/// Changes group properties in dot node attributes by subgraphs, composed of those grouped nodes.
/// The "group" attribute should be the last one in the node attributes list.
pub fn groups_into_subgraph<D: Display, V: Display>(dot_input: Dot<&Graph<D, V>>) -> String {
    let dot_input = &dot_input.to_string();

    // Regex patterns to parse nodes and edges
    let node_re = Regex::new(r#"(\d+)\s+\[(.*?)group\s*=\s*"([^"]+)".*?]"#).unwrap();
    let edge_re = Regex::new(r#"(\d+)\s*->\s*(\d+)\s+\[(.*?)]"#).unwrap();

    // Store nodes by groups
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    let mut edges = Vec::new();

    // Process nodes and group them
    for cap in node_re.captures_iter(dot_input) {
        let node_id = &cap[1];
        let attributes = &cap[2];
        let group = &cap[3];
        let node_definition = format!(
            r#"{} [{} group="\#{}"];"#,
            node_id,
            attributes.trim(),
            group
        );
        groups
            .entry(group.to_string())
            .or_default()
            .push(node_definition);
    }

    // Process edges
    for cap in edge_re.captures_iter(dot_input) {
        let source = &cap[1];
        let target = &cap[2];
        let attributes = &cap[3];
        edges.push(format!(
            r#"{} -> {} [{}];"#,
            source,
            target,
            attributes.trim()
        ));
    }

    // Generate the output DOT string
    let mut result = String::from("digraph {\n");

    // Add grouped nodes as subgraphs
    for (group, nodes) in groups {
        let group_style = ("#FFEECC", "#FBAF4F");

        result.push_str(&format!(
            r#"subgraph cluster_{} {{
               label = "{}";
               style = filled;
               fillcolor = "{}";
               color = "{}";"#,
            group, group, group_style.0, group_style.1
        ));

        for node in nodes {
            result.push_str(&format!("{}\n", node));
        }
        result.push_str("    }\n");
    }

    // Add edges
    if !edges.is_empty() {
        for edge in edges {
            result.push_str(&format!("{}\n", edge));
        }
    }

    result.push('}');
    result
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::link::Graph;
    use crate::test_utils::{data_link_from_dataset, trigger_link_from_dataset, NodeFinder};
    use td_common::uri::{Version, Versions};

    #[test]
    fn test_add_if_absent_new_node() {
        let mut graph: DiGraph<Dataset, ()> = DiGraph::new();
        let mut node_map = HashMap::new();
        let dataset = Dataset::new("dst", "d1");

        let node_index = add_if_absent(&mut graph, &mut node_map, dataset.clone());

        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph[node_index], dataset);
        assert_eq!(node_map.len(), 1);
        assert_eq!(node_map[&dataset], node_index);
    }

    #[test]
    fn test_add_if_absent_existing_node() {
        let mut graph: DiGraph<Dataset, ()> = DiGraph::new();
        let mut node_map = HashMap::new();
        let dataset = Dataset::new("dst", "d1");

        let node_index_1 = add_if_absent(&mut graph, &mut node_map, dataset.clone());
        let node_index_2 = add_if_absent(&mut graph, &mut node_map, dataset.clone());

        assert_eq!(graph.node_count(), 1);
        assert_eq!(node_index_1, node_index_2);
        assert_eq!(node_map.len(), 1);
        assert_eq!(node_map[&dataset], node_index_1);
    }

    #[test]
    fn test_dataset_graph_validate() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph = TriggerGraph(Graph(vec![]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1)
            .unwrap();

        assert!(dataset_graph.validate_dag().is_ok());
    }

    #[test]
    fn test_dataset_graph_dataset_for_index() {
        let dataset1 = Dataset::new("dst", "d1");

        let data_graph = DataGraph(Graph(vec![]));
        let trigger_graph = TriggerGraph(Graph(vec![]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 1);
        assert_eq!(dataset_graph.graph().edge_count(), 0);
        let dataset = dataset_graph.dataset_for_index(*dataset_graph.trigger_index());
        assert_eq!(dataset, &dataset1);
    }

    #[test]
    fn test_dataset_graph_dot() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph =
            TriggerGraph(Graph(vec![trigger_link_from_dataset(&dataset1, &dataset2)]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1)
            .unwrap();

        let dot = dataset_graph.dot();
        let dot_str = format!("{:?}", dot);
        assert!(dot_str.contains("d1"));
        assert!(dot_str.contains("d2"));
    }

    #[test]
    fn test_dataset_graph_builder() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph =
            TriggerGraph(Graph(vec![trigger_link_from_dataset(&dataset1, &dataset2)]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 2);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 2);

        let node_1 = dataset_graph.node_index_for_dataset(&dataset1).unwrap();
        let node_2 = dataset_graph.node_index_for_dataset(&dataset2).unwrap();
        let edges_connecting = dataset_graph.graph().edges_connecting(node_1, node_2);

        let mut has_data = false;
        let mut has_trigger = false;

        for edge in edges_connecting {
            match edge.weight() {
                TdVersions::Dataset { .. } => has_trigger = true,
                TdVersions::Table {
                    table, versions, ..
                } => {
                    assert_eq!(versions, &Versions::Single(Version::Head(0)));
                    assert_eq!(table, "table");
                    has_data = true
                }
            }
        }

        assert!(has_data && has_trigger);
    }

    #[test]
    fn test_dataset_graph_builder_no_data_link() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![]));

        let trigger_graph =
            TriggerGraph(Graph(vec![trigger_link_from_dataset(&dataset1, &dataset2)]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 2);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 1);

        let node_1 = dataset_graph.node_index_for_dataset(&dataset1).unwrap();
        let node_2 = dataset_graph.node_index_for_dataset(&dataset2).unwrap();
        let edges_connecting = dataset_graph.graph().edges_connecting(node_1, node_2);

        let mut has_data = false;
        let mut has_trigger = false;

        for edge in edges_connecting {
            match edge.weight() {
                TdVersions::Table { .. } => has_data = true,
                TdVersions::Dataset { .. } => has_trigger = true,
            }
        }

        assert!(!has_data && has_trigger);
    }

    #[test]
    fn test_dataset_graph_builder_no_trigger_link() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph = TriggerGraph(Graph(vec![]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 2);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 1);

        let node_1 = dataset_graph.node_index_for_dataset(&dataset1).unwrap();
        let node_2 = dataset_graph.node_index_for_dataset(&dataset2).unwrap();
        let edges_connecting = dataset_graph.graph().edges_connecting(node_1, node_2);

        let mut has_data = false;
        let mut has_trigger = false;

        for edge in edges_connecting {
            match edge.weight() {
                TdVersions::Dataset { .. } => has_trigger = true,
                TdVersions::Table {
                    table, versions, ..
                } => {
                    assert_eq!(versions, &Versions::Single(Version::Head(0)));
                    assert_eq!(table, "table");
                    has_data = true
                }
            }
        }

        assert!(has_data && !has_trigger);
    }

    #[test]
    fn test_dataset_graph_builder_cyclic_trigger() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![]));

        let trigger_graph = TriggerGraph(Graph(vec![
            trigger_link_from_dataset(&dataset1, &dataset2),
            trigger_link_from_dataset(&dataset2, &dataset1),
        ]));

        let result = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1)
            .unwrap();

        assert!(matches!(result.validate_dag(), Err(GraphError::Cyclic(_))));
    }

    #[test]
    fn test_dataset_graph_builder_cyclic_data() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");

        let data_graph = DataGraph(Graph(vec![
            data_link_from_dataset(&dataset1, &dataset2, "HEAD"),
            data_link_from_dataset(&dataset2, &dataset1, "HEAD"),
        ]));

        let trigger_graph = TriggerGraph(Graph(vec![]));

        let result = DatasetGraphBuilder::new(&data_graph, &trigger_graph).build(dataset1);

        assert!(result.is_ok());
    }

    #[test]
    fn test_dataset_graph_builder_multiple_edges() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");
        let dataset3 = Dataset::new("dst", "d3");

        let data_graph = DataGraph(Graph(vec![
            data_link_from_dataset(&dataset1, &dataset2, "HEAD"),
            data_link_from_dataset(&dataset2, &dataset3, "HEAD"),
            data_link_from_dataset(&dataset1, &dataset3, "HEAD"),
        ]));

        let trigger_graph = TriggerGraph(Graph(vec![
            trigger_link_from_dataset(&dataset1, &dataset2),
            trigger_link_from_dataset(&dataset2, &dataset3),
        ]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 3);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 5);
    }

    #[test]
    fn test_dataset_graph_builder_single_node() {
        let dataset1 = Dataset::new("dst", "d1");

        let data_graph = DataGraph(Graph(vec![]));
        let trigger_graph = TriggerGraph(Graph(vec![]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 1);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 0);
    }

    #[test]
    fn test_dataset_graph_builder_disconnected_graph() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");
        let dataset3 = Dataset::new("dst", "d3");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph =
            TriggerGraph(Graph(vec![trigger_link_from_dataset(&dataset2, &dataset3)]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 3);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 2);
    }

    #[test]
    fn test_dataset_graph_builder_multiple_triggers() {
        let dataset1 = Dataset::new("dst", "d1");
        let dataset2 = Dataset::new("dst", "d2");
        let dataset3 = Dataset::new("dst", "d3");

        let data_graph = DataGraph(Graph(vec![data_link_from_dataset(
            &dataset1, &dataset2, "HEAD",
        )]));

        let trigger_graph = TriggerGraph(Graph(vec![
            trigger_link_from_dataset(&dataset1, &dataset2),
            trigger_link_from_dataset(&dataset2, &dataset3),
        ]));

        let dataset_graph = DatasetGraphBuilder::new(&data_graph, &trigger_graph)
            .build(dataset1.clone())
            .unwrap();

        assert_eq!(dataset_graph.graph().node_count(), 3);
        assert_eq!(
            dataset_graph.graph()[*dataset_graph.trigger_index()],
            dataset1
        );
        assert_eq!(dataset_graph.graph().edge_count(), 3);
    }
}
