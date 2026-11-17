//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::execution::{FunctionNodeResponse, TableNodeResponse};
use crate::types::basic::{
    AtTime, CollectionId, CollectionLabel, DependencyPos, Enabled, FunctionId, FunctionName,
    FunctionVersionId, GroupId, GroupLabel, LogicalTableDataVersionId, Parent, Private,
    SelfDependency, System, TableFunctionParamPos, TableId, TableName, TableVersionId,
    TopologyTags, TopologyVersionId,
};
use crate::types::composed::TableVersions;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use utoipa::ToSchema;

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct FunctionNode {
    pub collection_id: CollectionId,
    pub collection: CollectionLabel,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    pub name: FunctionName,
    pub enabled: Enabled,
    pub group_id: GroupId,
    #[serde(default)]
    pub parent_function_id: Option<Parent<FunctionId>>,
    #[serde(default)]
    pub tags: TopologyTags,
}

impl Display for FunctionNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.collection, self.name)
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct TableNode {
    pub collection_id: CollectionId,
    pub collection: CollectionLabel,
    pub group_id: GroupId,
    pub function_version_id: FunctionVersionId,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub name: TableName,
    pub system: System,
    pub private: Private,
    #[serde(default)]
    pub tags: TopologyTags,
}

impl Display for TableNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.collection, self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, ToSchema)]
pub enum GraphNode<F = FunctionNode, T = TableNode> {
    Function(F),
    Table(T),
}

impl<F: Display, T: Display> Display for GraphNode<F, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphNode::Function(node) => write!(f, "{}", node),
            GraphNode::Table(node) => write!(f, "{}", node),
        }
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GraphDependency {
    pub dep_pos: DependencyPos,
    pub self_dependency: SelfDependency,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GraphOutput {
    pub output_pos: Option<TableFunctionParamPos>,
}

/// Graph versions, which will always hold the versions of the table, either input or output.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, ToSchema)]
pub enum GraphEdge<V> {
    // Table create
    Output {
        versions: V,
        output: GraphOutput,
    },
    // Function trigger
    Trigger {
        versions: V,
    },
    // Function data (doesn't imply a trigger)
    Dependency {
        versions: V,
        dependency: GraphDependency,
    },
}

impl<V: Display> Display for GraphEdge<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphEdge::Output { .. } => Ok(()),
            GraphEdge::Trigger { .. } => Ok(()),
            GraphEdge::Dependency { versions, .. } => {
                write!(f, "{versions}")
            }
        }
    }
}

impl<V> GraphEdge<V> {
    pub fn output(versions: V, output: GraphOutput) -> Self {
        Self::Output { versions, output }
    }

    pub fn trigger(versions: V) -> Self {
        Self::Trigger { versions }
    }

    pub fn dependency(versions: V, dependency: GraphDependency) -> Self {
        Self::Dependency {
            versions,
            dependency,
        }
    }

    pub fn versioned<VV>(&self, new_version: VV) -> GraphEdge<VV> {
        match self {
            GraphEdge::Output { output, .. } => GraphEdge::Output {
                versions: new_version,
                output: output.clone(),
            },
            GraphEdge::Trigger { .. } => GraphEdge::Trigger {
                versions: new_version,
            },
            GraphEdge::Dependency { dependency, .. } => GraphEdge::Dependency {
                versions: new_version,
                dependency: dependency.clone(),
            },
        }
    }

    pub fn versions(&self) -> &V {
        match self {
            GraphEdge::Output { versions, .. } => versions,
            GraphEdge::Trigger { versions, .. } => versions,
            GraphEdge::Dependency { versions, .. } => versions,
        }
    }

    pub fn dependency_pos(&self) -> Option<&DependencyPos> {
        match self {
            GraphEdge::Output { .. } => None,
            GraphEdge::Trigger { .. } => None,
            GraphEdge::Dependency { dependency, .. } => Some(&dependency.dep_pos),
        }
    }

    /// True for `Dependency` edges marked `self_dependency` (a function
    /// depending on its own output table). Self-dep semantics: the consumer
    /// reads the *previous* version, not the current one — V2 plan-build
    /// shifts `Versions` by -1 before resolving, mirroring V1's resolver.
    pub fn is_self_dependency(&self) -> bool {
        match self {
            GraphEdge::Dependency { dependency, .. } => *dependency.self_dependency,
            _ => false,
        }
    }

    pub fn output_pos(&self) -> Option<&TableFunctionParamPos> {
        match self {
            GraphEdge::Output { output, .. } => output.output_pos.as_ref(),
            GraphEdge::Trigger { .. } => None,
            GraphEdge::Dependency { .. } => None,
        }
    }
}

/// Represents the versions of a table. It has a list of optional tables because resolved `Versions`
/// can exist or not, and that is not necessarily an error.
#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct ResolvedVersion {
    pub inner: Vec<Option<LogicalTableDataVersionId>>,
    pub original: TableVersions,
}

impl Display for ResolvedVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.original)
    }
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GraphRelation<V> {
    pub function_version_id: FunctionVersionId,
    pub table_version_id: TableVersionId,
    pub edge: GraphEdge<V>,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct GraphCollection {
    pub name: CollectionLabel,
    pub group_id: GroupId,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct GraphGroup {
    pub name: GroupLabel,
}

/// The collections and groups a graph names, labelled as they are now, not as
/// of the graph's topology version: their current name, `[<id>]` once
/// deleted.
#[ta_objects::Dto]
pub struct GraphLabels {
    pub collections: BTreeMap<CollectionId, GraphCollection>,
    pub groups: BTreeMap<GroupId, GraphGroup>,
}

/// A graph of functions and tables, keyed by version.
#[ta_objects::Dto]
pub struct GraphResponse {
    pub all_functions: BTreeMap<FunctionVersionId, FunctionNodeResponse>,
    pub all_tables: BTreeMap<TableVersionId, TableNodeResponse>,
    pub system_tables: BTreeSet<TableVersionId>,
    pub user_tables: BTreeSet<TableVersionId>,
    pub relations: Vec<GraphRelation<TableVersions>>,
    #[serde(flatten)]
    pub labels: GraphLabels,
}

#[ta_objects::Dto]
pub struct TableFlowGraphResponse {
    pub table: TableVersionId,
    #[serde(flatten)]
    pub graph: GraphResponse,
}

/// One version of a project's whole topology, empty until something is
/// registered. A read at `at` later returns the same functions, tables and
/// relations; collection and group labels are the current ones.
#[ta_objects::Dto]
pub struct ProjectGraphResponse {
    pub version: TopologyVersionId,
    pub at: AtTime,
    #[serde(flatten)]
    pub graph: GraphResponse,
}
