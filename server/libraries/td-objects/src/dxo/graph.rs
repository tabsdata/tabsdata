//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::function::FunctionDBWithNames;
use crate::dxo::table::TableDBWithNames;
use crate::types::basic::{
    CollectionId, CollectionName, DependencyPos, Enabled, FunctionId, FunctionName,
    FunctionVersionId, GroupId, LogicalTableDataVersionId, Parent, Private, SelfDependency, System,
    TableFunctionParamPos, TableId, TableName, TableVersionId,
};
use crate::types::composed::TableVersions;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use td_error::TdError;
use utoipa::ToSchema;

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct FunctionNode {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    pub name: FunctionName,
    pub enabled: Enabled,
    pub group_id: GroupId,
    #[serde(default)]
    pub parent_function_id: Option<Parent<FunctionId>>,
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
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub name: TableName,
    pub system: System,
    pub private: Private,
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

impl GraphNode<FunctionNode, TableNode> {
    pub fn pair(
        function: &FunctionDBWithNames,
        table: &TableDBWithNames,
    ) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Function(FunctionNode {
                collection_id: function.base.base.collection_id,
                collection: function.collection.clone(),
                function_id: function.base.base.function_id,
                function_version_id: function.base.base.id.clone(),
                name: function.base.base.name.clone(),
                enabled: function.base.base.enabled,
                group_id: function.group_id,
                parent_function_id: function.base.base.parent_function_id,
            }),
            GraphNode::Table(TableNode {
                collection_id: table.base.base.collection_id,
                collection: table.collection.clone(),
                function_version_id: table.base.base.function_version_id.clone(),
                table_id: table.base.base.table_id,
                table_version_id: table.base.base.id,
                name: table.base.base.name.clone(),
                system: table.system,
                private: table.base.base.private,
            }),
        ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::graph::{
        FUNCTION_NAMES, TABLE_NAMES, dependency, function, table, trigger,
    };

    #[tokio::test]
    async fn test_graph_node_from_table() -> Result<(), TdError> {
        let function = function(&FUNCTION_NAMES[0]);
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]);
        let (function_node, table_node) = GraphNode::pair(&function, &table)?;

        if let GraphNode::Function(function) = function_node {
            assert_eq!(function.collection_id, table.base.base.collection_id);
            assert_eq!(function.collection, table.collection);
            assert_eq!(
                function.function_version_id,
                table.base.base.function_version_id
            );
            assert_eq!(function.name, table.function);
        } else {
            panic!("Expected GraphNode::Function");
        }

        if let GraphNode::Table(table_node) = table_node {
            assert_eq!(table_node.collection_id, table.base.base.collection_id);
            assert_eq!(table_node.collection, table.collection);
            assert_eq!(
                table_node.function_version_id,
                table.base.base.function_version_id
            );
            assert_eq!(table_node.table_version_id, table.base.base.id);
            assert_eq!(table_node.name, table.base.base.name);
        } else {
            panic!("Expected GraphNode::Table");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_dependency() -> Result<(), TdError> {
        let dependency = dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]);
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]);
        let function = function(&FUNCTION_NAMES[0]);
        let (function_node, table_node) = GraphNode::pair(&function, &table)?;

        if let GraphNode::Table(node) = table_node {
            assert_eq!(
                node.collection_id,
                *dependency.base.base.table_collection_id
            );
            assert_eq!(node.collection, *dependency.table_collection);
            assert_eq!(
                node.function_version_id,
                table.base.base.function_version_id
            );
            assert_eq!(node.table_version_id, table.base.base.id);
            assert_eq!(node.name, table.base.base.name);
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id, dependency.base.base.collection_id);
            assert_eq!(node.collection, dependency.collection);
            assert_eq!(node.function_version_id, function.base.base.id);
            assert_eq!(node.name, function.base.base.name);
        } else {
            panic!("Expected GraphNode::Function");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_trigger() -> Result<(), TdError> {
        let trigger = trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[0]);
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]);
        let function = function(&FUNCTION_NAMES[0]);
        let (function_node, table_node) = GraphNode::pair(&function, &table)?;

        if let GraphNode::Table(node) = table_node {
            assert_eq!(
                node.collection_id,
                *trigger.base.base.trigger_by_collection_id
            );
            assert_eq!(node.collection, *trigger.trigger_by_collection);
            assert_eq!(
                node.function_version_id,
                table.base.base.function_version_id
            );
            assert_eq!(node.table_version_id, table.base.base.id);
            assert_eq!(node.name, table.base.base.name);
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id, trigger.base.base.collection_id);
            assert_eq!(node.collection, trigger.collection);
            assert_eq!(node.function_version_id, function.base.base.id);
            assert_eq!(node.name, node.name);
        } else {
            panic!("Expected GraphNode::Function");
        }
        Ok(())
    }
}
