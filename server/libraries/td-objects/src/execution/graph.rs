//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::execution::defs::ResolvedVersionResponse;
use crate::dxo::function::defs::FunctionDBWithNames;
use crate::dxo::table::defs::TableDBWithNames;
use crate::dxo::table_data_version::defs::ExecutionTableDataVersionRead;
use crate::types::bool::{SelfDependency, System};
use crate::types::composed::TableVersions;
use crate::types::i32::{DependencyPos, TableFunctionParamPos};
use crate::types::id::{
    CollectionId, FunctionVersionId, TableDataVersionId, TableId, TableVersionId,
};
use crate::types::string::{CollectionName, FunctionName, TableName};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use td_error::TdError;
use utoipa::ToSchema;

/// Represents a function version to perform graph resolution.
#[td_type::Dto]
#[derive(Eq, PartialEq, Hash)]
#[td_type(builder(try_from = FunctionDBWithNames))]
pub struct FunctionNode {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    #[td_type(builder(field = "id"))]
    pub function_version_id: FunctionVersionId,
    pub name: FunctionName,
}

/// Represents a table version to perform graph resolution.
#[td_type::Dto]
#[derive(Eq, PartialEq, Hash)]
#[td_type(builder(try_from = TableDBWithNames))]
pub struct TableNode {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub table_id: TableId,
    #[td_type(builder(field = "id"))]
    pub table_version_id: TableVersionId,
    pub name: TableName,
    pub system: System,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct GraphDependency {
    pub dep_pos: DependencyPos,
    pub self_dependency: SelfDependency,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct GraphOutput {
    pub output_pos: Option<TableFunctionParamPos>,
}

/// Graph versions, which will always hold the versions of the table, either input or output.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, ToSchema)]
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
#[td_type::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct ResolvedVersion {
    pub inner: Vec<Option<ExecutionTableDataVersionRead>>,
    pub original: TableVersions,
}

impl Display for ResolvedVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.original)
    }
}

impl ResolvedVersion {
    pub fn to_response(
        &self,
    ) -> Result<
        (
            ResolvedVersionResponse,
            HashMap<TableDataVersionId, ExecutionTableDataVersionRead>,
        ),
        TdError,
    > {
        let mut resolved_info = HashMap::new();
        let inner = self
            .inner
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|t| {
                        resolved_info.insert(t.id, t.clone());
                        Ok::<_, TdError>(t.id)
                    })
                    .transpose()
            })
            .collect::<Result<_, _>>()?;

        Ok((
            ResolvedVersionResponse {
                inner,
                original: self.original.clone(),
            },
            resolved_info,
        ))
    }
}

/// Graph node representation. It can be a function or a table.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum GraphNode {
    Function(FunctionNode),
    Table(TableNode),
}

impl Display for GraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphNode::Function(node) => write!(f, "{}", node.name),
            GraphNode::Table(node) => write!(f, "{}", node.name),
        }
    }
}

impl GraphNode {
    pub fn output(table: &TableDBWithNames) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Function(
                FunctionNode::builder()
                    .collection_id(table.collection_id)
                    .collection(table.collection.clone())
                    .function_version_id(table.function_version_id)
                    .name(table.function.clone())
                    .build()?,
            ),
            GraphNode::Table(
                TableNode::builder()
                    .collection_id(table.collection_id)
                    .collection(table.collection.clone())
                    .function_version_id(table.function_version_id)
                    .table_id(table.table_id)
                    .table_version_id(table.id)
                    .name(table.name.clone())
                    .system(table.system.clone())
                    .build()?,
            ),
        ))
    }

    pub fn input(
        table: &TableDBWithNames,
        function: &FunctionDBWithNames,
    ) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Table(
                TableNode::builder()
                    .collection_id(table.collection_id)
                    .collection(table.collection.clone())
                    .function_version_id(table.function_version_id)
                    .table_id(table.table_id)
                    .table_version_id(table.id)
                    .name(table.name.clone())
                    .system(table.system.clone())
                    .build()?,
            ),
            GraphNode::Function(
                FunctionNode::builder()
                    .collection_id(function.collection_id)
                    .collection(function.collection.clone())
                    .function_version_id(function.id)
                    .name(function.name.clone())
                    .build()?,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::graph::{FUNCTION_NAMES, TABLE_NAMES, dependency, table, trigger};

    #[tokio::test]
    async fn test_graph_node_from_table() -> Result<(), TdError> {
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await;
        let (function_node, table_node) = GraphNode::output(&table)?;

        if let GraphNode::Function(function) = function_node {
            assert_eq!(function.collection_id, table.collection_id);
            assert_eq!(function.collection, table.collection);
            assert_eq!(function.function_version_id, table.function_version_id);
            assert_eq!(function.name, table.function);
        } else {
            panic!("Expected GraphNode::Function");
        }

        if let GraphNode::Table(table_node) = table_node {
            assert_eq!(table_node.collection_id, table.collection_id);
            assert_eq!(table_node.collection, table.collection);
            assert_eq!(table_node.function_version_id, table.function_version_id);
            assert_eq!(table_node.table_version_id, table.id);
            assert_eq!(table_node.name, table.name);
        } else {
            panic!("Expected GraphNode::Table");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_dependency() -> Result<(), TdError> {
        let (dependency, table, function) = dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await;
        let (table_node, function_node) = GraphNode::input(&table, &function)?;

        if let GraphNode::Table(node) = table_node {
            assert_eq!(node.collection_id, dependency.table_collection_id);
            assert_eq!(node.collection, dependency.table_collection);
            assert_eq!(node.function_version_id, table.function_version_id);
            assert_eq!(node.table_version_id, table.id);
            assert_eq!(node.name, table.name);
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id, dependency.collection_id);
            assert_eq!(node.collection, dependency.collection);
            assert_eq!(node.function_version_id, function.id);
            assert_eq!(node.name, function.name);
        } else {
            panic!("Expected GraphNode::Function");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_trigger() -> Result<(), TdError> {
        let (trigger, table, function) = trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await;
        let (table_node, function_node) = GraphNode::input(&table, &function)?;

        if let GraphNode::Table(node) = table_node {
            assert_eq!(node.collection_id, trigger.trigger_by_collection_id);
            assert_eq!(node.collection, trigger.trigger_by_collection);
            assert_eq!(node.function_version_id, table.function_version_id);
            assert_eq!(node.table_version_id, table.id);
            assert_eq!(node.name, table.name);
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id, trigger.collection_id);
            assert_eq!(node.collection, trigger.collection);
            assert_eq!(node.function_version_id, function.id);
            assert_eq!(node.name, node.name);
        } else {
            panic!("Expected GraphNode::Function");
        }
        Ok(())
    }
}
