//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, CollectionId, CollectionName, ConditionId, ConditionStatus, DependencyPos, Dot,
    ExecutionId, ExecutionName, ExecutionStatus, FunctionName, FunctionRunId, FunctionRunStatus,
    FunctionVersionId, HasData, SelfDependency, TableDataVersionId, TableDataVersionStatus,
    TableId, TableName, TableVersionId, TableVersions, TransactionByStr, TransactionId,
    TransactionKey, TransactionStatus, Trigger, TriggeredById, TriggeredOn,
};
use crate::types::dependency::DependencyVersionDBWithNames;
use crate::types::function::FunctionVersionDBWithNames;
use crate::types::table::TableVersionDBWithNames;
use crate::types::trigger::TriggerVersionDBWithNames;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use td_error::TdError;

#[td_type::Dao(sql_table = "executions")]
#[td_type(builder(try_from = FunctionVersionDBWithNames, skip_all))]
#[td_type(updater(try_from = RequestContext, skip_all))]
#[td_type(updater(try_from = ExecutionRequest, skip_all))]
pub struct ExecutionDB {
    #[builder(default)]
    id: ExecutionId,
    #[td_type(updater(try_from = ExecutionRequest, include))]
    name: Option<ExecutionName>,
    #[td_type(builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(field = "id"))]
    function_version_id: FunctionVersionId,
    #[td_type(updater(try_from = RequestContext, include, field = "time"))]
    triggered_on: TriggeredOn,
    #[td_type(updater(try_from = RequestContext, field = "user_id"))]
    triggered_by_id: TriggeredById,
    #[builder(default)]
    started_on: Option<AtTime>,
    #[builder(default)]
    ended_on: Option<AtTime>,
    #[builder(default = "ExecutionStatus::scheduled()")]
    status: ExecutionStatus,
}

#[td_type::Dao(sql_table = "transactions")]
#[td_type(builder(try_from = ExecutionDB, skip_all))]
pub struct TransactionDB {
    id: TransactionId, // no default as it has to be calculated depending on the execution
    #[td_type(builder(field = "id"))]
    execution_id: ExecutionId,
    transaction_by: TransactionByStr,
    transaction_key: TransactionKey,
    #[td_type(builder(include))]
    triggered_on: TriggeredOn,
    #[td_type(builder(include))]
    triggered_by_id: TriggeredById,
    #[builder(default)]
    started_on: Option<AtTime>,
    #[builder(default)]
    ended_on: Option<AtTime>,
    #[builder(default = "TransactionStatus::scheduled()")]
    status: TransactionStatus,
}

#[td_type::Dao(sql_table = "function_runs")]
#[td_type(builder(try_from = ExecutionDB, skip_all))]
pub struct FunctionRunDB {
    #[builder(default)]
    id: FunctionRunId,
    collection_id: CollectionId, // this is not the ExecutionDB function_version_id, as that's the trigger
    function_version_id: FunctionVersionId, // this is not the ExecutionDB function_version_id, as that's the trigger
    #[td_type(builder(field = "id"))]
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    #[td_type(builder(include))]
    triggered_on: TriggeredOn,
    trigger: Trigger,
    #[builder(default)]
    started_on: Option<AtTime>,
    #[builder(default)]
    ended_on: Option<AtTime>,
    #[builder(default = "FunctionRunStatus::scheduled()")]
    status: FunctionRunStatus,
}

#[td_type::Dao(
    sql_table = "table_data_versions",
    partition_by = "table_id",
    natural_order_by = "triggered_on"
)]
pub struct TableDataVersionDB {
    #[builder(default)]
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    #[builder(default)]
    has_data: Option<HasData>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    triggered_on: TriggeredOn,
    triggered_by_id: TriggeredById,
    #[builder(default = "TableDataVersionStatus::incomplete()")]
    status: TableDataVersionStatus,
}

// TODO we could have several requirement tables adding more conditions between functions
#[td_type::Dao(sql_table = "function_requirements")]
pub struct FunctionRequirementDB {
    #[builder(default)]
    id: ConditionId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    condition_function_run_id: FunctionRunId,
    condition_table_data_version: TableDataVersionId,
    #[builder(default = "ConditionStatus::incomplete()")]
    status: ConditionStatus,
}

#[td_type::Dto]
pub struct ExecutionRequest {
    name: Option<ExecutionName>,
}

#[td_type::Dto]
pub struct ExecutionResponse {
    id: ExecutionId,
    name: Option<ExecutionName>,
    all_functions: Vec<FunctionVersionResponse>,
    triggered_functions: Vec<FunctionVersionResponse>,
    manual_trigger: FunctionVersionResponse,
    all_tables: Vec<TableVersionResponse>,
    created_tables: Vec<TableVersionResponse>,
    triggered_on: TriggeredOn,
    dot: Dot,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionVersionNode))]
pub struct FunctionVersionResponse {
    collection_id: CollectionId,
    collection: CollectionName,
    function_version_id: FunctionVersionId,
    name: FunctionName,
}

#[td_type::Dto]
#[td_type(builder(try_from = TableVersionNode))]
pub struct TableVersionResponse {
    collection_id: CollectionId,
    collection: CollectionName,
    function_version_id: FunctionVersionId,
    table_id: TableId,
    table_version_id: TableVersionId,
    name: TableName,
}

/// Represents a function version to perform graph resolution.
#[td_type::Dlo]
#[td_type(builder(try_from = FunctionVersionDBWithNames))]
pub struct FunctionVersionNode {
    collection_id: CollectionId,
    collection: CollectionName,
    #[td_type(builder(field = "id"))]
    function_version_id: FunctionVersionId,
    name: FunctionName,
}

/// Represents a table version to perform graph resolution.
#[td_type::Dlo]
#[td_type(builder(try_from = TableVersionDBWithNames))]
pub struct TableVersionNode {
    collection_id: CollectionId,
    collection: CollectionName,
    function_version_id: FunctionVersionId,
    table_id: TableId,
    #[td_type(builder(field = "id"))]
    table_version_id: TableVersionId,
    name: TableName,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dlo]
pub struct GraphDependency {
    dep_pos: DependencyPos,
    self_dependency: SelfDependency,
}

/// Graph versions, which will always hold the versions of the table, either input or output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GraphEdge<V> {
    // Table create
    Output {
        versions: V,
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
                write!(f, "{}", versions)
            }
        }
    }
}

impl<V> GraphEdge<V> {
    pub fn output(versions: V) -> Self {
        Self::Output { versions }
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
            GraphEdge::Output { .. } => GraphEdge::Output {
                versions: new_version,
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
}

/// Represents the versions of a table. It has a list of optional tables because resolved `Versions`
/// can exist or not, and that is not necessarily an error.
#[td_type::Dlo]
pub struct ResolvedVersion {
    inner: Vec<Option<TableDataVersionDB>>,
    original: TableVersions,
}

impl Display for ResolvedVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// Graph node representation. It can be a function or a table.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum GraphNode {
    Function(FunctionVersionNode),
    Table(TableVersionNode),
}

impl Display for GraphNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphNode::Function(node) => write!(f, "{}", node.name()),
            GraphNode::Table(node) => write!(f, "{}", node.name()),
        }
    }
}

impl GraphNode {
    pub fn from_table(table: &TableVersionDBWithNames) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Function(
                FunctionVersionNode::builder()
                    .collection_id(table.collection_id())
                    .collection(table.collection())
                    .function_version_id(table.function_version_id())
                    .name(table.function())
                    .build()?,
            ),
            GraphNode::Table(
                TableVersionNode::builder()
                    .collection_id(table.collection_id())
                    .collection(table.collection())
                    .function_version_id(table.function_version_id())
                    .table_id(table.table_id())
                    .table_version_id(table.id())
                    .name(table.name())
                    .build()?,
            ),
        ))
    }

    pub fn from_dependency(dep: &DependencyVersionDBWithNames) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Table(
                TableVersionNode::builder()
                    .collection_id(dep.table_collection_id())
                    .collection(dep.table_collection())
                    .function_version_id(dep.table_function_version_id())
                    .table_id(dep.table_id())
                    .table_version_id(dep.table_version_id())
                    .name(dep.table_name())
                    .build()?,
            ),
            GraphNode::Function(
                FunctionVersionNode::builder()
                    .collection_id(dep.collection_id())
                    .collection(dep.collection())
                    .function_version_id(dep.function_version_id())
                    .name(dep.function())
                    .build()?,
            ),
        ))
    }

    pub fn from_trigger(trigger: &TriggerVersionDBWithNames) -> Result<(Self, Self), TdError> {
        Ok((
            GraphNode::Table(
                TableVersionNode::builder()
                    .collection_id(trigger.trigger_by_collection_id())
                    .collection(trigger.trigger_by_collection())
                    .function_version_id(trigger.trigger_by_function_version_id())
                    .table_id(trigger.trigger_by_table_id())
                    .table_version_id(trigger.trigger_by_table_version_id())
                    .name(trigger.trigger_by_table_name())
                    .build()?,
            ),
            GraphNode::Function(
                FunctionVersionNode::builder()
                    .collection_id(trigger.collection_id())
                    .collection(trigger.collection())
                    .function_version_id(trigger.function_version_id())
                    .name(trigger.function())
                    .build()?,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::test_utils::execution::{dependency, table, trigger};

    #[tokio::test]
    async fn test_graph_node_from_table() {
        let table = table("function_1", "table_1").await;
        let (function_node, table_node) = GraphNode::from_table(&table).unwrap();

        if let GraphNode::Function(function) = function_node {
            assert_eq!(function.collection_id(), table.collection_id());
            assert_eq!(function.collection(), table.collection());
            assert_eq!(function.function_version_id(), table.function_version_id());
            assert_eq!(function.name(), table.function());
        } else {
            panic!("Expected GraphNode::Function");
        }

        if let GraphNode::Table(table_node) = table_node {
            assert_eq!(table_node.collection_id(), table.collection_id());
            assert_eq!(table_node.collection(), table.collection());
            assert_eq!(
                table_node.function_version_id(),
                table.function_version_id()
            );
            assert_eq!(table_node.table_id(), table.table_id());
            assert_eq!(table_node.table_version_id(), table.id());
            assert_eq!(table_node.name(), table.name());
        } else {
            panic!("Expected GraphNode::Table");
        }
    }

    #[tokio::test]
    async fn test_graph_node_from_dependency() {
        let dependency = dependency("table_1", "function_2").await;
        let (table_node, function_node) = GraphNode::from_dependency(&dependency).unwrap();

        if let GraphNode::Table(table) = table_node {
            assert_eq!(table.collection_id(), dependency.table_collection_id());
            assert_eq!(table.collection(), dependency.table_collection());
            assert_eq!(
                table.function_version_id(),
                dependency.table_function_version_id()
            );
            assert_eq!(table.table_id(), dependency.table_id());
            assert_eq!(table.table_version_id(), dependency.table_version_id());
            assert_eq!(table.name(), dependency.table_name());
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(function) = function_node {
            assert_eq!(function.collection_id(), dependency.collection_id());
            assert_eq!(function.collection(), dependency.collection());
            assert_eq!(
                function.function_version_id(),
                dependency.function_version_id()
            );
            assert_eq!(function.name(), dependency.function());
        } else {
            panic!("Expected GraphNode::Function");
        }
    }

    #[tokio::test]
    async fn test_graph_node_from_trigger() {
        let trigger = trigger("table_1", "function_2").await;
        let (table_node, function_node) = GraphNode::from_trigger(&trigger).unwrap();

        if let GraphNode::Table(table) = table_node {
            assert_eq!(table.collection_id(), trigger.trigger_by_collection_id());
            assert_eq!(table.collection(), trigger.trigger_by_collection());
            assert_eq!(
                table.function_version_id(),
                trigger.trigger_by_function_version_id()
            );
            assert_eq!(table.table_id(), trigger.trigger_by_table_id());
            assert_eq!(
                table.table_version_id(),
                trigger.trigger_by_table_version_id()
            );
            assert_eq!(table.name(), trigger.trigger_by_table_name());
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(function) = function_node {
            assert_eq!(function.collection_id(), trigger.collection_id());
            assert_eq!(function.collection(), trigger.collection());
            assert_eq!(
                function.function_version_id(),
                trigger.function_version_id()
            );
            assert_eq!(function.name(), trigger.function());
        } else {
            panic!("Expected GraphNode::Function");
        }
    }
}
