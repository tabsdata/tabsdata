//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, DataLocation, DependencyPos, Dot, ExecutionId,
    ExecutionName, FunctionName, FunctionRunId, FunctionVersionId, HasData, InputIdx, Partitioned,
    RequirementId, SelfDependency, StorageVersion, TableDataVersionId, TableFunctionParamPos,
    TableId, TableName, TableVersionId, TableVersions, TransactionByStr, TransactionId,
    TransactionKey, Trigger, TriggeredOn, UserId, UserName, VersionPos, WorkerMessageId,
};
use crate::types::dependency::DependencyVersionDBWithNames;
use crate::types::function::FunctionVersionDBWithNames;
use crate::types::table::TableVersionDBWithNames;
use crate::types::trigger::TriggerVersionDBWithNames;
use crate::types::worker::FunctionOutput;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use td_common::datetime::IntoDateTimeUtc;
use td_common::execution_status::FunctionRunUpdateStatus;
use td_common::server::ResponseMessagePayload;
use td_error::TdError;

// Daos

#[td_type::Dao]
#[dao(sql_table = "executions")]
#[td_type(
    builder(try_from = FunctionVersionDBWithNames, skip_all),
    updater(try_from = RequestContext, skip_all),
    updater(try_from = ExecutionRequest, skip_all)
)]
pub struct ExecutionDB {
    #[builder(default)]
    #[td_type(extractor)]
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
    triggered_by_id: UserId,
}

#[td_type::Dao]
#[dao(sql_table = "executions__with_status")]
pub struct ExecutionDBWithStatus {
    id: ExecutionId,
    name: Option<ExecutionName>,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: ExecutionStatus,
}

#[td_type::Dao]
#[dao(sql_table = "transactions")]
#[td_type(builder(try_from = ExecutionDB, skip_all))]
pub struct TransactionDB {
    #[td_type(extractor)]
    id: TransactionId, // no default as it has to be calculated depending on the execution
    #[td_type(builder(field = "id"))]
    execution_id: ExecutionId,
    transaction_by: TransactionByStr,
    transaction_key: TransactionKey,
    #[td_type(builder(include))]
    triggered_on: TriggeredOn,
    #[td_type(builder(include))]
    triggered_by_id: UserId,
}

#[td_type::Dao]
#[dao(sql_table = "transactions__with_status")]
pub struct TransactionDBWithStatus {
    id: TransactionId,
    execution_id: ExecutionId,
    transaction_by: TransactionByStr,
    transaction_key: TransactionKey,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: TransactionStatus,
}

#[td_type::Dao]
#[dao(
    sql_table = "function_runs",
    partition_by = "id",
    versioned_at(order_by = "triggered_on", condition_by = "status")
)]
#[td_type(builder(try_from = ExecutionDB, skip_all))]
pub struct FunctionRunDB {
    #[builder(default)]
    id: FunctionRunId,
    collection_id: CollectionId, // this is not the ExecutionDB function_version_id, as that's the trigger
    function_version_id: FunctionVersionId, // this is not the ExecutionDB function_version_id, as that's the trigger
    #[td_type(extractor, builder(field = "id"))]
    execution_id: ExecutionId,
    #[td_type(extractor)]
    transaction_id: TransactionId,
    #[td_type(builder(include))]
    triggered_on: TriggeredOn,
    #[td_type(builder(include))]
    triggered_by_id: UserId,
    trigger: Trigger,
    #[builder(default)]
    started_on: Option<AtTime>,
    #[builder(default)]
    ended_on: Option<AtTime>,
    #[builder(default = FunctionRunStatus::Scheduled)]
    status: FunctionRunStatus,
}

#[td_type::Dao]
#[dao(sql_table = "function_runs__with_names")]
pub struct FunctionRunDBWithNames {
    id: FunctionRunId,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    triggered_on: TriggeredOn,
    trigger: Trigger,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,

    data_location: DataLocation,
    name: FunctionName,
    collection: CollectionName,
    execution: Option<ExecutionName>,
    triggered_by: UserName,
}

#[td_type::Dto]
#[td_type(builder(try_from = FunctionRunDBWithNames))]
pub struct FunctionRun {
    id: FunctionRunId,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    triggered_on: TriggeredOn,
    trigger: Trigger,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,

    data_location: DataLocation,
    name: FunctionName,
    collection: CollectionName,
    execution: Option<ExecutionName>,
    triggered_by: UserName,
    // TODO exception info
    // kind: Option<String>,
    // message: Option<String>,
    // error_code: Option<String>,
    // exit_status: i32,
}

#[td_type::Dao]
#[dao(sql_table = "executable_function_runs")]
pub struct ExecutableFunctionRunDB {
    #[td_type(extractor)]
    id: FunctionRunId,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    #[td_type(extractor)]
    execution_id: ExecutionId,
    #[td_type(extractor)]
    transaction_id: TransactionId,
    triggered_on: TriggeredOn,
    trigger: Trigger,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,

    data_location: DataLocation,
    storage_version: StorageVersion,
    bundle_id: BundleId,
    name: FunctionName,
    collection: CollectionName,
    execution: Option<ExecutionName>,
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions")]
pub struct TableDataVersionDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    table_version_id: TableVersionId,
    #[td_type(extractor)]
    function_version_id: FunctionVersionId,
    #[builder(default)]
    has_data: Option<HasData>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: Option<TableFunctionParamPos>,
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions__with_status")]
pub struct TableDataVersionDBWithStatus {
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    has_data: Option<HasData>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: Option<TableFunctionParamPos>,

    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    status: FunctionRunStatus,
    partitioned: Partitioned,
}

#[td_type::Dao]
#[dao(
    sql_table = "table_data_versions__with_names",
    partition_by = "table_version_id",
    versioned_at(order_by = "triggered_on", condition_by = "has_data")
)]
pub struct TableDataVersionDBWithNames {
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    has_data: Option<HasData>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: TableFunctionParamPos,

    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    status: FunctionRunStatus,
    partitioned: Partitioned,

    collection: CollectionName,
    function: FunctionName,
    triggered_by: UserName,
}

#[td_type::Dao]
#[dao(
    sql_table = "table_data_versions__active",
    partition_by = "table_id",
    versioned_at(order_by = "triggered_on", condition_by = "status")
)]
#[derive(Hash)]
pub struct ActiveTableDataVersionDB {
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    has_data: Option<HasData>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: TableFunctionParamPos,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    status: FunctionRunStatus,
    partitioned: Partitioned,
}

#[td_type::Dao]
#[dao(sql_table = "function_requirements")]
pub struct FunctionRequirementDB {
    #[builder(default)]
    id: RequirementId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    requirement_table_id: TableId,
    requirement_table_version_id: TableVersionId,
    #[builder(default)]
    requirement_function_run_id: Option<FunctionRunId>,
    #[builder(default)]
    requirement_table_data_version_id: Option<TableDataVersionId>,
    #[builder(default)]
    requirement_input_idx: Option<InputIdx>,
    #[builder(default)]
    requirement_dependency_pos: Option<DependencyPos>,
    requirement_version_pos: VersionPos,
}

#[td_type::Dao]
#[dao(sql_table = "function_requirements__with_status")]
pub struct FunctionRequirementDBWithStatus {
    id: RequirementId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    requirement_table_id: TableId,
    requirement_table_version_id: TableVersionId,
    requirement_function_run_id: Option<FunctionRunId>,
    requirement_table_data_version_id: Option<TableDataVersionId>,
    requirement_input_idx: Option<InputIdx>,
    requirement_dependency_pos: Option<DependencyPos>,
    requirement_version_pos: VersionPos,
    status: FunctionRunStatus,
}

#[td_type::Dao]
#[dao(
    sql_table = "function_requirements__with_names",
    partition_by = "id",
    versioned_at(order_by = "id", condition_by = "status"),
    recursive(up = "requirement_function_run_id", down = "function_run_id")
)]
pub struct FunctionRequirementDBWithNames {
    id: RequirementId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    requirement_table_id: TableId,
    requirement_table_version_id: TableVersionId,
    requirement_function_run_id: Option<FunctionRunId>,
    requirement_table_data_version_id: Option<TableDataVersionId>,
    requirement_input_idx: Option<InputIdx>,
    requirement_dependency_pos: Option<DependencyPos>,
    requirement_version_pos: VersionPos,
    status: FunctionRunStatus,
    collection: CollectionName,
    function: FunctionName,
    requirement_table: TableName,
}

#[td_type::Dao]
#[dao(sql_table = "worker_messages")]
pub struct WorkerMessageDB {
    #[builder(default)]
    id: WorkerMessageId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_version_id: FunctionVersionId,
    status: WorkerMessageStatus,
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of an execution.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Done --> [*]
///     Incomplete --> [*]
///
///     Scheduled --> Running
///     Running --> Done
///     Running --> Incomplete
/// ```
#[td_type::typed_enum]
pub enum ExecutionStatus {
    #[strum(to_string = "S")]
    Scheduled,
    #[strum(to_string = "R")]
    Running,
    #[strum(to_string = "D")]
    Done,
    #[strum(to_string = "I")]
    Incomplete,
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a transaction.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Published --> [*]
///
///     Scheduled --> Running
///     Scheduled --> OnHold
///     Scheduled --> Canceled
///     Running --> Published
///     Running --> Failed
///     Running --> Canceled
///     OnHold --> Canceled
///     OnHold --> Scheduled
///     Failed --> Scheduled
///     Failed --> Canceled
/// ```
#[td_type::typed_enum]
pub enum TransactionStatus {
    #[strum(serialize = "S")]
    Scheduled,
    #[strum(serialize = "R")]
    Running,
    #[strum(serialize = "F")]
    Failed,
    #[strum(serialize = "H")]
    OnHold,
    #[strum(serialize = "C")]
    Canceled,
    #[strum(serialize = "P")]
    Published,
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Represents the state of a function run.
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> Scheduled
///     Canceled --> [*]
///     Published --> [*]
///
///     Scheduled --> RunRequested
///     RunRequested --> Scheduled
///     RunRequested --> Running
///     Scheduled --> OnHold
///     Scheduled --> Canceled
///     Running --> Done
///     Running --> Error
///     Running --> Failed
///     Running --> Canceled
///     OnHold --> Canceled
///     OnHold --> Scheduled
///     Error --> Running
///     Error --> Canceled
///     Failed --> Scheduled
///     Failed --> Canceled
///     Done --> Published
///     Done --> Canceled
/// ```
#[td_type::typed_enum]
pub enum FunctionRunStatus {
    #[strum(to_string = "S")]
    Scheduled,
    #[strum(to_string = "RR")]
    RunRequested,
    #[strum(to_string = "RS")]
    ReScheduled,
    #[strum(to_string = "R")]
    Running,
    #[strum(to_string = "D")]
    Done,
    #[strum(to_string = "E")]
    Error,
    #[strum(to_string = "F")]
    Failed,
    #[strum(to_string = "H")]
    OnHold,
    #[strum(to_string = "C")]
    Canceled,
}

impl From<FunctionRunUpdateStatus> for FunctionRunStatus {
    fn from(value: FunctionRunUpdateStatus) -> Self {
        match value {
            FunctionRunUpdateStatus::Running => FunctionRunStatus::Running,
            FunctionRunUpdateStatus::Done => FunctionRunStatus::Done,
            FunctionRunUpdateStatus::Error => FunctionRunStatus::Error,
            FunctionRunUpdateStatus::Failed => FunctionRunStatus::Failed,
        }
    }
}

#[td_type::typed_enum]
pub enum WorkerMessageStatus {
    #[strum(to_string = "L")]
    Locked,
    #[strum(to_string = "U")]
    Unlocked,
}

// Update Daos and Dlos

#[td_type::Dlo]
pub struct UpdateFunctionRun {
    status: FunctionRunStatus,
    started_on: AtTime,
    ended_on: Option<AtTime>,
}

impl TryFrom<&CallbackRequest> for UpdateFunctionRun {
    type Error = TdError;

    fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
        Ok(UpdateFunctionRun::builder()
            .status(value.status().clone())
            .try_started_on(value.start().datetime_utc()?)?
            .ended_on(
                value
                    .end()
                    .map(|v| AtTime::try_from(v.datetime_utc()?))
                    .transpose()?,
            )
            .build()?)
    }
}

#[td_type::Dao]
#[dao(sql_table = "function_runs")]
#[td_type(builder(try_from = UpdateFunctionRun))]
pub struct UpdateFunctionRunDB {
    #[dao(immutable)]
    #[builder(default)]
    started_on: Option<AtTime>,
    #[dao(immutable)]
    #[builder(default)]
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,
}

impl UpdateFunctionRunDB {
    pub fn scheduled() -> Result<Self, TdError> {
        Ok(Self::builder()
            .status(FunctionRunStatus::Scheduled)
            .build()?)
    }

    pub async fn run_requested() -> Result<Self, TdError> {
        Ok(Self::builder()
            .status(FunctionRunStatus::RunRequested)
            .build()?)
    }

    pub async fn cancel() -> Result<Self, TdError> {
        Ok(Self::builder()
            .ended_on(AtTime::now().await)
            .status(FunctionRunStatus::Canceled)
            .build()?)
    }
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions")]
pub struct UpdateTableDataVersionDB {
    #[dao(immutable)]
    #[builder(default)]
    has_data: Option<HasData>,
}

#[td_type::Dao]
#[dao(sql_table = "worker_messages")]
pub struct UpdateWorkerMessageDB {
    status: WorkerMessageStatus,
}

impl UpdateWorkerMessageDB {
    pub fn unlocked() -> Result<Self, TdError> {
        Ok(Self::builder()
            .status(WorkerMessageStatus::Unlocked)
            .build()?)
    }
}

// Dtos

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

// TODO: Value is a placeholder, we need to define the actual type
pub type CallbackRequest = ResponseMessagePayload<FunctionOutput>;

// Dlos

/// Represents a function version to perform graph resolution.
#[td_type::Dlo]
#[derive(Hash)]
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
#[derive(Hash)]
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
#[derive(Hash)]
pub struct GraphDependency {
    dep_pos: DependencyPos,
    self_dependency: SelfDependency,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dlo]
#[derive(Hash)]
pub struct GraphOutput {
    output_pos: Option<TableFunctionParamPos>,
}

/// Graph versions, which will always hold the versions of the table, either input or output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
                write!(f, "{}", versions)
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
            GraphEdge::Dependency { dependency, .. } => Some(dependency.dep_pos()),
        }
    }

    pub fn output_pos(&self) -> Option<&TableFunctionParamPos> {
        match self {
            GraphEdge::Output { output, .. } => output.output_pos().as_ref(),
            GraphEdge::Trigger { .. } => None,
            GraphEdge::Dependency { .. } => None,
        }
    }
}

/// Represents the versions of a table. It has a list of optional tables because resolved `Versions`
/// can exist or not, and that is not necessarily an error.
#[td_type::Dlo]
#[derive(Hash)]
pub struct ResolvedVersion {
    inner: Vec<Option<ActiveTableDataVersionDB>>,
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
    use crate::types::test_utils::execution::{
        dependency, table, trigger, FUNCTION_NAMES, TABLE_NAMES,
    };

    #[tokio::test]
    async fn test_graph_node_from_table() -> Result<(), TdError> {
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await;
        let (function_node, table_node) = GraphNode::from_table(&table)?;

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
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_dependency() -> Result<(), TdError> {
        let dependency = dependency(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await;
        let (table_node, function_node) = GraphNode::from_dependency(&dependency)?;

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
        Ok(())
    }

    #[tokio::test]
    async fn test_graph_node_from_trigger() -> Result<(), TdError> {
        let trigger = trigger(&TABLE_NAMES[0], &FUNCTION_NAMES[0]).await;
        let (table_node, function_node) = GraphNode::from_trigger(&trigger)?;

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
        Ok(())
    }
}
