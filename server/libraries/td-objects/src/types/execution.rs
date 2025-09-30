//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, BundleId, CollectionId, CollectionName, ColumnCount, DataChanged, DataLocation,
    DependencyPos, Dot, ExecutionId, ExecutionName, ExecutionStatus, FunctionName, FunctionRunId,
    FunctionRunStatus, FunctionRunStatusCount, FunctionVersionId, GlobalStatus, HasData, InputIdx,
    RequirementId, RowCount, SchemaHash, SelfDependency, StatusCount, StorageVersion, System,
    TableDataVersionId, TableFunctionParamPos, TableId, TableName, TableVersionId, TableVersions,
    TransactionByStr, TransactionId, TransactionKey, TransactionStatus, Trigger, TriggeredOn,
    UserId, UserName, VersionPos, WorkerId, WorkerStatus,
};
use crate::types::function::FunctionDBWithNames;
use crate::types::table::TableDBWithNames;
use crate::types::worker::FunctionOutput;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use td_common::datetime::IntoDateTimeUtc;
use td_common::execution_status::WorkerCallbackStatus;
use td_common::server::ResponseMessagePayload;
use td_error::TdError;
use utoipa::ToSchema;

#[td_type::Dao]
#[dao(sql_table = "global_status_summary")]
pub struct GlobalStatusSummaryDB {
    status: GlobalStatus,
    function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
}

// Daos

#[td_type::Dao]
#[dao(sql_table = "executions")]
#[td_type(
    builder(try_from = FunctionDBWithNames, skip_all),
    updater(try_from = RequestContext, skip_all),
    updater(try_from = ExecutionRequest, skip_all)
)]
pub struct ExecutionDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: ExecutionId,
    #[td_type(updater(try_from = ExecutionRequest, include))]
    name: Option<ExecutionName>,
    #[td_type(extractor, builder(include))]
    collection_id: CollectionId,
    #[td_type(builder(field = "id"))]
    #[td_type(extractor)]
    function_version_id: FunctionVersionId,
    #[td_type(updater(try_from = RequestContext, include, field = "time"))]
    #[td_type(extractor)]
    triggered_on: TriggeredOn,
    #[td_type(updater(try_from = RequestContext, field = "user_id"))]
    triggered_by_id: UserId,
}

#[td_type::Dao]
#[dao(sql_table = "executions__with_names")]
pub struct ExecutionDBWithNames {
    id: ExecutionId,
    name: Option<ExecutionName>,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,

    collection: CollectionName,
    function: FunctionName,
    triggered_by: UserName,
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

    collection: CollectionName,
    function: FunctionName,
    triggered_by: UserName,

    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: ExecutionStatus,
    function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
}

#[td_type::Dto]
#[td_type(builder(try_from = ExecutionDBWithStatus))]
#[dto(list(on = ExecutionDBWithStatus))]
pub struct Execution {
    #[dto(list(filter, filter_like, order_by))]
    id: ExecutionId,
    #[dto(list(filter, filter_like))]
    name: Option<ExecutionName>,
    #[dto(list(filter, filter_like, order_by))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    function_version_id: FunctionVersionId,
    #[dto(list(pagination_by = "+", filter, filter_like))]
    triggered_on: TriggeredOn,
    #[dto(list(filter, filter_like, order_by))]
    triggered_by_id: UserId,

    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    function: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    triggered_by: UserName,

    #[dto(list(filter, filter_like))]
    started_on: Option<AtTime>,
    #[dto(list(filter, filter_like))]
    ended_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    status: ExecutionStatus,
    function_run_status_count: FunctionRunStatusCount,
}

#[td_type::Dao]
#[dao(sql_table = "transactions")]
#[td_type(builder(try_from = ExecutionDB, skip_all))]
pub struct TransactionDB {
    #[td_type(extractor)]
    id: TransactionId, // no default as it has to be calculated depending on the execution
    #[td_type(extractor)] // tied to its functions, not the execution
    collection_id: CollectionId,
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
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_by: TransactionByStr,
    transaction_key: TransactionKey,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: TransactionStatus,
    collection: CollectionName,
    execution: Option<ExecutionName>,
    triggered_by: UserName,

    function_run_status_count: sqlx::types::Json<HashMap<FunctionRunStatus, StatusCount>>,
}

#[td_type::Dto]
#[td_type(builder(try_from = TransactionDBWithStatus))]
#[dto(list(on = TransactionDBWithStatus))]
pub struct SynchrotronResponse {
    #[dto(list(filter, filter_like))]
    id: TransactionId,
    #[dto(list(filter, filter_like))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like))]
    execution_id: ExecutionId,
    #[dto(list(pagination_by = "+", filter, filter_like))]
    triggered_on: TriggeredOn,
    #[dto(list(filter, filter_like))]
    triggered_by_id: UserId,
    #[dto(list(filter, filter_like))]
    status: TransactionStatus,
}

#[td_type::Dto]
#[td_type(builder(try_from = TransactionDBWithStatus))]
#[dto(list(on = TransactionDBWithStatus))]
pub struct Transaction {
    #[dto(list(filter, filter_like, order_by))]
    id: TransactionId,
    #[dto(list(filter, filter_like))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like))]
    execution_id: ExecutionId,
    #[dto(list(pagination_by = "+", filter, filter_like))]
    triggered_on: TriggeredOn,
    #[dto(list(filter, order_by))]
    started_on: Option<AtTime>,
    #[dto(list(filter, order_by))]
    ended_on: Option<AtTime>,
    #[dto(list(filter, filter_like))]
    triggered_by_id: UserId,
    #[dto(list(filter, filter_like))]
    status: TransactionStatus,
    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like))]
    execution: Option<ExecutionName>,
    #[dto(list(filter, filter_like, order_by))]
    triggered_by: UserName,

    function_run_status_count: FunctionRunStatusCount,
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
#[dao(
    sql_table = "function_runs__with_names",
    partition_by = "id",
    versioned_at(order_by = "triggered_on", condition_by = "status")
)]
pub struct FunctionRunDBWithNames {
    id: FunctionRunId,
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    trigger: Trigger,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,

    name: FunctionName,
    collection: CollectionName,
    execution: Option<ExecutionName>,
    triggered_by: UserName,
}

#[td_type::Dto]
#[dto(list(on = FunctionRunDBWithNames))]
#[td_type(builder(try_from = FunctionRunDBWithNames))]
pub struct FunctionRun {
    #[dto(list(filter, filter_like, order_by))]
    id: FunctionRunId,
    #[dto(list(filter, filter_like, order_by))]
    collection_id: CollectionId,
    function_version_id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    execution_id: ExecutionId,
    #[dto(list(filter, filter_like, order_by))]
    transaction_id: TransactionId,
    #[dto(list(pagination_by = "+", filter, filter_like))]
    triggered_on: TriggeredOn,
    trigger: Trigger,
    #[dto(list(filter, filter_like))]
    started_on: Option<AtTime>,
    #[dto(list(filter, filter_like))]
    ended_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    status: FunctionRunStatus,

    #[dto(list(filter, filter_like, order_by))]
    name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like))]
    execution: Option<ExecutionName>,
    triggered_by: UserName,
    // TODO exception info
    // kind: Option<String>,
    // message: Option<String>,
    // error_code: Option<String>,
    // exit_status: i32,
}

#[td_type::Dao]
#[dao(sql_table = "function_runs__to_execute")]
pub struct FunctionRunToExecuteDB {
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

    name: FunctionName,
    collection: CollectionName,
    execution: Option<ExecutionName>,
    triggered_by: UserName,

    data_location: DataLocation,
    storage_version: StorageVersion,
    bundle_id: BundleId,
}

#[td_type::Dao]
#[dao(sql_table = "function_runs__to_commit")]
pub struct FunctionRunToCommitDB {
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
    #[builder(default)]
    column_count: Option<ColumnCount>,
    #[builder(default)]
    row_count: Option<RowCount>,
    #[builder(default)]
    schema_hash: Option<SchemaHash>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: Option<TableFunctionParamPos>,
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions__with_function")]
pub struct TableDataVersionDBWithFunction {
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    has_data: Option<HasData>,
    column_count: Option<ColumnCount>,
    row_count: Option<RowCount>,
    schema_hash: Option<SchemaHash>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: Option<TableFunctionParamPos>,

    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,
    data_location: DataLocation,
    storage_version: StorageVersion,
    with_data_table_data_version_id: Option<TableDataVersionId>,
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
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,
    data_location: DataLocation,
    storage_version: StorageVersion,
    with_data_table_data_version_id: Option<TableDataVersionId>,
}

#[td_type::Dao]
#[dao(
    sql_table = "table_data_versions__with_names",
    partition_by = "table_id",
    versioned_at(order_by = "triggered_on", condition_by = "status")
)]
pub struct TableDataVersionDBWithNames {
    id: TableDataVersionId,
    collection_id: CollectionId,
    table_id: TableId,
    name: TableName,
    table_version_id: TableVersionId,
    function_version_id: FunctionVersionId,
    has_data: Option<HasData>,
    column_count: Option<ColumnCount>,
    row_count: Option<RowCount>,
    schema_hash: Option<SchemaHash>,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_param_pos: TableFunctionParamPos,

    triggered_on: TriggeredOn,
    triggered_by_id: UserId,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: FunctionRunStatus,
    data_location: DataLocation,
    storage_version: StorageVersion,

    // Only available when this version has data, even if it is not generated by this version
    // (has_data or !has_data and previous version has_data)
    with_data_table_data_version_id: Option<TableDataVersionId>,
    with_data_column_count: Option<ColumnCount>,
    with_data_row_count: Option<RowCount>,
    with_data_schema_hash: Option<SchemaHash>,

    collection: CollectionName,
    function: FunctionName,
    created_by: UserName,
}

#[td_type::Dto]
#[dto(list(on = TableDataVersionDBWithNames))]
#[td_type(builder(try_from = TableDataVersionDBWithNames))]
pub struct TableDataVersion {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    id: TableDataVersionId,
    #[dto(list(filter, filter_like, order_by))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    table_id: TableId,
    #[dto(list(filter, filter_like, order_by))]
    name: TableName,
    #[dto(list(filter, filter_like, order_by))]
    table_version_id: TableVersionId,
    #[dto(list(filter, filter_like, order_by))]
    function_version_id: FunctionVersionId,
    #[td_type(builder(field = "with_data_column_count"))]
    column_count: Option<ColumnCount>,
    #[td_type(builder(field = "with_data_row_count"))]
    row_count: Option<RowCount>,
    #[dto(list(filter))]
    #[td_type(builder(field = "with_data_schema_hash"))]
    schema_hash: Option<SchemaHash>,
    #[td_type(builder(field = "has_data"))]
    #[dto(list(filter, filter_like, order_by))]
    data_changed: DataChanged,
    #[dto(list(filter, filter_like, order_by))]
    execution_id: ExecutionId,
    #[dto(list(filter, filter_like, order_by))]
    transaction_id: TransactionId,
    #[dto(list(filter, filter_like, order_by))]
    function_run_id: FunctionRunId,
    #[dto(list(filter, filter_like, order_by))]
    function_param_pos: Option<TableFunctionParamPos>,

    #[td_type(builder(field = "triggered_on"))]
    #[dto(list(filter, filter_like, order_by))]
    created_at: TriggeredOn,
    #[dto(list(filter, filter_like, order_by))]
    triggered_by_id: UserId,
    #[dto(list(filter, filter_like, order_by))]
    started_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    ended_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    status: FunctionRunStatus,

    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like, order_by))]
    function: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    created_by: UserName,
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
    requirement_function_version_id: FunctionVersionId,
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
    requirement_function_version_id: FunctionVersionId,
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
    requirement_function_version_id: FunctionVersionId,
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
#[dao(sql_table = "workers")]
pub struct WorkerDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: WorkerId,
    #[td_type(extractor)]
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_version_id: FunctionVersionId,
    message_status: WorkerMessageStatus,
    #[builder(default)]
    started_on: Option<AtTime>,
    #[builder(default)]
    ended_on: Option<AtTime>,
    status: WorkerStatus,
}

#[td_type::Dao]
#[dao(sql_table = "workers__with_names")]
pub struct WorkerDBWithNames {
    id: WorkerId,
    collection_id: CollectionId,
    execution_id: ExecutionId,
    transaction_id: TransactionId,
    function_run_id: FunctionRunId,
    function_version_id: FunctionVersionId,
    message_status: WorkerMessageStatus,
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: WorkerStatus,

    collection: CollectionName,
    execution: Option<ExecutionName>,
    function: FunctionName,
}

#[td_type::Dto]
#[td_type(builder(try_from = WorkerDBWithNames))]
#[dto(list(on = WorkerDBWithNames))]
pub struct Worker {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    id: WorkerId,
    #[dto(list(filter, filter_like, order_by))]
    collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    execution_id: ExecutionId,
    #[dto(list(filter, filter_like, order_by))]
    transaction_id: TransactionId,
    #[dto(list(filter, filter_like, order_by))]
    function_run_id: FunctionRunId,
    #[dto(list(filter, filter_like, order_by))]
    function_version_id: FunctionVersionId,
    #[dto(list(filter, filter_like, order_by))]
    message_status: WorkerMessageStatus,
    #[dto(list(filter, filter_like, order_by))]
    started_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    ended_on: Option<AtTime>,
    #[dto(list(filter, filter_like, order_by))]
    status: WorkerStatus,

    #[dto(list(filter, filter_like, order_by))]
    collection: CollectionName,
    #[dto(list(filter, filter_like))]
    execution: Option<ExecutionName>,
    #[dto(list(filter, filter_like, order_by))]
    function: FunctionName,
}

#[td_type::typed_enum]
pub enum WorkerMessageStatus {
    #[typed_enum(rename = "L")]
    Locked,
    #[typed_enum(rename = "U")]
    Unlocked,
}

// Update Daos and Dlos

#[td_type::Dlo]
pub struct UpdateWorkerExecution {
    started_on: AtTime,
    ended_on: Option<AtTime>,
    status: WorkerCallbackStatus,
}

impl TryFrom<&CallbackRequest> for UpdateWorkerExecution {
    type Error = TdError;

    fn try_from(value: &CallbackRequest) -> Result<Self, Self::Error> {
        Ok(UpdateWorkerExecution::builder()
            .try_started_on(value.start().datetime_utc()?)?
            .ended_on(
                value
                    .end()
                    .map(|v| AtTime::try_from(v.datetime_utc()?))
                    .transpose()?,
            )
            .status(value.status().clone())
            .build()?)
    }
}

#[td_type::Dao]
#[dao(sql_table = "workers")]
#[td_type(builder(try_from = UpdateWorkerExecution))]
pub struct UpdateWorkerDB {
    #[dao(immutable)]
    started_on: Option<AtTime>,
    ended_on: Option<AtTime>,
    status: WorkerStatus,
}

#[td_type::Dao]
#[dao(sql_table = "function_runs")]
#[td_type(builder(try_from = UpdateWorkerExecution))]
pub struct UpdateFunctionRunDB {
    #[dao(immutable)]
    #[builder(default)]
    started_on: Option<AtTime>,
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

    pub async fn recover() -> Result<Self, TdError> {
        Ok(Self::builder()
            .status(FunctionRunStatus::ReScheduled)
            .build()?)
    }

    pub async fn cancel() -> Result<Self, TdError> {
        Ok(Self::builder()
            .ended_on(AtTime::now())
            .status(FunctionRunStatus::Canceled)
            .build()?)
    }
}

#[td_type::Dao]
#[dao(sql_table = "function_runs")]
pub struct CommitFunctionRunDB {
    status: FunctionRunStatus,
}

impl Default for CommitFunctionRunDB {
    fn default() -> Self {
        Self::builder()
            .status(FunctionRunStatus::Committed)
            .build()
            .unwrap()
    }
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions")]
pub struct UpdateTableDataVersionDB {
    #[dao(immutable)]
    #[builder(default)]
    has_data: Option<HasData>,
    #[dao(immutable)]
    #[builder(default)]
    column_count: Option<ColumnCount>,
    #[dao(immutable)]
    #[builder(default)]
    row_count: Option<RowCount>,
    #[dao(immutable)]
    #[builder(default)]
    schema_hash: Option<SchemaHash>,
}

#[td_type::Dao]
#[dao(sql_table = "workers")]
pub struct UpdateWorkerMessageStatusDB {
    message_status: WorkerMessageStatus,
}

impl UpdateWorkerMessageStatusDB {
    pub fn unlocked() -> Result<Self, TdError> {
        Ok(Self::builder()
            .message_status(WorkerMessageStatus::Unlocked)
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
    // plan info
    id: ExecutionId,
    name: Option<ExecutionName>,
    triggered_on: TriggeredOn,
    dot: Dot,
    // functions info
    all_functions: HashMap<FunctionVersionId, FunctionVersionResponse>,
    triggered_functions: HashSet<FunctionVersionId>,
    manual_trigger: FunctionVersionId,
    // transactions info
    transactions: HashMap<TransactionId, HashSet<FunctionVersionId>>,
    // tables info
    all_tables: HashMap<TableVersionId, TableVersionResponse>,
    created_tables: HashSet<TableVersionId>,
    system_tables: HashSet<TableVersionId>,
    user_tables: HashSet<TableVersionId>,
    // relations info
    #[builder(setter(custom))]
    relations: Vec<(
        FunctionVersionId,
        TableVersionId,
        GraphEdge<ResolvedVersionResponse>,
    )>,
}

impl ExecutionResponseBuilder {
    // Override the relations setter to ensure consistent ordering for reliable comparisons.
    pub fn relations<
        VALUE: Into<
            Vec<(
                FunctionVersionId,
                TableVersionId,
                GraphEdge<ResolvedVersionResponse>,
            )>,
        >,
    >(
        &mut self,
        value: VALUE,
    ) -> &mut Self {
        let mut relations = value.into();
        relations.sort_by(|a, b| {
            let edge_order = |edge: &GraphEdge<ResolvedVersionResponse>| -> u8 {
                match edge {
                    GraphEdge::Trigger { .. } => 0,
                    GraphEdge::Dependency { .. } => 1,
                    GraphEdge::Output { .. } => 2,
                }
            };
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then_with(|| edge_order(&a.2).cmp(&edge_order(&b.2)))
        });
        self.relations = Some(relations);
        self
    }
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
    table_version_id: TableVersionId,
    name: TableName,
}

// TODO: Value is a placeholder, we need to define the actual type
pub type CallbackRequest = ResponseMessagePayload<FunctionOutput>;

// Dlos

/// Represents a function version to perform graph resolution.
#[td_type::Dlo]
#[derive(Hash)]
#[td_type(builder(try_from = FunctionDBWithNames))]
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
#[td_type(builder(try_from = TableDBWithNames))]
pub struct TableVersionNode {
    collection_id: CollectionId,
    collection: CollectionName,
    function_version_id: FunctionVersionId,
    table_id: TableId,
    #[td_type(builder(field = "id"))]
    table_version_id: TableVersionId,
    name: TableName,
    system: System,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dlo]
#[derive(Hash, ToSchema)]
pub struct GraphDependency {
    dep_pos: DependencyPos,
    self_dependency: SelfDependency,
}

/// Adds contextual information to dependency graph edges.
#[td_type::Dlo]
#[derive(Hash, ToSchema)]
pub struct GraphOutput {
    output_pos: Option<TableFunctionParamPos>,
}

/// Graph versions, which will always hold the versions of the table, either input or output.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
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

/// Represents the versions of a table to be included in the response.
#[td_type::Dto]
pub struct ResolvedVersionResponse {
    inner: Vec<Option<TableDataVersionId>>,
    original: TableVersions,
}

impl From<&ResolvedVersion> for ResolvedVersionResponse {
    fn from(value: &ResolvedVersion) -> Self {
        let mut inner: Vec<_> = value
            .inner
            .iter()
            .map(|v| v.as_ref().map(|t| t.id))
            .collect();

        // Sort to ensure consistent ordering for reliable comparisons.
        inner.sort();

        Self {
            inner,
            original: value.original.clone(),
        }
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
    pub fn output(table: &TableDBWithNames) -> Result<(Self, Self), TdError> {
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
                    .system(table.system())
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
                TableVersionNode::builder()
                    .collection_id(table.collection_id())
                    .collection(table.collection())
                    .function_version_id(table.function_version_id())
                    .table_id(table.table_id())
                    .table_version_id(table.id())
                    .name(table.name())
                    .system(table.system())
                    .build()?,
            ),
            GraphNode::Function(
                FunctionVersionNode::builder()
                    .collection_id(function.collection_id())
                    .collection(function.collection())
                    .function_version_id(function.id())
                    .name(function.name())
                    .build()?,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::test_utils::execution::{
        FUNCTION_NAMES, TABLE_NAMES, dependency, table, trigger,
    };

    #[tokio::test]
    async fn test_graph_node_from_table() -> Result<(), TdError> {
        let table = table(&FUNCTION_NAMES[0], &TABLE_NAMES[0]).await;
        let (function_node, table_node) = GraphNode::output(&table)?;

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
            assert_eq!(table_node.table_version_id(), table.id());
            assert_eq!(table_node.name(), table.name());
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
            assert_eq!(node.collection_id(), dependency.table_collection_id());
            assert_eq!(node.collection(), dependency.table_collection());
            assert_eq!(node.function_version_id(), table.function_version_id());
            assert_eq!(node.table_version_id(), table.id());
            assert_eq!(node.name(), table.name());
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id(), dependency.collection_id());
            assert_eq!(node.collection(), dependency.collection());
            assert_eq!(node.function_version_id(), function.id());
            assert_eq!(node.name(), function.name());
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
            assert_eq!(node.collection_id(), trigger.trigger_by_collection_id());
            assert_eq!(node.collection(), trigger.trigger_by_collection());
            assert_eq!(node.function_version_id(), table.function_version_id());
            assert_eq!(node.table_version_id(), table.id());
            assert_eq!(node.name(), table.name());
        } else {
            panic!("Expected GraphNode::Table");
        }

        if let GraphNode::Function(node) = function_node {
            assert_eq!(node.collection_id(), trigger.collection_id());
            assert_eq!(node.collection(), trigger.collection());
            assert_eq!(node.function_version_id(), function.id());
            assert_eq!(node.name(), node.name());
        } else {
            panic!("Expected GraphNode::Function");
        }
        Ok(())
    }
}
