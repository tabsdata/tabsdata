//
// Copyright 2026 Tabsdata Inc.
//

//! DAO / DLO models for the logical-version layer: LFR / LTDV insert-once
//! identity rows and the LFR → LTDV dependency-binding rows.
//!
//! Plan-build inserts the identity rows alone; their attempts are SCD2
//! versions on `physical_function_runs` / `physical_table_data_versions`,
//! the newest being the current one.

use crate::dxo::worker::WorkerExecutionUpdate;
use crate::types::basic::{
    AtTime, ByteCount, CollectionId, CollectionName, CollectionWhenDeleted, ColumnCount,
    DataChanged, DependencyPos, Ended, EndedDay, EndedHour, EndedMonth, EndedWeek, EpochMs,
    ExecutionId, ExecutionName, FunctionId, FunctionName, FunctionVersionId, GroupId, GroupName,
    HasData, InputIdx, LogicalFunctionRunId, LogicalFunctionRunInputId, LogicalFunctionRunStatus,
    LogicalTableDataVersionId, Patch, PhysicalFunctionRunId, PhysicalFunctionRunStatus,
    PhysicalTableDataVersionId, Private, ProjectId, ProjectName, RowCount, SchemaChanged,
    SchemaDataChanged, SchemaHash, Started, StorageVersion, TableFunctionParamPos, TableId,
    TableName, TableVersionCount, TableVersionId, TransactionId, Trigger, TriggeredOn, UserId,
    UserName, VersionPos, WithData,
};
use ta_objects::FromDao;
use td_error::TdError;

/// `previous_function_run_id` is the attempt the replay supersedes.
#[ta_objects::Dto]
pub struct LogicalFunctionRunReplayResponse {
    pub logical_function_run_id: LogicalFunctionRunId,
    pub previous_function_run_id: PhysicalFunctionRunId,
    pub new_function_run_id: PhysicalFunctionRunId,
    pub function_version_id: FunctionVersionId,
}

#[ta_objects::Dao]
#[dao(table = "logical_function_runs", ops(insert), project_scoped)]
pub struct LogicalFunctionRunInsertDB {
    #[dao(pk)]
    pub id: LogicalFunctionRunId,
    pub collection_id: CollectionId,
    // (execution_id, function_id) is an LFR's natural key — one LFR per
    // logical run within an execution.
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    // The *function version* an attempt runs lives on each concrete FR row —
    // replays can attach a fresh FR with a different function_version_id
    // under the same LFR identity.
    pub function_id: FunctionId,
    // Plan-time fvid this LFR was minted against; set once at plan-build,
    // never updated. Topology reference for mint / bind: a target fvid's
    // head must equal `head_of(function_version_id)`.
    pub function_version_id: FunctionVersionId,
    pub trigger: Trigger,
    #[dao(order_by)]
    pub triggered_on: TriggeredOn,
    pub triggered_by_id: UserId,
    // Canonical execution status. Sticky once Committed: replay attempts
    // still mint a new FR, but never demote a Committed LFR.
    pub status: LogicalFunctionRunStatus,
    // Group snapshot copied from collections.group_id at LFR mint; immutable
    // so a later group reassignment doesn't shift historical runs. No
    // default: the group is project-scoped and must be stamped explicitly
    // from the run's collection.
    pub group_id: GroupId,
}

/// The row as the database holds it: `project_id` is the `project_isolation`
/// policy's, defaulted from the connection the insert ran on. The
/// cross-project scheduler reads it back.
#[ta_objects::Dao]
#[dao(
    table = "logical_function_runs",
    ops(select),
    unique(ExecutionId, FunctionId),
    project_scoped
)]
pub struct LogicalFunctionRunDB {
    #[dao(flatten)]
    pub base: LogicalFunctionRunInsertDB,
    pub project_id: ProjectId,
}

// LFR identity row joined with the current PFR's per-attempt fields;
// PFR-derived fields are NULL when there is no current PFR.
// `status` is the canonical LFR-level (sticky-Committed) status — not the
// PFR's per-attempt status.
#[ta_objects::Dao]
#[dao(view = "function_runs__with_names", project_scoped)]
pub struct FunctionRunDBWithNames {
    #[dao(flatten)]
    pub base: LogicalFunctionRunDB,
    pub name: FunctionName,
    pub collection: CollectionName,
    // True when the owning collection is soft-deleted; `collection` still
    // carries the real pre-deletion name (resolved view).
    pub collection_deleted: CollectionWhenDeleted,
    pub execution: Option<ExecutionName>,
    pub triggered_by: UserName,
    pub group: Option<GroupName>,
    pub project: Option<ProjectName>,
    // This LFR's current PFR; NULL while none was minted, or after a void.
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    // Per-attempt audit status from the current PFR; the canonical
    // sticky-Committed `status` lives on the LFR side.
    pub physical_status: Option<PhysicalFunctionRunStatus>,
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
    // Bucket floors / aggregated metrics from the current PFR. NULL until the
    // attempt terminates; metric_* additionally NULL for non-output statuses.
    pub ended_on_hour_ms: Option<EndedHour<EpochMs>>,
    pub ended_on_day_ms: Option<EndedDay<EpochMs>>,
    pub ended_on_week_ms: Option<EndedWeek<EpochMs>>,
    pub ended_on_month_ms: Option<EndedMonth<EpochMs>>,
    pub metric_table_versions: Option<TableVersionCount>,
    pub metric_rows: Option<RowCount>,
    pub metric_data_bytes: Option<ByteCount>,
}

/// One row per `(execution_id, function_id)`: the LFR identity (canonical
/// sticky-Committed status) merged with the current PFR's per-attempt
/// fields. Per-attempt drill-downs (workers, logs) go through
/// `physical_function_run_id`.
#[ta_objects::Dto]
#[dto(list(on = FunctionRunDBWithNames))]
pub struct FunctionRun {
    #[dto(list(pagination_by = "+", filter))]
    pub id: LogicalFunctionRunId,
    #[dto(list(filter))]
    pub project_id: ProjectId,
    #[dto(list(filter, order_by))]
    pub collection_id: CollectionId,
    #[dto(list(filter, filter_like, order_by))]
    pub execution_id: ExecutionId,
    #[dto(list(filter, filter_like, order_by))]
    pub transaction_id: TransactionId,
    #[dto(list(filter, order_by))]
    pub function_id: FunctionId,
    #[dto(list(filter, order_by))]
    pub function_version_id: FunctionVersionId,
    pub trigger: Trigger,
    #[dto(list(filter, order_by))]
    pub triggered_on: TriggeredOn,
    #[dto(list(filter, order_by))]
    pub triggered_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub status: LogicalFunctionRunStatus,

    #[dto(list(filter, filter_like, order_by))]
    pub name: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    pub collection_deleted: CollectionWhenDeleted,
    #[dto(list(filter, filter_like))]
    pub execution: Option<ExecutionName>,
    #[dto(list(filter, filter_like, order_by))]
    pub project: Option<ProjectName>,
    pub triggered_by: UserName,
    // Per-attempt fields from the current PFR;
    // `physical_function_run_id` is NULL while there is none.
    #[dto(list(filter, order_by))]
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    #[dto(list(filter, order_by))]
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter, order_by))]
    pub ended_on: Option<Ended<AtTime>>,
    // PFR-side per-attempt status; committed-only filters use the canonical
    // LFR `status` above.
    #[dto(list(filter, order_by))]
    pub physical_status: Option<PhysicalFunctionRunStatus>,
    #[dto(list(filter, order_by))]
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
}

impl FromDao for FunctionRun {
    type Dao = FunctionRunDBWithNames;

    fn from_dao(dao: FunctionRunDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            project_id: dao.base.project_id,
            collection_id: dao.base.base.collection_id,
            execution_id: dao.base.base.execution_id,
            transaction_id: dao.base.base.transaction_id,
            function_id: dao.base.base.function_id,
            function_version_id: dao.base.base.function_version_id,
            trigger: dao.base.base.trigger,
            triggered_on: dao.base.base.triggered_on,
            triggered_by_id: dao.base.base.triggered_by_id,
            status: dao.base.base.status,
            name: dao.name,
            collection: dao.collection,
            collection_deleted: dao.collection_deleted,
            execution: dao.execution,
            project: dao.project,
            triggered_by: dao.triggered_by,
            physical_function_run_id: dao.physical_function_run_id,
            started_on: dao.started_on,
            ended_on: dao.ended_on,
            physical_status: dao.physical_status,
            function_version_patch_id: dao.function_version_patch_id,
        })
    }
}

// Per-attempt wall-clock fields (started_on, ended_on) live on
// PhysicalFunctionRunDB, not here.
#[ta_objects::Dao]
#[dao(
    table = "logical_function_runs",
    ops(update(LogicalFunctionRunDB)),
    project_scoped
)]
pub struct LogicalFunctionRunStatusUpdateDB {
    pub status: LogicalFunctionRunStatus,
}

impl LogicalFunctionRunStatusUpdateDB {
    pub fn scheduled() -> Self {
        Self {
            status: LogicalFunctionRunStatus::Scheduled,
        }
    }

    pub fn requested() -> Self {
        Self {
            status: LogicalFunctionRunStatus::Requested,
        }
    }

    pub fn recover() -> Self {
        Self {
            status: LogicalFunctionRunStatus::ReScheduled,
        }
    }

    pub fn failed() -> Self {
        Self {
            status: LogicalFunctionRunStatus::Failed,
        }
    }

    pub fn cancel() -> Self {
        Self {
            status: LogicalFunctionRunStatus::Canceled,
        }
    }

    pub fn committed() -> Self {
        Self {
            status: LogicalFunctionRunStatus::Committed,
        }
    }
}

#[ta_objects::Dao]
#[dao(
    table = "logical_table_data_versions",
    ops(insert),
    timeline,
    project_scoped
)]
pub struct LogicalTableDataVersionInsertDB {
    #[dao(pk)]
    pub id: LogicalTableDataVersionId,
    pub collection_id: CollectionId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    #[dao(partition_by)]
    pub table_id: TableId,
    // NULL when the LTDV is a lazy-mint over a V1-born concrete TDV
    // whose producer pre-dates the logical layer.
    pub producer_logical_function_run_id: Option<LogicalFunctionRunId>,
    pub function_param_pos: Option<TableFunctionParamPos>,
    // Table privateness snapshot, copied from the output table at mint.
    // Lets metrics exclude private tables without joining the versioned
    // `tables` table.
    pub private: Private,
    // The *table version* (schema) an attempt produces lives on each
    // concrete TDV row — replays can attach a concrete TDV with a different
    // table_version_id under the same LTDV identity.
    #[dao(order_by, timeline(version_by))]
    pub triggered_on: TriggeredOn,
    pub triggered_by_id: UserId,
    // Equal by construction to the producing LFR's group_id (both copied
    // off the same FunctionNode at plan-build). No default: the group is
    // project-scoped and must be stamped explicitly.
    pub group_id: GroupId,
}

/// The row as the database holds it: `project_id` is the `project_isolation`
/// policy's, defaulted from the connection the insert ran on. The
/// cross-project dispatch tick reads it back.
#[ta_objects::Dao]
#[dao(
    table = "logical_table_data_versions",
    ops(select),
    timeline,
    project_scoped
)]
pub struct LogicalTableDataVersionDB {
    #[dao(flatten)]
    pub base: LogicalTableDataVersionInsertDB,
    pub project_id: ProjectId,
}

// LTDV identity rows whose producer LFR is not Canceled/Yanked — the
// version resolver's "active" timeline.
#[ta_objects::Dao]
#[dao(view = "logical_table_data_versions__active", timeline, project_scoped)]
pub struct ActiveLogicalTableDataVersionDB {
    #[dao(flatten)]
    pub base: LogicalTableDataVersionDB,
}

// LTDV identity row joined with the current PTDV's per-attempt fields (and
// its producer PFR's); PTDV/PFR-derived fields are NULL when there is no
// current PTDV.
#[ta_objects::Dao]
#[dao(view = "table_data_versions__with_names", timeline, project_scoped)]
pub struct TableDataVersionDBWithNames {
    #[dao(flatten)]
    pub base: LogicalTableDataVersionDB,
    pub collection: CollectionName,
    pub execution: Option<ExecutionName>,
    pub triggered_by: UserName,
    pub group: Option<GroupName>,
    // This LTDV's current PTDV; NULL while none was minted, or after a void.
    pub physical_table_data_version_id: Option<PhysicalTableDataVersionId>,
    pub name: Option<TableName>,
    pub table_version_id: Option<TableVersionId>,
    pub function_version_id: Option<FunctionVersionId>,
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
    pub has_data: Option<HasData>,
    pub schema_changed: Option<SchemaChanged>,
    pub column_count: Option<ColumnCount>,
    pub row_count: Option<RowCount>,
    pub data_bytes: Option<ByteCount>,
    pub schema_hash: Option<SchemaHash>,
    pub with_data_table_data_version_id: Option<WithData<PhysicalTableDataVersionId>>,
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
    // Canonical (sticky-Committed) status from the LTDV's producer LFR;
    // use this for committed-only filters.
    pub status: Option<LogicalFunctionRunStatus>,
    // PFR-side per-attempt status (Running, Done, …).
    pub physical_status: Option<PhysicalFunctionRunStatus>,
    pub function: Option<FunctionName>,
    pub storage_version: Option<StorageVersion>,
}

/// One row per LTDV identity: the logical identity merged with the
/// current PTDV's (and its producer PFR's) per-attempt fields.
/// Per-attempt drill (download, schema, sample) goes through
/// `physical_table_data_version_id`.
#[ta_objects::Dto]
#[dto(list(on = TableDataVersionDBWithNames))]
pub struct TableDataVersion {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    pub id: LogicalTableDataVersionId,
    #[dto(list(filter, order_by))]
    pub collection_id: CollectionId,
    #[dto(list(filter, order_by))]
    pub table_id: TableId,
    #[dto(list(filter, filter_like, order_by))]
    pub execution_id: ExecutionId,
    #[dto(list(filter, filter_like, order_by))]
    pub transaction_id: TransactionId,
    #[dto(list(filter, order_by))]
    pub producer_logical_function_run_id: Option<LogicalFunctionRunId>,
    #[dto(list(filter, order_by))]
    pub function_param_pos: Option<TableFunctionParamPos>,
    #[dto(list(filter, order_by))]
    pub created_at: TriggeredOn,
    #[dto(list(filter, order_by))]
    pub triggered_by_id: UserId,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    #[dto(list(filter, filter_like))]
    pub execution: Option<ExecutionName>,
    pub created_by: UserName,

    // Current PTDV identity + derived per-attempt audit; NULL while there
    // is none.
    #[dto(list(filter, order_by))]
    pub physical_table_data_version_id: Option<PhysicalTableDataVersionId>,
    #[dto(list(filter, filter_like, order_by))]
    pub name: Option<TableName>,
    #[dto(list(filter, order_by))]
    pub table_version_id: Option<TableVersionId>,
    #[dto(list(filter, filter_like, order_by))]
    pub function_version_id: Option<FunctionVersionId>,
    #[dto(list(filter, order_by))]
    pub function_version_patch_id: Option<Patch<FunctionVersionId>>,
    #[dto(list(filter, col = HasData))]
    pub data_changed: DataChanged,
    #[dto(list(filter, col = SchemaChanged))]
    pub schema_data_changed: SchemaDataChanged,
    pub column_count: Option<ColumnCount>,
    pub row_count: Option<RowCount>,
    pub data_bytes: Option<ByteCount>,
    #[dto(list(filter))]
    pub schema_hash: Option<SchemaHash>,
    pub with_data_table_data_version_id: Option<WithData<PhysicalTableDataVersionId>>,
    #[dto(list(filter, order_by))]
    pub physical_function_run_id: Option<PhysicalFunctionRunId>,
    #[dto(list(filter, order_by))]
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter, order_by))]
    pub ended_on: Option<Ended<AtTime>>,
    // Canonical LFR-level status (sticky-Committed); filter on this for
    // committed-only.
    #[dto(list(filter, order_by))]
    pub status: Option<LogicalFunctionRunStatus>,
    // PFR-side per-attempt status (Running, Done, …).
    #[dto(list(filter, order_by))]
    pub physical_status: Option<PhysicalFunctionRunStatus>,
    #[dto(list(filter, filter_like, order_by))]
    pub function: Option<FunctionName>,
    pub storage_version: Option<StorageVersion>,
}

impl FromDao for TableDataVersion {
    type Dao = TableDataVersionDBWithNames;

    fn from_dao(dao: TableDataVersionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            collection_id: dao.base.base.collection_id,
            table_id: dao.base.base.table_id,
            execution_id: dao.base.base.execution_id,
            transaction_id: dao.base.base.transaction_id,
            producer_logical_function_run_id: dao.base.base.producer_logical_function_run_id,
            function_param_pos: dao.base.base.function_param_pos,
            created_at: dao.base.base.triggered_on,
            triggered_by_id: dao.base.base.triggered_by_id,
            collection: dao.collection,
            execution: dao.execution,
            created_by: dao.triggered_by,
            physical_table_data_version_id: dao.physical_table_data_version_id,
            name: dao.name,
            table_version_id: dao.table_version_id,
            function_version_id: dao.function_version_id,
            function_version_patch_id: dao.function_version_patch_id,
            data_changed: dao.has_data.into(),
            schema_data_changed: dao.schema_changed.into(),
            column_count: dao.column_count,
            row_count: dao.row_count,
            data_bytes: dao.data_bytes,
            schema_hash: dao.schema_hash,
            with_data_table_data_version_id: dao.with_data_table_data_version_id,
            physical_function_run_id: dao.physical_function_run_id,
            started_on: dao.started_on,
            ended_on: dao.ended_on,
            status: dao.status,
            physical_status: dao.physical_status,
            function: dao.function,
            storage_version: dao.storage_version,
        })
    }
}

#[ta_objects::Dao]
#[dao(table = "logical_function_run_inputs", ops(insert), project_scoped)]
pub struct LogicalFunctionRunInputInsertDB {
    #[dao(pk)]
    pub id: LogicalFunctionRunInputId,
    pub collection_id: CollectionId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub logical_function_run_id: LogicalFunctionRunId,
    // NULL when the dep edge resolved to no LTDV at plan-build (e.g. self-dep
    // on first execution); the row is still emitted so the worker yaml
    // renders the input slot from `DependencyDB` at dispatch.
    pub requirement_logical_table_data_version_id: Option<LogicalTableDataVersionId>,
    // NULL on trigger-edge rows.
    pub requirement_input_idx: Option<InputIdx>,
    pub requirement_dependency_pos: Option<DependencyPos>,
    pub requirement_version_pos: VersionPos,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(table = "logical_function_run_inputs", ops(select), project_scoped)]
pub struct LogicalFunctionRunInputDB {
    #[dao(flatten)]
    pub base: LogicalFunctionRunInputInsertDB,
    pub project_id: ProjectId,
}
impl TryFrom<&WorkerExecutionUpdate> for LogicalFunctionRunStatusUpdateDB {
    type Error = TdError;
    fn try_from(src: &WorkerExecutionUpdate) -> Result<Self, Self::Error> {
        Ok(LogicalFunctionRunStatusUpdateDB {
            status: src.status.clone().try_into()?,
        })
    }
}

#[ta_objects::UrlParam]
pub struct LogicalFunctionRunParam {
    pub logical_function_run: LogicalFunctionRunId,
}
