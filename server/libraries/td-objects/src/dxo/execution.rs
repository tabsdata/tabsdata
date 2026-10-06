//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::function::{Function, FunctionDBWithNames};
use crate::dxo::graph::{FunctionNode, GraphEdge, TableNode};
use crate::dxo::logical::TableDataVersion;
use crate::types::basic::{
    AllowDisabled, AtTime, CollectionId, CollectionName, CollectionWhenDeleted, Dot, Enabled,
    Ended, ExecutionId, ExecutionName, ExecutionStatus, ExternalUid, FunctionId, FunctionName,
    FunctionOnly, FunctionVersionId, InputIdx, LogicalFunctionRunStatus, LogicalTableDataVersionId,
    OverrideDisabledTrigger, ProjectId, ProjectName, SkipIfRunning, Started, StatusCount, System,
    TableName, TableVersionId, TransactionId, TriggerType, TriggeredOn, UserId, UserLabel,
};
use crate::types::composed::TableVersions;
use crate::types::json::Json;
use crate::types::recursion::RecursionOptions;
use crate::types::status_count::FunctionRunStatusCount;
use std::collections::{HashMap, HashSet};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dto]
pub struct ExecutionRequest {
    pub name: Option<ExecutionName>,
    #[serde(default)]
    pub function_only: FunctionOnly,
    #[serde(default)]
    pub override_disabled_trigger: OverrideDisabledTrigger,
    #[serde(default)]
    pub skip_if_running: SkipIfRunning,
    #[serde(default)]
    pub external_uid: Option<ExternalUid>,
}

impl From<&InternalExecutionRequest> for ExecutionRequest {
    fn from(value: &InternalExecutionRequest) -> Self {
        ExecutionRequest {
            name: value.name.clone(),
            function_only: value.function_only,
            override_disabled_trigger: OverrideDisabledTrigger::FALSE,
            skip_if_running: value.skip_if_running,
            external_uid: value.external_uid.clone(),
        }
    }
}

#[ta_objects::Dto]
pub struct InternalExecutionRequest {
    pub name: Option<ExecutionName>,
    #[serde(default)]
    pub function_only: FunctionOnly,
    #[serde(default)]
    pub skip_if_running: SkipIfRunning,
    #[serde(default)]
    pub external_uid: Option<ExternalUid>,
    pub trigger_type: TriggerType,
}

#[ta_objects::Dao]
#[dao(table = "executions", ops(insert), project_scoped)]
pub struct ExecutionInsertDB {
    #[dao(pk)]
    pub id: ExecutionId,
    pub name: Option<ExecutionName>,
    pub collection_id: CollectionId,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    #[dao(order_by)]
    pub triggered_on: TriggeredOn,
    pub triggered_by_id: UserId,
    pub trigger_type: TriggerType,

    /// Caller-supplied idempotency key. Unique per `function_id` when set;
    /// NULL allows multiple executions for the same function.
    pub external_uid: Option<ExternalUid>,

    pub recursion_options: Option<Json<RecursionOptions>>,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(
    table = "executions",
    ops(select),
    unique(FunctionId, ExternalUid),
    project_scoped
)]
pub struct ExecutionDB {
    #[dao(flatten)]
    pub base: ExecutionInsertDB,
    pub project_id: ProjectId,

    /// Rolled up from the execution's logical runs and their attempts by the
    /// status roll-up door whenever a run or attempt changes; never written
    /// from here.
    pub status: ExecutionStatus,
    pub started_on: Option<Started<AtTime>>,
    pub ended_on: Option<Ended<AtTime>>,
}

#[ta_objects::Dao]
#[dao(view = "executions__with_names", project_scoped)]
pub struct ExecutionDBWithNames {
    #[dao(flatten)]
    pub base: ExecutionDB,
    pub collection: CollectionName,
    // True when the owning collection is soft-deleted; `collection` still
    // carries the real pre-deletion name (resolved view).
    pub collection_deleted: CollectionWhenDeleted,
    pub function: FunctionName,
    pub triggered_by: UserLabel,
    pub project: Option<ProjectName>,
    pub function_run_status_count: Json<HashMap<LogicalFunctionRunStatus, StatusCount>>,
}

#[ta_objects::Dto]
#[dto(list(on = ExecutionDBWithNames))]
pub struct Execution {
    #[dto(list(pagination_by = "+", filter, filter_like))]
    pub id: ExecutionId,
    #[dto(list(filter, filter_like))]
    pub name: Option<ExecutionName>,
    #[dto(list(filter, order_by))]
    pub collection_id: CollectionId,
    pub function_id: FunctionId,
    #[dto(list(filter))]
    pub function_version_id: FunctionVersionId,
    #[dto(list(filter, order_by))]
    pub triggered_on: TriggeredOn,
    pub triggered_by_id: UserId,
    #[dto(list(filter, order_by))]
    pub trigger_type: TriggerType,
    pub external_uid: Option<ExternalUid>,

    #[dto(list(filter, filter_like, order_by))]
    pub collection: CollectionName,
    pub collection_deleted: CollectionWhenDeleted,
    #[dto(list(filter, filter_like, order_by))]
    pub function: FunctionName,
    #[dto(list(filter, filter_like, order_by))]
    pub triggered_by: UserLabel,
    #[dto(list(filter, filter_like, order_by))]
    pub project: Option<ProjectName>,
    #[dto(list(filter))]
    pub project_id: ProjectId,

    #[dto(list(filter, order_by))]
    pub started_on: Option<Started<AtTime>>,
    #[dto(list(filter, order_by))]
    pub ended_on: Option<Ended<AtTime>>,
    #[dto(list(filter, order_by))]
    pub status: ExecutionStatus,
    pub function_run_status_count: FunctionRunStatusCount,
}

impl FromDao for Execution {
    type Dao = ExecutionDBWithNames;

    fn from_dao(dao: ExecutionDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            name: dao.base.base.name,
            collection_id: dao.base.base.collection_id,
            function_id: dao.base.base.function_id,
            function_version_id: dao.base.base.function_version_id,
            triggered_on: dao.base.base.triggered_on,
            triggered_by_id: dao.base.base.triggered_by_id,
            trigger_type: dao.base.base.trigger_type,
            external_uid: dao.base.base.external_uid,
            collection: dao.collection,
            collection_deleted: dao.collection_deleted,
            function: dao.function,
            triggered_by: dao.triggered_by,
            project: dao.project,
            project_id: dao.base.project_id,
            started_on: dao.base.started_on,
            ended_on: dao.base.ended_on,
            status: dao.base.status,
            function_run_status_count: dao.function_run_status_count.into(),
        })
    }
}

#[ta_objects::Dto]
pub struct ExecutionDetails {
    pub execution: Execution,
    pub functions: Vec<Function>,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct FunctionNodeResponse {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub name: FunctionName,
    pub enabled: Enabled,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Hash)]
pub struct TableNodeResponse {
    pub collection_id: CollectionId,
    pub collection: CollectionName,
    pub function_version_id: FunctionVersionId,
    pub table_version_id: TableVersionId,
    pub name: TableName,
    pub system: System,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResolvedVersionResponse {
    pub inner: Vec<Option<LogicalTableDataVersionId>>,
    /// Per-slot provenance idx, paired 1:1 with `inner` on dependency
    /// edges (empty on trigger/output edges). Slots without a data
    /// version carry `InputIdx::NO_DATA`. Sourced from the persisted
    /// `logical_function_run_inputs` rows, so reads always reproduce
    /// the idxs the plan actually ran with.
    pub input_idxs: Vec<InputIdx>,
    pub original: TableVersions,
}

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct ExecutionResponse {
    pub id: ExecutionId,
    pub name: Option<ExecutionName>,
    pub triggered_on: TriggeredOn,
    pub trigger_type: TriggerType,
    pub dot: Dot,
    pub all_functions: HashMap<FunctionVersionId, FunctionNodeResponse>,
    pub triggered_functions: HashSet<FunctionVersionId>,
    pub manual_trigger: FunctionVersionId,
    pub transactions: HashMap<TransactionId, HashSet<FunctionVersionId>>,
    pub all_tables: HashMap<TableVersionId, TableNodeResponse>,
    pub created_tables: HashSet<TableVersionId>,
    pub system_tables: HashSet<TableVersionId>,
    pub user_tables: HashSet<TableVersionId>,
    pub relations: Vec<(
        FunctionVersionId,
        TableVersionId,
        GraphEdge<ResolvedVersionResponse>,
    )>,
    pub relations_info: HashMap<LogicalTableDataVersionId, TableDataVersion>,
}
impl From<&FunctionNode> for FunctionNodeResponse {
    fn from(src: &FunctionNode) -> Self {
        FunctionNodeResponse {
            collection_id: src.collection_id,
            collection: src.collection.clone(),
            function_version_id: src.function_version_id.clone(),
            name: src.name.clone(),
            enabled: src.enabled,
        }
    }
}

impl From<&TableNode> for TableNodeResponse {
    fn from(src: &TableNode) -> Self {
        TableNodeResponse {
            collection_id: src.collection_id,
            collection: src.collection.clone(),
            function_version_id: src.function_version_id.clone(),
            table_version_id: src.table_version_id,
            name: src.name.clone(),
            system: src.system,
        }
    }
}
impl From<&FunctionDBWithNames> for FunctionNode {
    fn from(src: &FunctionDBWithNames) -> Self {
        FunctionNode {
            collection_id: src.base.base.collection_id,
            collection: src.collection.clone(),
            function_id: src.base.base.function_id,
            function_version_id: src.base.base.id.clone(),
            name: src.base.base.name.clone(),
            enabled: src.base.base.enabled,
            group_id: src.group_id,
        }
    }
}

#[ta_objects::UrlParam]
pub struct ExecutionParam {
    pub execution: ExecutionId,
}

/// `?with_disabled=` — include executions of disabled functions in the listing.
#[ta_objects::QueryParam]
pub struct AllowDisabledParam {
    #[serde(default)]
    pub with_disabled: AllowDisabled,
}
