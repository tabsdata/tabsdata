//
// Copyright 2026 Tabsdata Inc.
//

//! The `/metrics` facts: one row per event of an ended attempt (`A`), per
//! version written (`O`) or read (`I`), plus an egress copy (`E`) of each
//! read when the run writes nothing.

use crate::types::basic::{
    AtTime, ByteCount, CollectionId, CurrentAttempt, EdgeFirst, EdgeFirstGroup, EdgeKind, EndedDay,
    EndedHour, EndedMonth, EndedWeek, EpochMs, ExecutionId, FactSide, FunctionId, GroupId, HasData,
    LogicalFunctionRunId, LogicalFunctionRunStatus, LogicalTableDataVersionId,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, Private, ProjectId, ReplayOrdinal, RowCount,
    RunHasInputs, RunHasOutputs, TableId, TransactionId,
};

#[ta_objects::Dao]
#[dao(table = "metrics_facts", ops(insert), project_scoped)]
pub struct MetricsFactInsertDB {
    pub ts: AtTime,
    pub ts_hour_ms: EndedHour<EpochMs>,
    pub ts_day_ms: EndedDay<EpochMs>,
    pub ts_week_ms: EndedWeek<EpochMs>,
    pub ts_month_ms: EndedMonth<EpochMs>,
    pub from_group_id: Option<GroupId>,
    pub from_collection_id: Option<CollectionId>,
    pub group_id: GroupId,
    pub collection_id: CollectionId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub function_id: FunctionId,
    pub function_run_id: LogicalFunctionRunId,
    pub attempt_id: PhysicalFunctionRunId,
    pub attempt_outcome: PhysicalFunctionRunStatus,
    /// Follows the run on current `Done` attempts only.
    pub run_status: LogicalFunctionRunStatus,
    pub replay: ReplayOrdinal,
    pub current: CurrentAttempt,
    pub run_has_inputs: RunHasInputs,
    pub run_has_outputs: RunHasOutputs,
    pub side: FactSide,
    pub table_id: Option<TableId>,
    pub table_version_id: Option<LogicalTableDataVersionId>,
    pub private: Private,
    pub has_data: HasData,
    pub row_count: RowCount,
    pub data_bytes: ByteCount,
    /// On a written row: when an ended attempt first bound the version as an
    /// input. A version's, not the attempt's: a re-materialization inherits it.
    pub first_consumed_on: Option<AtTime>,
    pub edge_kind: Option<EdgeKind>,
    pub edge_first: EdgeFirst,
    pub edge_first_group: EdgeFirstGroup,
}

/// A fact has no key of its own: it is an event, addressed only in bulk.
#[ta_objects::Dao]
#[dao(table = "metrics_facts", ops(select), project_scoped)]
pub struct MetricsFactDB {
    pub ts: AtTime,
    pub ts_hour_ms: EndedHour<EpochMs>,
    pub ts_day_ms: EndedDay<EpochMs>,
    pub ts_week_ms: EndedWeek<EpochMs>,
    pub ts_month_ms: EndedMonth<EpochMs>,
    pub project_id: ProjectId,
    pub from_group_id: Option<GroupId>,
    pub from_collection_id: Option<CollectionId>,
    pub group_id: GroupId,
    pub collection_id: CollectionId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub function_id: FunctionId,
    pub function_run_id: LogicalFunctionRunId,
    pub attempt_id: PhysicalFunctionRunId,
    pub attempt_outcome: PhysicalFunctionRunStatus,
    pub run_status: LogicalFunctionRunStatus,
    pub replay: ReplayOrdinal,
    pub current: CurrentAttempt,
    pub run_has_inputs: RunHasInputs,
    pub run_has_outputs: RunHasOutputs,
    pub side: FactSide,
    pub table_id: Option<TableId>,
    pub table_version_id: Option<LogicalTableDataVersionId>,
    pub private: Private,
    pub has_data: HasData,
    pub row_count: RowCount,
    pub data_bytes: ByteCount,
    pub first_consumed_on: Option<AtTime>,
    pub edge_kind: Option<EdgeKind>,
    pub edge_first: EdgeFirst,
    pub edge_first_group: EdgeFirstGroup,
}

/// One `(side, edge kind, endpoints, project, status, table)` of a window.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow)]
pub struct MetricsTableRow {
    pub side: FactSide,
    pub edge_kind: Option<EdgeKind>,
    pub from_collection_id: Option<CollectionId>,
    pub from_group_id: Option<GroupId>,
    pub collection_id: CollectionId,
    pub group_id: GroupId,
    pub project_id: ProjectId,
    pub run_status: LogicalFunctionRunStatus,
    pub table_id: Option<TableId>,
    pub fn_runs: i64,
    pub versions: i64,
    pub versions_with_data: i64,
    pub versions_unconsumed: i64,
    pub row_count: i64,
    pub data_bytes: i64,
}
