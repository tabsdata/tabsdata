//
// Copyright 2026 Tabsdata Inc.
//

//! The fact rows of one ended attempt. The v9→v10 rung derives the same rows
//! in SQL; the two agreeing is the DAO gate.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use td_common::bucket::BucketFloors;
use td_error::TdError;
use td_objects::dxo::metrics_facts::MetricsFactInsertDB;
use td_objects::types::basic::{
    AtTime, ByteCount, CollectionId, CurrentAttempt, EdgeFirst, EdgeFirstGroup, EdgeKind, EpochMs,
    ExecutionId, FactSide, FunctionId, GroupId, HasData, LogicalFunctionRunId,
    LogicalFunctionRunStatus, LogicalTableDataVersionId, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus, Private, ReplayOrdinal, RowCount, RunHasInputs, RunHasOutputs,
    TableId, TransactionId,
};

pub struct Version {
    pub collection_id: CollectionId,
    pub group_id: GroupId,
    pub table_id: TableId,
    pub table_version_id: LogicalTableDataVersionId,
    pub private: bool,
    pub has_data: bool,
    pub row_count: i64,
    pub data_bytes: i64,
}

pub struct EndedAttempt {
    pub collection_id: CollectionId,
    pub group_id: GroupId,
    pub execution_id: ExecutionId,
    pub transaction_id: TransactionId,
    pub function_id: FunctionId,
    pub function_run_id: LogicalFunctionRunId,
    pub attempt_id: PhysicalFunctionRunId,
    pub attempt_outcome: PhysicalFunctionRunStatus,
    pub run_status: LogicalFunctionRunStatus,
    pub replay: i64,
    pub current: bool,
    pub run_has_inputs: bool,
    pub ended_on: AtTime,
    pub outs: Vec<Version>,
    pub ins: Vec<Version>,
}

/// A failed attempt keeps its own outcome; a `Done` one follows its run.
pub fn stamped_status(
    outcome: PhysicalFunctionRunStatus,
    run_status: LogicalFunctionRunStatus,
) -> LogicalFunctionRunStatus {
    match outcome {
        PhysicalFunctionRunStatus::Error => LogicalFunctionRunStatus::Error,
        PhysicalFunctionRunStatus::Failed => LogicalFunctionRunStatus::Failed,
        _ => run_status,
    }
}

struct Row<'a> {
    attempt: &'a EndedAttempt,
    status: LogicalFunctionRunStatus,
    floors: BucketFloors,
    run_has_outputs: bool,
}

impl Row<'_> {
    fn build(
        &self,
        side: FactSide,
        from: Option<&Version>,
        version: Option<&Version>,
        edge_kind: Option<EdgeKind>,
        edge_first: bool,
        edge_first_group: bool,
    ) -> Result<MetricsFactInsertDB, TdError> {
        let a = self.attempt;
        Ok(MetricsFactInsertDB {
            ts: a.ended_on,
            ts_hour_ms: EpochMs::try_from(self.floors.hour_ms)?.into(),
            ts_day_ms: EpochMs::try_from(self.floors.day_ms)?.into(),
            ts_week_ms: EpochMs::try_from(self.floors.week_ms)?.into(),
            ts_month_ms: EpochMs::try_from(self.floors.month_ms)?.into(),
            from_group_id: from.map(|v| v.group_id),
            from_collection_id: from.map(|v| v.collection_id),
            group_id: a.group_id,
            collection_id: a.collection_id,
            execution_id: a.execution_id.clone(),
            transaction_id: a.transaction_id.clone(),
            function_id: a.function_id,
            function_run_id: a.function_run_id.clone(),
            attempt_id: a.attempt_id.clone(),
            attempt_outcome: a.attempt_outcome,
            run_status: self.status,
            replay: ReplayOrdinal::try_from(a.replay)?,
            current: CurrentAttempt::from(a.current),
            run_has_inputs: RunHasInputs::from(a.run_has_inputs),
            run_has_outputs: RunHasOutputs::from(self.run_has_outputs),
            side,
            table_id: version.map(|v| v.table_id),
            table_version_id: version.map(|v| v.table_version_id.clone()),
            private: Private::from(version.is_some_and(|v| v.private)),
            has_data: HasData::from(version.is_some_and(|v| v.has_data)),
            row_count: RowCount::try_from(version.map_or(0, |v| v.row_count))?,
            data_bytes: ByteCount::try_from(version.map_or(0, |v| v.data_bytes))?,
            first_consumed_on: None,
            edge_kind,
            edge_first: EdgeFirst::from(edge_first),
            edge_first_group: EdgeFirstGroup::from(edge_first_group),
        })
    }
}

/// An egress copy of each read version is added when the run writes nothing.
pub fn fact_rows(attempt: &EndedAttempt) -> Result<Vec<MetricsFactInsertDB>, TdError> {
    let ended: DateTime<Utc> = **attempt.ended_on;
    let row = Row {
        attempt,
        status: stamped_status(attempt.attempt_outcome, attempt.run_status),
        floors: BucketFloors::from_utc(ended),
        run_has_outputs: !attempt.outs.is_empty(),
    };

    let mut rows = vec![row.build(FactSide::Attempt, None, None, None, false, false)?];

    let source_edge = !attempt.run_has_inputs;
    for (pos, out) in attempt.outs.iter().enumerate() {
        let first = source_edge && pos == 0;
        rows.push(row.build(
            FactSide::Written,
            None,
            Some(out),
            source_edge.then_some(EdgeKind::ExternalSource),
            first,
            first,
        )?);
    }

    let mut seen_collection: HashSet<CollectionId> = HashSet::new();
    let mut seen_group: HashSet<GroupId> = HashSet::new();
    let egress = !row.run_has_outputs;
    for (pos, input) in attempt.ins.iter().enumerate() {
        rows.push(row.build(
            FactSide::Read,
            Some(input),
            Some(input),
            Some(EdgeKind::Real),
            seen_collection.insert(input.collection_id),
            seen_group.insert(input.group_id),
        )?);
        if egress {
            let first = pos == 0;
            rows.push(row.build(
                FactSide::Egress,
                Some(input),
                Some(input),
                Some(EdgeKind::Egress),
                first,
                first,
            )?);
        }
    }
    Ok(rows)
}
