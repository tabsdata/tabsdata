//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::ExecutionPlan;
use td_execution::topology::ProjectTopology;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB, LogicalTableDataVersionDB,
};
use td_objects::types::basic::{FunctionId, LogicalTableDataVersionId, TableId};
use td_queries::Queries;

/// The plan an execution recorded, rebuilt from its runs and their inputs
/// over the topology version it planned on: a trigger row names the version
/// it pinned, and that version its table. It never plans again, so how plans
/// are computed may change without changing what older executions read.
pub async fn recorded_execution_plan<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    execution: &ExecutionDB,
    topology: &ProjectTopology,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
    inputs: &[LogicalFunctionRunInputInsertDB],
) -> Result<ExecutionPlan, TdError> {
    let function_of: HashMap<_, FunctionId> = logical_function_runs
        .iter()
        .map(|lfr| (&lfr.id, lfr.function_id))
        .collect();
    let pinned: Vec<LogicalTableDataVersionId> = inputs
        .iter()
        .filter(|i| i.requirement_dependency_pos.is_none())
        .filter_map(|i| i.requirement_logical_table_data_version_id.clone())
        .collect();
    let table_of: HashMap<LogicalTableDataVersionId, TableId> = conn
        .timeline()
        .chunked()
        .fetch_all::<LogicalTableDataVersionDB>(&pinned, ())
        .await?
        .into_iter()
        .map(|l| (l.base.id, l.base.table_id))
        .collect();

    let mut triggers = Vec::new();
    let mut dependencies = Vec::new();
    for input in inputs {
        let function = function_of[&input.logical_function_run_id];
        match input.requirement_dependency_pos {
            Some(pos) => dependencies.push((function, pos)),
            None => {
                let table = input
                    .requirement_logical_table_data_version_id
                    .as_ref()
                    .and_then(|id| table_of.get(id));
                if let Some(table) = table {
                    triggers.push((function, *table));
                }
            }
        }
    }
    triggers.sort();
    triggers.dedup();
    dependencies.sort();
    dependencies.dedup();
    topology.recorded(&execution.function_id, &triggers, &dependencies)
}
