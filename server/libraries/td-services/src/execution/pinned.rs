//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_execution::{ExecutionPlan, ResolvedExecutionPlan};
use td_objects::dxo::graph::{FunctionNode, GraphEdge, GraphNode, ResolvedVersion};
use td_objects::dxo::logical::{
    LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB, LogicalTableDataVersionDB,
};
use td_objects::types::basic::{
    FunctionId, LogicalFunctionRunId, LogicalTableDataVersionId, TableId, VersionPos,
};
use td_objects::types::table_ref::Versions;
use td_queries::Queries;

/// The versions an execution's plan pinned on each edge, read back from what
/// the plan persisted: its runs' inputs, and the versions its runs produce.
pub async fn pinned_execution_plan_versions<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    execution_plan: &ExecutionPlan,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
    inputs: &[LogicalFunctionRunInputInsertDB],
) -> Result<ResolvedExecutionPlan, TdError> {
    let producers: Vec<Option<LogicalFunctionRunId>> = logical_function_runs
        .iter()
        .map(|lfr| Some(lfr.id.clone()))
        .collect();
    let produced: Vec<LogicalTableDataVersionDB> =
        conn.timeline().chunked().fetch_all(&producers, ()).await?;
    let produced_by_table: HashMap<TableId, LogicalTableDataVersionId> = produced
        .iter()
        .map(|l| (l.base.table_id, l.base.id.clone()))
        .collect();
    let mut table_of: HashMap<LogicalTableDataVersionId, TableId> = produced
        .into_iter()
        .map(|l| (l.base.id, l.base.table_id))
        .collect();
    // A trigger on a table the plan does not write pinned an older version.
    let elsewhere: Vec<LogicalTableDataVersionId> = inputs
        .iter()
        .filter(|i| i.requirement_dependency_pos.is_none())
        .filter_map(|i| i.requirement_logical_table_data_version_id.clone())
        .filter(|id| !table_of.contains_key(id))
        .collect();
    if !elsewhere.is_empty() {
        let read: Vec<LogicalTableDataVersionDB> =
            conn.timeline().chunked().fetch_all(&elsewhere, ()).await?;
        table_of.extend(read.into_iter().map(|l| (l.base.id, l.base.table_id)));
    }

    let lfr_of: HashMap<FunctionId, &LogicalFunctionRunId> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, &lfr.id))
        .collect();
    let mut inputs_of: HashMap<&LogicalFunctionRunId, Vec<&LogicalFunctionRunInputInsertDB>> =
        HashMap::new();
    for input in inputs {
        inputs_of
            .entry(&input.logical_function_run_id)
            .or_default()
            .push(input);
    }
    for rows in inputs_of.values_mut() {
        rows.sort_by_key(|i| *i.requirement_version_pos);
    }

    let pinned = Pinned {
        produced_by_table,
        table_of,
        lfr_of,
        inputs_of,
    };
    execution_plan
        .map(
            |node| async move { Ok::<_, TdError>(node.clone()) },
            |edge, source, target| {
                let resolved = pinned.versions(edge, source, target);
                async move { resolved }
            },
        )
        .await
}

struct Pinned<'a> {
    produced_by_table: HashMap<TableId, LogicalTableDataVersionId>,
    table_of: HashMap<LogicalTableDataVersionId, TableId>,
    lfr_of: HashMap<FunctionId, &'a LogicalFunctionRunId>,
    inputs_of: HashMap<&'a LogicalFunctionRunId, Vec<&'a LogicalFunctionRunInputInsertDB>>,
}

impl Pinned<'_> {
    fn versions(
        &self,
        edge: &GraphEdge<Versions>,
        source: &GraphNode,
        target: &GraphNode,
    ) -> Result<ResolvedVersion, TdError> {
        let inner = match (edge, source, target) {
            (GraphEdge::Output { .. }, _, GraphNode::Table(table)) => {
                vec![self.produced_by_table.get(&table.table_id).cloned()]
            }
            (GraphEdge::Trigger { .. }, GraphNode::Table(table), GraphNode::Function(function)) => {
                let pinned = self
                    .inputs(function)?
                    .filter(|i| i.requirement_dependency_pos.is_none())
                    .filter_map(|i| i.requirement_logical_table_data_version_id.clone())
                    .find(|id| self.table_of.get(id) == Some(&table.table_id));
                vec![pinned]
            }
            (
                GraphEdge::Dependency { dependency, .. },
                GraphNode::Table(_),
                GraphNode::Function(function),
            ) => {
                let rows: Vec<_> = self
                    .inputs(function)?
                    .filter(|i| i.requirement_dependency_pos == Some(dependency.dep_pos))
                    .collect();
                if rows
                    .iter()
                    .any(|i| i.requirement_version_pos == VersionPos::NO_VERSIONS)
                {
                    Vec::new()
                } else {
                    rows.iter()
                        .map(|i| i.requirement_logical_table_data_version_id.clone())
                        .collect()
                }
            }
            _ => Err(api_error!(
                ApiError::InternalError,
                "Execution plan edge between unexpected nodes"
            ))?,
        };
        Ok(ResolvedVersion {
            inner,
            original: edge.versions().clone().into(),
        })
    }

    fn inputs(
        &self,
        function: &FunctionNode,
    ) -> Result<impl Iterator<Item = &LogicalFunctionRunInputInsertDB>, TdError> {
        let lfr = self.lfr_of.get(&function.function_id).ok_or_else(|| {
            api_error!(
                ApiError::InternalError,
                "Function '{}' of the execution has no run",
                function.name
            )
        })?;
        Ok(self.inputs_of.get(lfr).into_iter().flatten().copied())
    }
}
