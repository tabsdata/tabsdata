//
// Copyright 2026 Tabsdata Inc.
//

use itertools::{Either, Itertools};
use std::collections::{HashMap, HashSet};
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::transaction_map::TransactionMap;
use td_execution::{ResolvedExecutionPlan, TransactionBy};
use td_objects::FromDao;
use td_objects::dxo::execution::{
    ExecutionInsertDB, ExecutionResponse, FunctionNodeResponse, ResolvedVersionResponse,
    TableNodeResponse,
};
use td_objects::dxo::graph::GraphNode;
use td_objects::dxo::logical::{
    LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB, TableDataVersion,
    TableDataVersionDBWithNames,
};
use td_objects::types::basic::{
    DependencyPos, Dot, FunctionId, InputIdx, LogicalTableDataVersionId, VersionPos,
};
use td_queries::Queries;

/// Every data version the plan's edges reference, as its logical identity
/// merged with whatever materialization is current for it right now.
pub async fn fetch_relations_info<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    execution_plan: &ResolvedExecutionPlan,
) -> Result<HashMap<LogicalTableDataVersionId, TableDataVersion>, TdError> {
    let ids: Vec<LogicalTableDataVersionId> = execution_plan
        .graph
        .inner
        .edge_weights()
        .flat_map(|edge| edge.versions().inner.iter().flatten().cloned())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    conn.timeline()
        .chunked()
        .fetch_all::<TableDataVersionDBWithNames>(&ids, ())
        .await?
        .into_iter()
        .map(|dao| {
            let info = TableDataVersion::from_dao(dao)?;
            Ok((info.id.clone(), info))
        })
        .collect()
}

pub async fn build_response(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution: &ExecutionInsertDB,
    execution_plan: &ResolvedExecutionPlan,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
    logical_function_run_inputs: &[LogicalFunctionRunInputInsertDB],
    relations_info: HashMap<LogicalTableDataVersionId, TableDataVersion>,
) -> Result<ExecutionResponse, TdError> {
    let dot = Dot::try_from(execution_plan.dot().to_string())?;

    // `input_idx` comes from the persisted LFRI rows so the response carries
    // the idxs the plan actually ran with — identically on execute and read.
    let function_by_lfr: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (&lfr.id, lfr.function_id))
        .collect();
    let idx_by_slot: HashMap<(FunctionId, DependencyPos, VersionPos), InputIdx> =
        logical_function_run_inputs
            .iter()
            .filter_map(|lfri| {
                let idx = lfri.requirement_input_idx?;
                let dep_pos = lfri.requirement_dependency_pos?;
                let function_id = function_by_lfr.get(&lfri.logical_function_run_id)?;
                Some(((*function_id, dep_pos, lfri.requirement_version_pos), idx))
            })
            .collect();
    let idx_by_slot = &idx_by_slot;

    let roots = execution_plan.transaction_roots();
    let transactions = execution_plan
        .triggered_functions()
        .into_iter()
        .chain(std::iter::once(execution_plan.trigger()))
        .try_fold(HashMap::new(), |mut acc, f| {
            let transaction = transaction_map.get(&roots.key(transaction_by, f)?)?;
            let entry: &mut HashSet<_> = acc.entry(transaction.id.clone()).or_default();
            entry.insert(f.function_version_id.clone());
            Ok::<_, TdError>(acc)
        })?;

    let execution_plan = execution_plan
        .map(
            |node| async move {
                let response = match node {
                    GraphNode::Function(function) => {
                        GraphNode::Function(FunctionNodeResponse::from(function))
                    }
                    GraphNode::Table(table) => GraphNode::Table(TableNodeResponse::from(table)),
                };
                Ok::<_, TdError>(response)
            },
            |edge, _, target| async move {
                let inner = edge.versions().inner.clone();

                // Pair each resolved slot with its persisted `input_idx`;
                // slots without a matching LFRI row render as NO_DATA.
                let input_idxs = match (edge.dependency_pos(), target) {
                    (Some(dep_pos), GraphNode::Function(function)) => {
                        let multiple = edge.versions().original.is_multiple();
                        (0..inner.len())
                            .map(|slot| {
                                let version_pos = if multiple {
                                    VersionPos::try_from(slot as i32)?
                                } else {
                                    VersionPos::SINGLE_VERSION
                                };
                                Ok(idx_by_slot
                                    .get(&(function.function_id, *dep_pos, version_pos))
                                    .copied()
                                    .unwrap_or(InputIdx::NO_DATA))
                            })
                            .collect::<Result<Vec<_>, TdError>>()?
                    }
                    _ => Vec::new(),
                };

                let response = ResolvedVersionResponse {
                    inner,
                    input_idxs,
                    original: edge.versions().original.clone(),
                };
                Ok::<_, TdError>(response)
            },
        )
        .await?;

    let relations = execution_plan
        .relations()
        .into_iter()
        .map(|(f, t, e)| {
            let resolved_version = e.versioned(e.versions().clone());
            (
                f.function_version_id.clone(),
                t.table_version_id,
                resolved_version,
            )
        })
        .sorted()
        .collect();

    let all_functions = execution_plan
        .functions()
        .into_iter()
        .map(|f| (f.function_version_id.clone(), f.clone()))
        .collect();
    let triggered_functions = execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| f.function_version_id.clone())
        .collect();

    let all_tables_set = execution_plan.tables();
    let all_tables = all_tables_set
        .iter()
        .map(|t| (t.table_version_id, (*t).clone()))
        .collect();
    let created_tables = execution_plan
        .created_tables()
        .iter()
        .map(|t| t.table_version_id)
        .collect();
    let (system_tables, user_tables): (HashSet<_>, HashSet<_>) =
        all_tables_set.iter().partition_map(|t| {
            if *t.system {
                Either::Left(t.table_version_id)
            } else {
                Either::Right(t.table_version_id)
            }
        });

    let response = ExecutionResponse {
        id: execution.id.clone(),
        name: execution.name.clone(),
        triggered_on: execution.triggered_on,
        trigger_type: execution.trigger_type.clone(),
        transactions,
        dot,
        all_functions,
        triggered_functions,
        manual_trigger: execution.function_version_id.clone(),
        all_tables,
        created_tables,
        system_tables,
        user_tables,
        relations,
        relations_info,
    };
    Ok(response)
}
