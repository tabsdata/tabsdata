//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;
use td_database::access::{InProject, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{ApiError, TdError, api_error};
use td_execution::transaction_map::TransactionMap;
use td_execution::{ExecutionPlan, ResolvedExecutionPlan, TransactionBy};
use td_objects::dxo::execution::ExecutionInsertDB;
use td_objects::dxo::logical::{
    LogicalFunctionRunInputInsertDB, LogicalFunctionRunInsertDB, LogicalTableDataVersionInsertDB,
};
use td_objects::dxo::transaction::TransactionInsertDB;
use td_objects::dxo::transaction::TransactionValue;
use td_objects::types::basic::{
    InputIdx, LogicalFunctionRunId, LogicalFunctionRunInputId, LogicalFunctionRunStatus,
    LogicalTableDataVersionId, Trigger, VersionPos,
};
use td_queries::Queries;
use tracing::{debug, instrument};

pub fn build_transaction_map(
    transaction_by: &TransactionBy,
    execution: &ExecutionInsertDB,
    execution_plan: &ExecutionPlan,
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut transaction_map = TransactionMap::empty(transaction_by.clone());

    let roots = execution_plan.transaction_roots();
    for function in execution_plan.triggered_functions() {
        transaction_map.add(execution, roots.root(function))?;
    }

    Ok(transaction_map)
}

pub fn build_transactions(
    transaction_map: &TransactionMap<TransactionBy>,
    execution: &ExecutionInsertDB,
) -> Result<Vec<TransactionInsertDB>, TdError> {
    let transactions = transaction_map
        .iter()
        .map(|t| {
            let transaction = transaction_map.get(t)?;
            Ok(TransactionInsertDB {
                id: transaction.id.clone(),
                collection_id: transaction.collection_id,
                execution_id: execution.id.clone(),
                transaction_by: transaction.transaction_by.clone(),
                transaction_key: transaction.transaction_key.clone(),
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                group_id: transaction.group_id,
            })
        })
        .collect::<Result<_, TdError>>()?;

    Ok(transactions)
}

/// One LFR per triggered function; dispatch mints its first attempt.
pub fn build_logical_function_runs(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution_plan: &ExecutionPlan,
    execution: &ExecutionInsertDB,
) -> Result<Vec<LogicalFunctionRunInsertDB>, TdError> {
    let start = execution_plan.trigger();
    let roots = execution_plan.transaction_roots();
    execution_plan
        .triggered_functions()
        .into_iter()
        .map(|f| {
            let transaction = transaction_map.get(&roots.key(transaction_by, f)?)?;
            let trigger = if f == start {
                Trigger::Manual
            } else {
                Trigger::Dependency
            };
            Ok(LogicalFunctionRunInsertDB {
                id: LogicalFunctionRunId::new(),
                collection_id: f.collection_id,
                execution_id: execution.id.clone(),
                transaction_id: transaction.id.clone(),
                function_id: f.function_id,
                function_version_id: f.function_version_id.clone(),
                trigger,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                status: LogicalFunctionRunStatus::Scheduled,
                group_id: f.group_id,
            })
        })
        .collect()
}

/// One LTDV per declared output edge; the producer is the LFR of the
/// function on the edge's source side. Which materialization it ends up
/// bound to is the materialization's own business.
pub fn build_logical_table_data_versions(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    execution_plan: &ExecutionPlan,
    execution: &ExecutionInsertDB,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
) -> Result<Vec<LogicalTableDataVersionInsertDB>, TdError> {
    let roots = execution_plan.transaction_roots();
    // Keyed by function_id (logical identity); carries group_id so the LTDV
    // snapshot stays equal-by-construction to the LFR's.
    let ffr_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, (lfr.id.clone(), lfr.group_id)))
        .collect();

    execution_plan
        .outputs()
        .iter()
        .map(|(t, f, edge)| {
            let transaction = transaction_map.get(&roots.key(transaction_by, f)?)?;
            let (producer, group_id) = ffr_by_function.get(&f.function_id).ok_or_else(|| {
                api_error!(
                    ApiError::InternalError,
                    "Producer LFR not found for function_id {} while building LTDVs",
                    f.function_id
                )
            })?;
            Ok(LogicalTableDataVersionInsertDB {
                id: LogicalTableDataVersionId::new(),
                collection_id: f.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                table_id: t.table_id,
                producer_logical_function_run_id: Some(producer.clone()),
                function_param_pos: edge.output_pos().cloned(),
                private: t.private,
                triggered_on: execution.triggered_on,
                triggered_by_id: execution.triggered_by_id,
                group_id: *group_id,
            })
        })
        .collect()
}

/// One row per resolved-version per input edge of every triggered function:
/// edges without resolved versions (trigger edges, or dep edges that
/// resolved to nothing) emit a single `requirement_version_pos = NO_VERSIONS`
/// row; dep edges with resolved versions emit one row per version.
///
/// Provenance `input_idx`: a single index across the plan's resolved input
/// versions. Edges are walked in `(function_id, dep_pos)` order so the
/// assignment is reproducible whenever the plan is rebuilt. Rows without a
/// bound version carry `InputIdx::NO_DATA`; trigger rows carry NULL.
pub fn build_logical_function_run_inputs(
    transaction_by: &TransactionBy,
    transaction_map: &TransactionMap<TransactionBy>,
    resolved_plan: &ResolvedExecutionPlan,
    logical_function_runs: &[LogicalFunctionRunInsertDB],
) -> Result<Vec<LogicalFunctionRunInputInsertDB>, TdError> {
    let lfr_id_by_function: HashMap<_, _> = logical_function_runs
        .iter()
        .map(|lfr| (lfr.function_id, lfr.id.clone()))
        .collect();
    let roots = resolved_plan.transaction_roots();

    let mut edges: Vec<_> = resolved_plan.inputs().into_iter().collect();
    edges.sort_by_key(|(function, _, edge)| (function.function_id, edge.dependency_pos().cloned()));

    let mut next_input_idx: i32 = 0;
    let mut rows = Vec::new();
    for (function, table, edge) in edges {
        let Some(consumer_lfr_id) = lfr_id_by_function.get(&function.function_id) else {
            return Err(api_error!(
                ApiError::InternalError,
                "Consumer LFR not found for function_id {} while building logical inputs",
                function.function_id
            ));
        };
        let transaction = transaction_map.get(&roots.key(transaction_by, function)?)?;

        let dependency_pos = edge.dependency_pos().cloned();
        let resolved = &edge.versions().inner;

        // Unresolved slots still emit rows (NULL requirement LTDV): the
        // worker yaml at dispatch falls back to `DependencyDB` for those.
        if resolved.is_empty() {
            rows.push(LogicalFunctionRunInputInsertDB {
                id: LogicalFunctionRunInputId::default(),
                collection_id: function.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                logical_function_run_id: consumer_lfr_id.clone(),
                requirement_logical_table_data_version_id: None,
                requirement_input_idx: dependency_pos.is_some().then_some(InputIdx::NO_DATA),
                requirement_dependency_pos: dependency_pos,
                requirement_version_pos: VersionPos::NO_VERSIONS,
                requirement_table_version_id: dependency_pos
                    .is_some()
                    .then_some(table.table_version_id),
            });
            continue;
        }

        // A self dependency's shift keeps the requested multiplicity.
        let multiple = edge.versions().original.is_multiple();
        for (slot, ltdv) in resolved.iter().enumerate() {
            let version_pos = if multiple {
                VersionPos::try_from(slot as i32)?
            } else {
                VersionPos::SINGLE_VERSION
            };
            let requirement_input_idx = if dependency_pos.is_none() {
                None
            } else if ltdv.is_some() {
                let idx = InputIdx::try_from(next_input_idx)?;
                next_input_idx += 1;
                Some(idx)
            } else {
                Some(InputIdx::NO_DATA)
            };
            rows.push(LogicalFunctionRunInputInsertDB {
                id: LogicalFunctionRunInputId::default(),
                collection_id: function.collection_id,
                execution_id: transaction.execution_id.clone(),
                transaction_id: transaction.id.clone(),
                logical_function_run_id: consumer_lfr_id.clone(),
                requirement_logical_table_data_version_id: ltdv.clone(),
                requirement_input_idx,
                requirement_dependency_pos: dependency_pos,
                requirement_version_pos: version_pos,
                requirement_table_version_id: dependency_pos
                    .is_some()
                    .then_some(table.table_version_id),
            });
        }
    }

    Ok(rows)
}

/// Flips to OnHold the plan's LFRs whose upstream inputs are Failed/OnHold.
#[instrument(
    name = "execution.update_initial_function_run_status",
    skip_all,
    fields(execution_id = %execution.id),
    level = "debug",
    err(Debug, level = "debug")
)]
pub async fn update_initial_function_run_status(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    execution: &ExecutionInsertDB,
) -> Result<(), TdError> {
    let lfr_ids = conn.logical_runs().blocked_ids(&execution.id).await?;

    debug!(
        execution_id = %execution.id,
        on_hold_count = lfr_ids.len(),
        "execution.initial_on_hold: {} LFR(s) start OnHold",
        lfr_ids.len()
    );

    conn.logical_runs()
        .set_status(&lfr_ids, LogicalFunctionRunStatus::OnHold)
        .await?;

    Ok(())
}

// Similar to build_transaction_map, but using already existing transactions instead of creating new ones.
pub fn build_existing_transaction_map(
    transaction_by: &TransactionBy,
    transactions: &[TransactionInsertDB],
) -> Result<TransactionMap<TransactionBy>, TdError> {
    let mut mapped_transactions = HashMap::new();
    for transaction in transactions.iter() {
        if !mapped_transactions.contains_key(&transaction.transaction_key) {
            mapped_transactions.insert(
                transaction.transaction_key.clone(),
                TransactionValue::from(transaction),
            );
        }
    }

    // We should always have at least one transaction, and all transactions should have the same transaction_by.
    // In case it differs with the one configured in the system, we prioritize the one from the transaction.
    // This can happen when loading transactions from previous executions.
    let transaction_by_str = transactions.first().map(|t| t.transaction_by.clone());
    let transaction_by = if let Some(transaction_by_str) = transaction_by_str {
        transaction_by_str.parse().unwrap_or(transaction_by.clone())
    } else {
        transaction_by.clone()
    };
    let transaction_map = TransactionMap::from_map(mapped_transactions, transaction_by);
    Ok(transaction_map)
}
