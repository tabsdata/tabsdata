//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use ta_execution::graphs::{ExecutionGraph, GraphBuilder};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::execution::graph::FunctionNodeBuilder;
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::CteQueries;
use td_objects::sql::recursive::RecursiveQueries;
use td_objects::table_ref::Versions;
use td_objects::types::basic::{AtTime, FunctionId, FunctionStatus};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use te_execution::transaction::TransactionBy;

pub async fn assert_function_status(
    Input(function): Input<FunctionDBWithNames>,
) -> Result<(), TdError> {
    if !matches!(function.status, FunctionStatus::Active) {
        Err(api_error!(
            ApiError::InputError,
            "Function '{}' must be in an active status to execute. Current status: '{:?}'",
            function.name,
            function.status
        ))?
    }
    Ok(())
}

pub async fn find_trigger_graph(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(function): Input<FunctionId>,
) -> Result<Vec<TriggerDBWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Find triggered functions at the given time
    let trigger_graph: Vec<TriggerDBWithNames> = queries
        .select_recursive_versions_at::<{ TriggerDBWithNames::Active }, TriggerDBWithNames, { FunctionDBWithNames::DownstreamTrigger }, FunctionDBWithNames>(
            Some(&at_time),
            Some(&at_time),
            function.deref(),
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(trigger_graph)
}

pub async fn find_all_input_tables(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(trigger_function): Input<FunctionDBWithNames>,
    Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
) -> Result<Vec<DependencyDBWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Compute unique triggered function ids
    // Add manual trigger function (which is an implicit trigger)
    let manual_trigger = &trigger_function.function_id;
    let unique_triggered_function_ids = trigger_graph
        .iter()
        .map(|f| &f.function_id)
        .chain(std::iter::once(manual_trigger))
        .collect::<HashSet<_>>();
    let unique_triggered_function_ids: Vec<_> = unique_triggered_function_ids.into_iter().collect();

    // TODO this should be chunked
    let dep_graph: Vec<DependencyDBWithNames> = queries
        .find_versions_at::<{ DependencyDBWithNames::Active }, DependencyDBWithNames>(
            Some(&at_time),
            &unique_triggered_function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(dep_graph)
}

pub async fn build_execution_template(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(trigger_function): Input<FunctionDBWithNames>,
    Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
    Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
) -> Result<ExecutionGraph<Versions>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Compute unique triggered function ids
    // Add manual trigger function (which is an implicit trigger)
    let manual_trigger = &trigger_function.function_id;
    let unique_triggered_function_ids = trigger_graph
        .iter()
        .map(|f| &f.function_id)
        .chain(std::iter::once(manual_trigger))
        .collect::<HashSet<_>>();
    let unique_triggered_function_ids: Vec<_> = unique_triggered_function_ids.into_iter().collect();
    let unique_triggered_table_ids: HashSet<_> = trigger_graph
        .iter()
        .map(|t| &t.trigger_by_table_id)
        .collect();
    let unique_triggered_table_ids: Vec<_> = unique_triggered_table_ids.into_iter().collect();

    // Find output tables for each triggered_function
    // TODO this should be chunked
    let output_tables: Vec<TableDBWithNames> = queries
        .find_versions_at::<{ TableDBWithNames::Output }, TableDBWithNames>(
            Some(&at_time),
            &unique_triggered_function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // Find table-function pair for each triggered_function
    // TODO this should be chunked
    let trigger_tables: Vec<TableDBWithNames> = queries
        .find_versions_at::<{ TableDBWithNames::Output }, TableDBWithNames>(
            Some(&at_time),
            &unique_triggered_table_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    let trigger_tables: HashMap<_, _> = trigger_tables.iter().map(|t| (t.table_id, t)).collect();

    // TODO this should be chunked
    let trigger_functions: Vec<FunctionDBWithNames> = queries
        .find_versions_at::<{ FunctionDBWithNames::DownstreamTrigger }, FunctionDBWithNames>(
            Some(&at_time),
            &unique_triggered_function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    let trigger_functions: HashMap<_, _> = trigger_functions
        .iter()
        .map(|t| (&t.function_id, t))
        .collect();

    // Recompute the trigger graph with table-function pairs
    let trigger_graph = trigger_graph
        .iter()
        .filter_map(|trigger| {
            // Filter out triggers that do not have a corresponding active table or function
            // This can happen on active functions that have frozen tables (because we query
            // the trigger graph by active functions only)
            if let (Some(table), Some(function)) = (
                trigger_tables.get(&trigger.trigger_by_table_id),
                trigger_functions.get(&trigger.function_id),
            ) {
                Some((trigger, *table, *function))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Compute unique dependency functions-tables
    let unique_dep_function_ids = dep_graph
        .iter()
        .map(|f| &f.function_id)
        .collect::<HashSet<_>>();
    let unique_dep_function_ids: Vec<_> = unique_dep_function_ids.into_iter().collect();
    let unique_dep_table_ids: HashSet<_> = dep_graph.iter().map(|t| t.table_id).collect();
    let unique_dep_table_ids: Vec<_> = unique_dep_table_ids.into_iter().collect();

    // Find table-function pair for each dependency
    // TODO this should be chunked
    let dep_tables: Vec<TableDBWithNames> = queries
        .find_versions_at::<{ TableDBWithNames::InputDependency }, TableDBWithNames>(
            Some(&at_time),
            &unique_dep_table_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    let dep_tables: HashMap<_, _> = dep_tables.iter().map(|t| (t.table_id, t)).collect();

    // TODO this should be chunked
    let dep_functions: Vec<FunctionDBWithNames> = queries
        .find_versions_at::<{ FunctionDBWithNames::Active }, FunctionDBWithNames>(
            Some(&at_time),
            &unique_dep_function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;
    let dep_functions: HashMap<_, _> = dep_functions.iter().map(|t| (t.function_id, t)).collect();

    // Recompute the dependency graph with table-function pairs
    let dep_graph = dep_graph
        .iter()
        .filter_map(|dep| {
            // It should not happen that a dependency does not have a corresponding
            // entry in the dep_tables or dep_functions, but we filter it out just in case.
            if let (Some(table), Some(function)) = (
                dep_tables.get(&dep.table_id),
                dep_functions.get(&dep.function_id),
            ) {
                Some((dep, *table, *function))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let trigger = FunctionNodeBuilder::try_from(&*trigger_function)?.build()?;
    let graph = GraphBuilder::new(&output_tables, &trigger_graph, &dep_graph).build(trigger)?;

    graph.validate_dag()?;
    graph.validate_transaction(transaction_by.deref())?;

    Ok(graph)
}
