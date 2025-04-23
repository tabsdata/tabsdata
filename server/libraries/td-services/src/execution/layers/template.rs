//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::HashSet;
use std::ops::Deref;
use ta_execution::graphs::{ExecutionGraph, GraphBuilder};
use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::sql::cte::CteQueries;
use td_objects::sql::recursive::RecursiveQueries;
use td_objects::sql::DerefQueries;
use td_objects::types::basic::{AtTime, DependencyStatus, FunctionId, FunctionStatus, TableStatus};
use td_objects::types::dependency::DependencyVersionDBWithNames;
use td_objects::types::execution::FunctionVersionNodeBuilder;
use td_objects::types::function::{FunctionVersionDB, FunctionVersionDBWithNames};
use td_objects::types::table::TableVersionDBWithNames;
use td_objects::types::table_ref::Versions;
use td_objects::types::trigger::TriggerVersionDBWithNames;
use td_objects::types::{DataAccessObject, NaturalOrder, PartitionBy, Recursive, Status};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};
use te_execution::transaction::TransactionBy;

pub async fn version_graph<Q, V>(
    SrvCtx(queries): SrvCtx<Q>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(function): Input<FunctionId>,
) -> Result<Vec<V>, TdError>
where
    Q: DerefQueries,
    V: DataAccessObject + PartitionBy + Recursive + NaturalOrder + Status,
    V::Status: Default,
    V::NaturalOrder: From<AtTime>,
{
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let v_at_time = V::NaturalOrder::from(at_time.deref().clone());

    let result: Vec<V> = queries
        .select_recursive_versions_at::<V, FunctionVersionDB, _>(
            Some(&v_at_time),
            Some(&[&V::Status::default()]),
            Some(&at_time),
            Some(&[&FunctionStatus::Active]),
            function.deref(),
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(result)
}

pub async fn build_execution_template<Q: DerefQueries>(
    SrvCtx(queries): SrvCtx<Q>,
    SrvCtx(transaction_by): SrvCtx<TransactionBy>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(trigger_function_version): Input<FunctionVersionDBWithNames>,
    Input(trigger_graph): Input<Vec<TriggerVersionDBWithNames>>,
) -> Result<ExecutionGraph<Versions>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // We find unique triggered functions
    let unique_triggered_functions = trigger_graph
        .iter()
        .map(|f| f.function_version_id())
        .collect::<HashSet<_>>();

    let manual_trigger = trigger_function_version.id();
    let unique_triggered_functions: Vec<_> = unique_triggered_functions
        .into_iter()
        .chain(std::iter::once(manual_trigger))
        .collect();

    // So we can find output and input tables for them
    // TODO this should be chunked
    let status = [&DependencyStatus::Active];
    let input_tables: Vec<DependencyVersionDBWithNames> = queries
        .find_versions_at::<DependencyVersionDBWithNames>(
            Some(&at_time),
            Some(&status),
            &unique_triggered_functions,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // TODO this should be chunked
    let status = [&TableStatus::Active, &TableStatus::Frozen];
    let output_tables: Vec<TableVersionDBWithNames> = queries
        .find_versions_at::<TableVersionDBWithNames>(
            Some(&at_time),
            Some(&status),
            &unique_triggered_functions,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    let trigger = FunctionVersionNodeBuilder::try_from(&*trigger_function_version)?.build()?;
    let graph = GraphBuilder::new(&trigger_graph, &output_tables, &input_tables).build(trigger)?;

    graph.validate_dag()?;
    graph.validate_transaction(transaction_by.deref())?;

    Ok(graph)
}
