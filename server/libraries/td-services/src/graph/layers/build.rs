//
// Copyright 2025 Tabs Data Inc.
//

use crate::graph::layers::{FunctionOrTable, GraphConfig};
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::States;
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDB, FunctionDBWithNames};
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::graph::{TdGraph, TdGraphBuilder};
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::CteQueries;
use td_objects::sql::recursive::RecursiveQueries;
use td_objects::types::basic::{AtTime, FunctionId, TableId};
use td_objects::types::recursion::RecursionOptions;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub struct GraphBuilder<
    C: GraphConfig<TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>,
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
> {
    config: std::marker::PhantomData<C>,
}

impl<
    C: GraphConfig<TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>,
    const TRIGGER_STATE: u8,
    const DEPENDENCY_STATE: u8,
    const FUNCTION_STATE: u8,
    const TABLE_STATE: u8,
> GraphBuilder<C, TRIGGER_STATE, DEPENDENCY_STATE, FUNCTION_STATE, TABLE_STATE>
where
    TriggerDBWithNames: States<TRIGGER_STATE>,
    DependencyDBWithNames: States<DEPENDENCY_STATE>,
    FunctionDB: States<FUNCTION_STATE>,
    TableDB: States<TABLE_STATE>,
{
    pub async fn find_recursive_trigger_graph(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(function_or_table): Input<FunctionOrTable>,
        Input(recursion_options): Input<RecursionOptions>,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let trigger_graph = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => {
                queries
                    .select_recursive_versions_at_with::<TRIGGER_STATE, TriggerDBWithNames, FUNCTION_STATE, FunctionDB>(
                        Some(&at_time),
                        Some(&at_time),
                        function_id,
                        recursion_options.deref(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?
            }
            FunctionOrTable::Table(table_id) => {
                queries
                    .select_recursive_versions_at_with::<TRIGGER_STATE, TriggerDBWithNames, TABLE_STATE, TableDB>(
                        Some(&at_time),
                        Some(&at_time),
                        table_id,
                        recursion_options.deref(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?
            }
        };

        Ok(trigger_graph)
    }

    pub async fn find_recursive_dependency_graph(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(function_or_table): Input<FunctionOrTable>,
        Input(recursion_options): Input<RecursionOptions>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let dep_graph = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) =>{
                queries
                    .select_recursive_versions_at_with::<DEPENDENCY_STATE, DependencyDBWithNames, FUNCTION_STATE, FunctionDB>(
                        Some(&at_time),
                        Some(&at_time),
                        function_id,
                        recursion_options.deref(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?
            }
            FunctionOrTable::Table(table_id)=> {
                queries
                    .select_recursive_versions_at_with::<DEPENDENCY_STATE, DependencyDBWithNames, TABLE_STATE, TableDB>(
                        Some(&at_time),
                        Some(&at_time),
                        table_id,
                        recursion_options.deref(),
                    )?
                    .build_query_as()
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?
            }
        };

        Ok(dep_graph)
    }

    pub async fn main_function_id(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(function_or_table): Input<FunctionOrTable>,
        Input(at_time): Input<AtTime>,
    ) -> Result<FunctionId, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let function_id = match function_or_table.deref() {
            FunctionOrTable::Function(function_id) => *function_id,
            FunctionOrTable::Table(table_id) => {
                let table: TableDB = queries
                    .select_versions_at::<TABLE_STATE, TableDB>(Some(&at_time), &table_id)?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                table.function_id
            }
        };

        Ok(function_id)
    }

    pub async fn unique_function_ids(
        Input(function_id): Input<FunctionId>,
        Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
        Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
    ) -> Result<Vec<FunctionId>, TdError> {
        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|f| [f.trigger_by_function_id, f.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|d| [d.table_function_id, d.function_id]);

        let unique_triggered_function_ids = trigger_function_ids
            .into_iter()
            .chain(dep_function_ids.into_iter())
            .chain(std::iter::once(*function_id.deref()))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn unique_triggered_function_ids(
        Input(function_id): Input<FunctionId>,
        Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
    ) -> Result<Vec<FunctionId>, TdError> {
        let unique_triggered_function_ids = trigger_graph
            .iter()
            .map(|f| f.function_id)
            .chain(std::iter::once(*function_id.deref()))
            .unique()
            .collect();

        Ok(unique_triggered_function_ids)
    }

    pub async fn find_triggered_functions_dependency_graph(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(triggered_functions): Input<Vec<FunctionId>>,
    ) -> Result<Vec<DependencyDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        // TODO this should be chunked
        let dep_graph: Vec<DependencyDBWithNames> = queries
            .find_versions_at::<DEPENDENCY_STATE, DependencyDBWithNames>(
                Some(&at_time),
                triggered_functions.deref(),
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        Ok(dep_graph)
    }

    pub async fn find_functions_output_tables(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(functions): Input<Vec<FunctionId>>,
    ) -> Result<Vec<TableDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        // Find output tables for each triggered_function
        // TODO this should be chunked
        let output_tables: Vec<TableDBWithNames> = queries
            .find_versions_at::<{ TableDBWithNames::Active }, TableDBWithNames>(
                Some(&at_time),
                functions.deref(),
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        Ok(output_tables)
    }

    pub async fn find_all_tables_map(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
        Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
        Input(output_tables): Input<Vec<TableDBWithNames>>,
    ) -> Result<HashMap<TableId, TableDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let trigger_table_ids = trigger_graph.iter().map(|t| t.trigger_by_table_id);
        let dep_table_ids = dep_graph.iter().map(|t| t.table_id);
        let output_table_ids = output_tables.iter().map(|t| t.table_id);

        let all_table_ids = trigger_table_ids
            .chain(dep_table_ids)
            .chain(output_table_ids)
            .unique()
            .collect::<Vec<_>>();

        // TODO this should be chunked
        let all_tables: Vec<TableDBWithNames> = queries
            .find_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
                Some(&at_time),
                all_table_ids.deref(),
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        let all_tables = all_tables
            .into_iter()
            .map(|t| (t.table_id, t))
            .collect::<HashMap<_, _>>();

        Ok(all_tables)
    }

    pub async fn find_all_functions_map(
        SrvCtx(queries): SrvCtx<DaoQueries>,
        Connection(connection): Connection,
        Input(at_time): Input<AtTime>,
        Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
        Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
        Input(output_tables): Input<Vec<TableDBWithNames>>,
    ) -> Result<HashMap<FunctionId, FunctionDBWithNames>, TdError> {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let trigger_function_ids = trigger_graph
            .iter()
            .flat_map(|t| [t.trigger_by_function_id, t.function_id]);
        let dep_function_ids = dep_graph
            .iter()
            .flat_map(|t| [t.table_function_id, t.function_id]);
        let output_function_ids = output_tables.iter().map(|t| t.function_id);

        let all_function_ids = trigger_function_ids
            .chain(dep_function_ids)
            .chain(output_function_ids)
            .unique()
            .collect::<Vec<_>>();

        // TODO this should be chunked
        let all_functions: Vec<FunctionDBWithNames> = queries
            .find_versions_at::<{ FunctionDBWithNames::All }, FunctionDBWithNames>(
                Some(&at_time),
                all_function_ids.deref(),
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        let all_functions = all_functions
            .into_iter()
            .map(|f| (f.function_id, f))
            .collect::<HashMap<_, _>>();

        Ok(all_functions)
    }

    pub async fn build_td_graph(
        Input(trigger_graph): Input<Vec<TriggerDBWithNames>>,
        Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
        Input(output_tables): Input<Vec<TableDBWithNames>>,
        Input(all_tables_map): Input<HashMap<TableId, TableDBWithNames>>,
        Input(all_functions_map): Input<HashMap<FunctionId, FunctionDBWithNames>>,
    ) -> Result<TdGraph, TdError> {
        let graph = TdGraphBuilder::new(&all_tables_map, &all_functions_map)
            .with_triggers(&trigger_graph)?
            .with_dependencies(&dep_graph)?
            .with_outputs(&output_tables)?
            .build();
        Ok(graph)
    }
}
