//
// Copyright 2025 Tabs Data Inc.
//

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::execution::{FunctionNodeResponseBuilder, TableNodeResponseBuilder};
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::{TableDB, TableDBWithNames, TableGraphResponse};
use td_objects::execution::graph::{GraphDependency, GraphEdge};
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::CteQueries;
use td_objects::sql::recursive::{RecursionOptions, RecursiveQueries};
use td_objects::types::basic::{AtTime, TableId};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub async fn find_dependency_graph(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(table_id): Input<TableId>,
) -> Result<Vec<DependencyDBWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let dep_graph: Vec<DependencyDBWithNames> = queries
        .select_recursive_versions_at_with::<{ DependencyDBWithNames::Active }, DependencyDBWithNames, { TableDB::Available }, TableDB>(
            Some(&at_time),
            Some(&at_time),
            table_id.deref(),
            RecursionOptions {
                max_upstream_depth: None,
                max_downstream_depth: None,
            }
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    Ok(dep_graph)
}

pub async fn build_response(
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Connection(connection): Connection,
    Input(at_time): Input<AtTime>,
    Input(table_id): Input<TableId>,
    Input(dep_graph): Input<Vec<DependencyDBWithNames>>,
) -> Result<TableGraphResponse, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Find current table function
    let current_table: TableDBWithNames = queries
        .select_versions_at::<{ TableDBWithNames::Available }, TableDBWithNames>(
            Some(&at_time),
            table_id.deref(),
        )?
        .build_query_as()
        .fetch_one(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    // Find all functions involved
    let function_ids = dep_graph
        .iter()
        .flat_map(|d| [d.function_id, d.table_function_id])
        .chain(std::iter::once(current_table.function_id))
        .collect::<HashSet<_>>();
    let function_ids = function_ids.into_iter().collect::<Vec<_>>();

    // TODO this should be chunked
    let all_functions: Vec<FunctionDBWithNames> = queries
        .find_versions_at::<{ FunctionDBWithNames::All }, FunctionDBWithNames>(
            Some(&at_time),
            &function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    let mut functions_map = HashMap::new();
    let all_functions = all_functions
        .iter()
        .map(|f| {
            functions_map.insert(f.function_id, f);
            Ok::<_, TdError>((f.id, FunctionNodeResponseBuilder::try_from(f)?.build()?))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    // Find all tables (we assume all tables in functions found by dependencies could be relevant)
    // TODO this should be chunked
    let tables_found: Vec<TableDBWithNames> = queries
        .find_versions_at::<{ TableDBWithNames::All }, TableDBWithNames>(
            Some(&at_time),
            &function_ids,
        )?
        .build_query_as()
        .fetch_all(&mut *conn)
        .await
        .map_err(handle_sql_err)?;

    let mut tables_map = HashMap::new();
    let mut system_tables = HashSet::new();
    let mut user_tables = HashSet::new();
    let all_tables = tables_found
        .iter()
        .map(|t| {
            tables_map.insert(t.table_id, t);
            if *t.system {
                system_tables.insert(t.id);
            } else {
                user_tables.insert(t.id);
            }
            Ok::<_, TdError>((t.id, TableNodeResponseBuilder::try_from(t)?.build()?))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    // Build relations
    let relations = dep_graph
        .iter()
        .map(|d| {
            let function = functions_map[&d.function_id];
            let table = tables_map[&d.table_id];
            Ok::<_, TdError>((
                function.id,
                table.id,
                GraphEdge::dependency(
                    d.table_versions.clone(),
                    GraphDependency::builder()
                        .dep_pos(d.dep_pos)
                        .self_dependency(function.id == table.function_version_id)
                        .build()?,
                ),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TableGraphResponse {
        table: current_table.id,
        all_functions,
        all_tables,
        system_tables,
        user_tables,
        relations,
    })
}
