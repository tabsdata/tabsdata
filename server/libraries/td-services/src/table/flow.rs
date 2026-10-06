//
// Copyright 2025 Tabsdata Inc.
//

use itertools::{Either, Itertools};
use std::collections::HashSet;
use td_error::TdError;
use td_execution::graph::{MapGraph, TdGraph};
use td_objects::dxo::execution::{FunctionNodeResponse, TableNodeResponse};
use td_objects::dxo::graph::GraphNode;
use td_objects::dxo::table::{TableDBWithNames, TableFlowGraphResponse};
use td_objects::types::composed::TableVersions;

// similar to execution response
pub async fn build_response(
    table: &TableDBWithNames,
    graph: &TdGraph,
) -> Result<TableFlowGraphResponse, TdError> {
    let graph = graph
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
            |edge, _, _| async {
                let table_versions = TableVersions::new(edge.versions());
                Ok::<_, TdError>(table_versions)
            },
        )
        .await?;

    let relations = graph
        .relations()
        .into_iter()
        .map(|(f, t, e)| (f.function_version_id.clone(), t.table_version_id, e.clone()))
        .sorted()
        .collect();

    let all_functions = graph
        .functions()
        .into_iter()
        .map(|f| (f.function_version_id.clone(), f.clone()))
        .collect();

    let all_tables_set = graph.tables();
    let all_tables = all_tables_set
        .iter()
        .map(|t| (t.table_version_id, (*t).clone()))
        .collect();
    let (system_tables, user_tables): (HashSet<_>, HashSet<_>) =
        all_tables_set.iter().partition_map(|t| {
            if *t.system {
                Either::Left(t.table_version_id)
            } else {
                Either::Right(t.table_version_id)
            }
        });

    let response = TableFlowGraphResponse {
        table: table.base.base.id,
        all_functions,
        all_tables,
        system_tables,
        user_tables,
        relations,
    };
    Ok(response)
}
