//
// Copyright 2025 Tabs Data Inc.
//

use itertools::{Either, Itertools};
use std::collections::HashSet;
use td_error::TdError;
use td_objects::dxo::execution::{FunctionNodeResponseBuilder, TableNodeResponseBuilder};
use td_objects::dxo::graph::GraphNode;
use td_objects::dxo::table::{TableDBWithNames, TableFlowGraphResponse};
use td_objects::graph::{MapGraph, TdGraph};
use td_objects::types::composed::TableVersions;
use td_objects::types::recursion::RecursionOptions;
use td_tower::extractors::Ref;

pub async fn with_recursion_options() -> Result<RecursionOptions, TdError> {
    Ok(RecursionOptions::new(None, None))
}

// similar to execution response
pub async fn build_response(
    table: Ref<TableDBWithNames>,
    graph: Ref<TdGraph>,
) -> Result<TableFlowGraphResponse, TdError> {
    let graph = graph
        .map(
            |node| async move {
                let response = match node {
                    GraphNode::Function(function) => GraphNode::Function(
                        FunctionNodeResponseBuilder::try_from(function)?.build()?,
                    ),
                    GraphNode::Table(table) => {
                        GraphNode::Table(TableNodeResponseBuilder::try_from(table)?.build()?)
                    }
                };
                Ok::<_, TdError>(response)
            },
            |edge, _, _| async {
                let table_versions = TableVersions::new(edge.versions());
                Ok::<_, TdError>(table_versions)
            },
        )
        .await?;

    // Relations info
    let relations = graph
        .relations()
        .into_iter()
        .map(|(f, t, e)| (f.function_version_id.clone(), t.table_version_id, e.clone()))
        .sorted()
        .collect();

    // Functions info
    let all_functions = graph
        .functions()
        .into_iter()
        .map(|f| (f.function_version_id.clone(), f.clone()))
        .collect();

    // Tables info
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
        table: table.id,
        all_functions,
        all_tables,
        system_tables,
        user_tables,
        relations,
    };
    Ok(response)
}
