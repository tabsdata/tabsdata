//
// Copyright 2026 Tabsdata Inc.
//

use itertools::{Either, Itertools};
use std::collections::BTreeSet;
use td_error::TdError;
use td_execution::graph::TopologyGraph;
use td_objects::dxo::execution::{FunctionNodeResponse, TableNodeResponse};
use td_objects::dxo::graph::{GraphLabels, GraphNode, GraphRelation, GraphResponse};
use td_objects::types::composed::TableVersions;

/// A graph as the API serves it: nodes keyed by version, system tables told
/// apart from user ones, its relations in a stable order, and the labels of
/// the collections and groups it names.
pub async fn graph_response(
    graph: &TopologyGraph,
    labels: GraphLabels,
) -> Result<GraphResponse, TdError> {
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
        .map(|(f, t, e)| GraphRelation {
            function_version_id: f.function_version_id.clone(),
            table_version_id: t.table_version_id,
            edge: e.clone(),
        })
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
    let (system_tables, user_tables): (BTreeSet<_>, BTreeSet<_>) =
        all_tables_set.iter().partition_map(|t| {
            if *t.system {
                Either::Left(t.table_version_id)
            } else {
                Either::Right(t.table_version_id)
            }
        });

    Ok(GraphResponse {
        all_functions,
        all_tables,
        system_tables,
        user_tables,
        relations,
        labels,
    })
}
