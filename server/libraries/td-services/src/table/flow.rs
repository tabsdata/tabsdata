//
// Copyright 2025 Tabsdata Inc.
//

use crate::graph::graph_response;
use td_error::TdError;
use td_execution::graph::TdGraph;
use td_objects::dxo::graph::TableFlowGraphResponse;
use td_objects::dxo::table::TableDBWithNames;

pub async fn build_response(
    table: &TableDBWithNames,
    graph: &TdGraph,
) -> Result<TableFlowGraphResponse, TdError> {
    Ok(TableFlowGraphResponse {
        table: table.base.base.id,
        graph: graph_response(graph).await?,
    })
}
