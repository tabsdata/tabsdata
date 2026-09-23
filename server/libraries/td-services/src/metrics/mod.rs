//
// Copyright 2026 Tabsdata Inc.
//

//! Operational metrics services — `/inflight`, `/metrics`, `/snapshot`.

#[cfg(test)]
mod behavior;
mod fold;
mod names;
pub mod services;

use crate::metrics::fold::{external_nodes, fold_edges, fold_nodes};
use crate::metrics::names::Names;
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::metrics::{
    InflightRequest, InflightResponse, MetricsRequest, MetricsResponse,
};
use td_objects::types::visible_projects::VisibleProjects;
use td_queries::Queries;

pub(crate) async fn inflight_response<S: SeesProjectRows, M: AccessMode>(
    req: &InflightRequest,
    visible_projects: &VisibleProjects,
    conn: &mut DomainConnection<Apiserver, S, M>,
) -> Result<InflightResponse, TdError> {
    let mut rows = conn.metrics().inflight(req, visible_projects).await?;
    rows.sort_by_key(|r| (r.project_id, r.collection_id, r.group_id, r.status));
    Ok(InflightResponse { rows })
}

pub(crate) async fn metrics_response<S: SeesProjectRows, M: AccessMode>(
    req: &MetricsRequest,
    visible_projects: &VisibleProjects,
    conn: &mut DomainConnection<Apiserver, S, M>,
) -> Result<MetricsResponse, TdError> {
    let rows = conn.metrics().per_table(req, visible_projects).await?;
    let names = Names::of(conn, &rows).await?;
    let edges = fold_edges(req.dim, &rows, &names)?;
    let mut nodes = fold_nodes(req.dim, &rows, &names)?;
    nodes.extend(external_nodes(&edges)?);
    Ok(MetricsResponse {
        dim: req.dim,
        start_ms: req.window.start_ms,
        end_ms: req.window.end_ms,
        nodes,
        edges,
    })
}
