//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::{inflight_response, metrics_response};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::metrics::{SnapshotRequest, SnapshotResponse};
use td_objects::types::visible_projects::VisibleProjects;

/// `GET /snapshot` — combined `/metrics` + `/inflight` on a single
/// pool connection. Sub-responses keep their existing shapes.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory]
async fn snapshot(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<SnapshotRequest>>,
) -> Result<SnapshotResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    let conn = gated_conn.get(grant);
    let metrics = metrics_response(&param.metrics, &VisibleProjects::All, conn).await?;
    let inflight = inflight_response(&param.inflight, &VisibleProjects::All, conn).await?;
    Ok(SnapshotResponse { metrics, inflight })
}

/// `GET /snapshot` (global) — combined `/metrics` + `/inflight` across every
/// project on one connection. `DbPool<Global>` bypasses RLS so the query spans
/// all tenants; the result is then narrowed to the projects the requester
/// may view.
#[service_factory]
async fn snapshot_global(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ReadRequest<SnapshotRequest>,
) -> Result<SnapshotResponse, TdError> {
    let (ctx, param) = request.into_parts();
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    let conn = gated_conn.get(grant);
    let metrics = metrics_response(&param.metrics, &visible, conn).await?;
    let inflight = inflight_response(&param.inflight, &visible, conn).await?;
    Ok(SnapshotResponse { metrics, inflight })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::metrics::{Dimension, InflightRequest, MetricsRequest};
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_snapshot_empty_window(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = SnapshotRequest {
            metrics: MetricsRequest {
                dim: Dimension::Collection,
                window: TimeWindow::new(0, 86_400_000),
                unit: WindowUnit::Day,
                collections: DimensionFilter::default(),
                groups: DimensionFilter::default(),
                statuses: vec![],
            },
            inflight: InflightRequest {
                collections: DimensionFilter::default(),
                groups: DimensionFilter::default(),
                statuses: vec![],
            },
        };
        let service = Services::with_defaults(db);
        let response = service
            .snapshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?
            .data;
        assert_eq!(response.metrics.dim, Dimension::Collection);
        assert_eq!(response.metrics.unit, WindowUnit::Day);
        assert!(response.metrics.nodes.is_empty());
        assert!(response.metrics.edges.is_empty());
        assert!(response.inflight.rows.is_empty());
        Ok(())
    }
}
