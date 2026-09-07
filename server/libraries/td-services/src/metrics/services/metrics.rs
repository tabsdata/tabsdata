//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::metrics_response;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::metrics::{MetricsRequest, MetricsResponse};
use td_objects::types::visible_projects::VisibleProjects;

/// `GET /metrics` — consolidated operational endpoint. Returns
/// `(nodes, edges)` over a window, parameterised by `dim`
/// (`collection` | `group`). System tables are excluded at the SQL
/// layer (see `td-queries/src/daos/metrics.rs`).
///
/// The connection GUC is set to the request's project and RLS filters the
/// aggregation to it — the queries carry no project predicate of their own.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory]
async fn metrics(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<MetricsRequest>>,
) -> Result<MetricsResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    metrics_response(&param, &VisibleProjects::All, gated_conn.get(grant)).await
}

/// `GET /metrics` (global) — the same nodes+edges aggregation across every
/// project. `DbPool<Global>` bypasses RLS so the query spans all tenants; the
/// result is then narrowed to the projects the requester may view.
#[service_factory]
async fn metrics_global(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ReadRequest<MetricsRequest>,
) -> Result<MetricsResponse, TdError> {
    let (ctx, param) = request.into_parts();
    let (grant, visible_projects) = authz.visible_projects(gated_conn, &ctx).await?;
    metrics_response(&param, &visible_projects, gated_conn.get(grant)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::metrics::Dimension;
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_metrics_empty_window(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = MetricsRequest {
            dim: Dimension::Collection,
            window: TimeWindow::new(0, 86_400_000),
            unit: WindowUnit::Day,
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = Services::with_defaults(db);
        let response = service
            .metrics(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?
            .data;
        assert_eq!(response.dim, Dimension::Collection);
        assert_eq!(response.unit, WindowUnit::Day);
        assert_eq!(response.start_ms, 0);
        assert_eq!(response.end_ms, 86_400_000);
        assert!(response.nodes.is_empty());
        assert!(response.edges.is_empty());
        Ok(())
    }
}
