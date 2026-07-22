//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::layers::{build_metrics_response, build_metrics_response_global};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject, Visible};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::metrics::{MetricsRequest, MetricsResponse};
use td_objects::tower_service::with::With;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /metrics` — consolidated operational endpoint. Returns
/// `(nodes, edges)` over a window, parameterised by `dim`
/// (`collection` | `group`). System tables are excluded at the SQL
/// layer (see `td-queries/src/daos/metrics.rs`).
///
/// The connection GUC is set to the request's project and RLS filters the
/// aggregation to it — the queries carry no project predicate of their own.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory(
    name = MetricsService,
    request = ReadRequest<MetricsRequest>,
    response = MetricsResponse,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<MetricsRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<MetricsRequest>>::extract_name::<MetricsRequest>),
        // Authorize: the requester must be able to view the project in scope.
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(build_metrics_response),
    )
}

/// `GET /metrics` (global) — the same nodes+edges aggregation across every
/// project. `scope = "Global"` bypasses RLS so the query spans all tenants;
/// the result is then narrowed to the projects the requester may view
/// (`Visible::projects` + the `*_global` builder).
#[service_factory(
    name = MetricsGlobalService,
    request = ReadRequest<MetricsRequest>,
    response = MetricsResponse,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service_global() {
    layers!(
        from_fn(With::<ReadRequest<MetricsRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<MetricsRequest>>::extract_name::<MetricsRequest>),
        // Narrow the cross-project aggregation to the requester's viewable projects.
        from_fn(Visible::projects),
        from_fn(build_metrics_response_global),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::metrics::Dim;
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_metrics_empty_window(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = MetricsRequest {
            dim: Dim::Collection,
            window: TimeWindow::new(0, 86_400_000),
            unit: WindowUnit::Day,
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = MetricsService::with_defaults(db).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?;
        assert_eq!(response.dim, Dim::Collection);
        assert_eq!(response.unit, WindowUnit::Day);
        assert_eq!(response.start_ms, 0);
        assert_eq!(response.end_ms, 86_400_000);
        assert!(response.nodes.is_empty());
        assert!(response.edges.is_empty());
        Ok(())
    }
}
