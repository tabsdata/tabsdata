//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::layers::{build_snapshot_response, build_snapshot_response_global};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject, Visible};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::metrics::{SnapshotRequest, SnapshotResponse};
use td_objects::tower_service::with::With;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /snapshot` — combined `/metrics` + `/inflight` on a single
/// pool connection. Sub-responses keep their existing shapes.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory(
    name = SnapshotService,
    request = ReadRequest<SnapshotRequest>,
    response = SnapshotResponse,
    connection = ConnectionProvider,
    // Tenant-scoped like /metrics and /inflight: the URL's {project} drives the
    // GUC and RLS scopes the aggregation.
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<SnapshotRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<SnapshotRequest>>::extract_name::<SnapshotRequest>),
        // Authorize: the requester must be able to view the project in scope.
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(build_snapshot_response),
    )
}

/// `GET /snapshot` (global) — combined `/metrics` + `/inflight` across every
/// project on one connection. `scope = "Global"` bypasses RLS so the query
/// spans all tenants; the result is then narrowed to the projects the requester
/// may view (`Visible::projects` + the `*_global` builder).
#[service_factory(
    name = SnapshotGlobalService,
    request = ReadRequest<SnapshotRequest>,
    response = SnapshotResponse,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service_global() {
    layers!(
        from_fn(With::<ReadRequest<SnapshotRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<SnapshotRequest>>::extract_name::<SnapshotRequest>),
        // Narrow the cross-project aggregation to the requester's viewable projects.
        from_fn(Visible::projects),
        from_fn(build_snapshot_response_global),
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
    use td_objects::dxo::metrics::{Dim, InflightRequest, MetricsRequest};
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::{AccessTokenId, UserId};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_snapshot_empty_window(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = SnapshotRequest {
            metrics: MetricsRequest {
                dim: Dim::Collection,
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
        let service = SnapshotService::with_defaults(db).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?;
        assert_eq!(response.metrics.dim, Dim::Collection);
        assert_eq!(response.metrics.unit, WindowUnit::Day);
        assert!(response.metrics.nodes.is_empty());
        assert!(response.metrics.edges.is_empty());
        assert!(response.inflight.rows.is_empty());
        Ok(())
    }
}
