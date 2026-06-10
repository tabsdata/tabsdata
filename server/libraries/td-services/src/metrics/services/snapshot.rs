//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::layers::build_snapshot_response;
use ta_services::factory::service_factory;
use td_objects::dxo::metrics::{SnapshotRequest, SnapshotResponse};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /snapshot` — combined `/metrics` + `/inflight` on a single
/// pool connection. Sub-responses keep their existing shapes.
#[service_factory(
    name = SnapshotService,
    request = SnapshotRequest,
    response = SnapshotResponse,
    connection = ConnectionProvider,
    // Tenant-scoped like /metrics and /inflight: the URL's {project} drives the
    // GUC and RLS scopes the aggregation.
    scope = "Project",
)]
fn service() {
    layers!(from_fn(build_snapshot_response))
}

/// `GET /snapshot` (global) — combined `/metrics` + `/inflight` across every
/// project on one connection. `scope = "Global"` bypasses RLS so the query
/// spans all tenants; the inner layer is identical (scope-agnostic).
#[service_factory(
    name = SnapshotGlobalService,
    request = SnapshotRequest,
    response = SnapshotResponse,
    connection = ConnectionProvider,
    scope = "Global",
)]
fn service_global() {
    layers!(from_fn(build_snapshot_response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::dxo::metrics::{Dim, InflightRequest, MetricsRequest};
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::ProjectId;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_snapshot_empty_window(db: TdPool) -> Result<(), TdError> {
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
            .raw_oneshot(request.in_project(ProjectId::sandbox()))
            .await?;
        assert_eq!(response.metrics.dim, Dim::Collection);
        assert_eq!(response.metrics.unit, WindowUnit::Day);
        assert!(response.metrics.nodes.is_empty());
        assert!(response.metrics.edges.is_empty());
        assert!(response.inflight.rows.is_empty());
        Ok(())
    }
}
