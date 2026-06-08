//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::layers::build_metrics_response;
use ta_services::factory::service_factory;
use td_objects::dxo::metrics::{MetricsRequest, MetricsResponse};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /metrics` — consolidated operational endpoint. Returns
/// `(nodes, edges)` over a window, parameterised by `dim`
/// (`collection` | `group`). System tables are excluded at the SQL
/// layer (see `td-queries/src/daos/metrics.rs`).
// Tenant-scoped: the request carries the resolved project (ProjectScoped), so
// the connection GUC is set and RLS filters the aggregation to that project —
// the queries carry no project predicate of their own.
#[service_factory(
    name = MetricsService,
    request = MetricsRequest,
    response = MetricsResponse,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(from_fn(build_metrics_response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::metrics::Dim;
    use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
    use td_objects::types::basic::ProjectId;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_metrics_empty_window(db: TdPool) -> Result<(), TdError> {
        let request = MetricsRequest {
            project_id: ProjectId::sandbox(),
            dim: Dim::Collection,
            window: TimeWindow::new(0, 86_400_000),
            unit: WindowUnit::Day,
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = MetricsService::with_defaults(db).service();
        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.dim, Dim::Collection);
        assert_eq!(response.unit, WindowUnit::Day);
        assert_eq!(response.start_ms, 0);
        assert_eq!(response.end_ms, 86_400_000);
        assert!(response.nodes.is_empty());
        assert!(response.edges.is_empty());
        Ok(())
    }
}
