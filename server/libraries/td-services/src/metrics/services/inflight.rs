//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::layers::build_inflight_response;
use ta_services::factory::service_factory;
use td_objects::dxo::metrics::{InflightRequest, InflightResponse};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /inflight` — point-in-time count of in-flight LFRs broken
/// out by `(collection_id, group_id, status)`. Inflight is defined by
/// status, not by `ended_on IS NULL`.
// Tenant-scoped (see `MetricsService`): RLS filters to the request's project.
#[service_factory(
    name = InflightService,
    request = InflightRequest,
    response = InflightResponse,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(from_fn(build_inflight_response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::query_filters::DimensionFilter;
    use td_objects::types::basic::ProjectId;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_inflight_empty_db(db: TdPool) -> Result<(), TdError> {
        let request = InflightRequest {
            project_id: ProjectId::sandbox(),
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = InflightService::with_defaults(db).service();
        let response = service.raw_oneshot(request).await?;
        assert!(response.rows.is_empty());
        Ok(())
    }
}
