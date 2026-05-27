//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::services::inflight::InflightService;
use crate::metrics::services::metrics::MetricsService;
use ta_services::factory::ServiceFactory;

mod inflight;
mod metrics;

#[derive(ServiceFactory)]
pub struct OperationalMetricsServices {
    pub inflight: InflightService,
    pub metrics: MetricsService,
}
