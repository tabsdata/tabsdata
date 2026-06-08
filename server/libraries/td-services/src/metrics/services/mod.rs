//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::services::inflight::InflightService;
use crate::metrics::services::metrics::MetricsService;
use crate::metrics::services::snapshot::SnapshotService;
use ta_services::factory::ServiceFactory;

mod inflight;
mod metrics;
mod snapshot;

#[derive(ServiceFactory)]
pub struct OperationalMetricsServices {
    pub inflight: InflightService,
    pub metrics: MetricsService,
    pub snapshot: SnapshotService,
}
