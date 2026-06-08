//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::services::inflight::{InflightGlobalService, InflightService};
use crate::metrics::services::metrics::{MetricsGlobalService, MetricsService};
use crate::metrics::services::snapshot::{SnapshotGlobalService, SnapshotService};
use ta_services::factory::ServiceFactory;

mod inflight;
mod metrics;
mod snapshot;

#[derive(ServiceFactory)]
pub struct OperationalMetricsServices {
    pub inflight: InflightService,
    pub metrics: MetricsService,
    pub snapshot: SnapshotService,
    pub inflight_global: InflightGlobalService,
    pub metrics_global: MetricsGlobalService,
    pub snapshot_global: SnapshotGlobalService,
}
