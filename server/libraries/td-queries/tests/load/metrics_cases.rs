//
// Copyright 2026 Tabs Data Inc.
//

//! Metrics-family load cases — the heaviest aggregations in the crate.

use std::time::Instant;

use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::metrics::{Dim, InflightRequest, MetricsRequest};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
use td_queries::MetricsEntry;

use crate::dataset::{self, WINDOW_END_MS, WINDOW_START_MS};
use crate::harness::record;
use crate::profile::SizeProfile;

fn metrics_request(dim: Dim) -> MetricsRequest {
    MetricsRequest {
        dim,
        window: TimeWindow::new(WINDOW_START_MS, WINDOW_END_MS),
        unit: WindowUnit::Day,
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    }
}

#[td_test::test(db)]
async fn load_metrics_nodes(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    let seeded = dataset::metrics_dataset(&db, profile.metrics()).await?;

    let req = metrics_request(Dim::Collection);
    let mut conn = db.acquire().await.unwrap();
    let t = Instant::now();
    let rows = conn.metrics().metrics_nodes(&req).await?;
    record(
        "metrics::metrics_nodes",
        &db,
        profile,
        t,
        rows.len(),
        seeded,
    );

    assert!(!rows.is_empty(), "seeded publishers must yield node rows");
    Ok(())
}

#[td_test::test(db)]
async fn load_metrics_edges(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    let seeded = dataset::metrics_dataset(&db, profile.metrics()).await?;

    let req = metrics_request(Dim::Collection);
    let mut conn = db.acquire().await.unwrap();
    let t = Instant::now();
    let rows = conn.metrics().metrics_edges(&req).await?;
    record(
        "metrics::metrics_edges",
        &db,
        profile,
        t,
        rows.len(),
        seeded,
    );

    assert!(
        !rows.is_empty(),
        "publishers yield synthetic edges; consumers yield real edges"
    );
    Ok(())
}

#[td_test::test(db)]
async fn load_metrics_inflight(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    // All seeded runs are `Done`: /inflight returns no rows, but the scan
    // under the status filter is the measured cost.
    let seeded = dataset::metrics_dataset(&db, profile.metrics()).await?;

    let req = InflightRequest {
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    };
    let mut conn = db.acquire().await.unwrap();
    let t = Instant::now();
    let rows = conn.metrics().inflight(&req).await?;
    record("metrics::inflight", &db, profile, t, rows.len(), seeded);
    Ok(())
}
