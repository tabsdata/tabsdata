//
// Copyright 2026 Tabsdata Inc.
//

//! Metrics-family load cases — the heaviest aggregations in the crate.

use crate::dataset::{self, WINDOW_END_MS, WINDOW_START_MS};
use crate::harness::record;
use crate::profile::SizeProfile;
use std::time::Instant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::metrics::{Dimension, InflightRequest, MetricsRequest};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow};
use td_objects::types::basic::{
    EpochMs, IncludePrivate, IncludeReplaced, LogicalFunctionRunStatus,
};
use td_objects::types::visible_projects::VisibleProjects;
use td_queries::Queries;
use td_queries::test_utils::derive_metrics_facts;

fn metrics_request(dim: Dimension) -> MetricsRequest {
    MetricsRequest {
        dim,
        window: TimeWindow::new(
            EpochMs::try_from(WINDOW_START_MS).unwrap(),
            EpochMs::try_from(WINDOW_END_MS).unwrap(),
        ),
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: LogicalFunctionRunStatus::ENDED.to_vec(),
        include_replaced: IncludeReplaced::FALSE,
        include_private: IncludePrivate::FALSE,
    }
}

#[td_test::test(db)]
async fn load_metrics_per_table(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    let seeded = dataset::metrics_dataset(&db, profile.metrics()).await?;

    let req = metrics_request(Dimension::Collection);
    let mut conn = db.begin().await.unwrap();
    let t = Instant::now();
    let rows = conn
        .metrics()
        .per_table(&req, &VisibleProjects::All)
        .await?;
    record("metrics::per_table", profile, t, rows.len(), seeded);

    assert!(!rows.is_empty(), "seeded runs must yield fact rows");
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
    let mut conn = db.begin().await.unwrap();
    let t = Instant::now();
    let rows = conn.metrics().inflight(&req, &VisibleProjects::All).await?;
    record("metrics::inflight", profile, t, rows.len(), seeded);
    Ok(())
}

#[td_test::test(db)]
async fn load_metrics_backfill(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    let seeded = dataset::metrics_dataset(&db, profile.metrics()).await?;

    let t = Instant::now();
    derive_metrics_facts(&db).await?;
    record("metrics::backfill", profile, t, 1, seeded);
    Ok(())
}
