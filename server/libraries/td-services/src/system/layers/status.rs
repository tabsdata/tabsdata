//
// Copyright 2025 Tabs Data Inc.
//

use std::time::Instant;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::system::{ApiStatus, HealthStatus};
use td_tower::extractors::MutRef;

pub async fn database_status(
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<ApiStatus, TdError> {
    let start = Instant::now();

    let database_status = match conn.ping().await {
        Ok(_) => HealthStatus::OK,
        Err(e) => HealthStatus::DatabaseError(e.to_string()),
    };
    let status = ApiStatus::builder()
        .status(database_status)
        .latency_as_nanos(start.elapsed().as_nanos())
        .build()?;

    Ok(status)
}
