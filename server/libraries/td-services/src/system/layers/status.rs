//
// Copyright 2025 Tabsdata Inc.
//

use std::time::Instant;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::system::{ApiStatus, HealthStatus};
use td_tower::extractors::MutRef;

pub async fn database_status(
    mut conn: MutRef<DomainConnection<Apiserver>>,
) -> Result<ApiStatus, TdError> {
    let start = Instant::now();

    let database_status = match conn.get().await {
        Ok(conn) => match conn.ping().await {
            Ok(_) => HealthStatus::OK,
            Err(e) => HealthStatus::DatabaseError(e.to_string()),
        },
        Err(e) => HealthStatus::DatabaseError(e.to_string()),
    };
    let status = ApiStatus::builder()
        .status(database_status)
        .latency_as_nanos(start.elapsed().as_nanos())
        .build()?;

    Ok(status)
}
