//
// Copyright 2025 Tabsdata Inc.
//

use sqlx::Connection;
use std::time::Instant;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::system::{ApiStatus, HealthStatus};

pub async fn database_status(conn: &mut DomainConnection<Apiserver>) -> Result<ApiStatus, TdError> {
    let start = Instant::now();

    let database_status = match conn.ping().await {
        Ok(_) => HealthStatus::OK,
        Err(e) => HealthStatus::DatabaseError(e.to_string()),
    };
    let status = ApiStatus {
        status: database_status,
        latency_as_nanos: start.elapsed().as_nanos(),
    };

    Ok(status)
}
