//
// Copyright 2025 Tabs Data Inc.
//

use std::time::Instant;
use td_error::TdError;
use td_objects::dxo::system::{ApiStatus, HealthStatus};
use td_tower::extractors::{Connection, IntoMutSqlConnection};

pub async fn database_status(Connection(connection): Connection) -> Result<ApiStatus, TdError> {
    let start = Instant::now();

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let database_status = match sqlx::Connection::ping(conn).await {
        Ok(_) => HealthStatus::OK,
        Err(e) => HealthStatus::DatabaseError(e.to_string()),
    };
    let status = ApiStatus::builder()
        .status(database_status)
        .latency_as_nanos(start.elapsed().as_nanos())
        .build()?;

    Ok(status)
}
