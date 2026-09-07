//
// Copyright 2025 Tabsdata Inc.
//

use sqlx::Connection;
use std::time::Instant;
use ta_services::factory::service_factory;
use td_database::access::DbScope;
use td_database::access::{GatedConnection, Grant, RO};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::system::ApiStatus;
use td_objects::dxo::system::HealthStatus;

#[service_factory]
async fn status(conn: &mut GatedConnection<RO>, _request: ()) -> Result<ApiStatus, TdError> {
    let conn = conn.get(Grant::asserted(
        "health probe: unauthenticated status endpoint, ping only",
    ));
    database_status(conn).await
}

pub async fn database_status<S: DbScope>(
    conn: &mut DomainConnection<Apiserver, S>,
) -> Result<ApiStatus, TdError> {
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

#[cfg(test)]
mod tests {
    use crate::Services;
    use td_database::pool::TdPool;
    use td_objects::dxo::system::HealthStatus;

    #[td_test::test(db)]
    async fn test_database_status_service(db: TdPool) {
        let service = Services::with_defaults(db);
        let response = service.status(()).await.unwrap();

        assert!(matches!(response.status, HealthStatus::OK));
        assert!(response.latency_as_nanos > 0);
    }
}
