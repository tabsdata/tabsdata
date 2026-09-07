//
// Copyright 2025 Tabsdata Inc.
//

use crate::system::status::database_status;
use ta_services::factory::service_factory;
use td_database::access::{GatedConnection, Grant, RO};
use td_error::TdError;
use td_objects::dxo::system::ApiStatus;

#[service_factory]
async fn status(conn: &mut GatedConnection<RO>, _request: ()) -> Result<ApiStatus, TdError> {
    let conn = conn.get(Grant::asserted(
        "health probe: unauthenticated status endpoint, ping only",
    ));
    database_status(conn).await
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
