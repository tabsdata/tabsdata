//
// Copyright 2025 Tabs Data Inc.
//

use crate::system::layers::status::database_status;
use td_objects::types::system::ApiStatus;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = StatusService,
    request = (),
    response = ApiStatus,
    connection = ConnectionProvider,
)]
fn service() {
    layers!(from_fn(database_status))
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::types::system::HealthStatus;
    use td_tower::td_service::TdService;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_status_service(db: DbPool) {
        use td_tower::metadata::type_of_val;

        StatusService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<(), ApiStatus>(&[type_of_val(&database_status)]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_database_status_service(db: DbPool) {
        let service = StatusService::with_defaults(db).service().await;
        let response = service.oneshot(()).await.unwrap();

        assert!(matches!(response.status(), HealthStatus::OK));
        assert!(*response.latency_as_nanos() > 0);
    }
}
