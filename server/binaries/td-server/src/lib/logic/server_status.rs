//
// Copyright 2024 Tabs Data Inc.
//

//! Status logic

use getset::Getters;
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use std::time::Instant;
use td_apiforge::apiserver_schema;
use td_database::sql::DbPool;
use td_error::TdError;
use td_tower::default_services::{ConnectionProvider, ServiceEntry, ServiceReturn, Share};
use td_tower::extractors;
use td_tower::extractors::IntoMutSqlConnection;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::ServiceBuilder;

/// API: Status schema.
#[apiserver_schema]
#[derive(Debug, Clone, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct ApiStatus {
    status: HealthStatus,
    latency_as_nanos: u128,
}

impl ApiStatus {
    pub fn new(status: HealthStatus, latency_as_nanos: u128) -> Self {
        ApiStatus {
            status,
            latency_as_nanos,
        }
    }
}

#[apiserver_schema]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    OK,
    DatabaseError(String),
}

/// API: Status logic.
pub struct StatusLogic {
    status_service_provider: ServiceProvider<(), ApiStatus, TdError>,
}

impl StatusLogic {
    /// Creates a new instance of [`StatusLogic`], to be done on per-request basis.
    pub fn new(db: DbPool) -> Self {
        StatusLogic {
            status_service_provider: Self::status_service_provider(db),
        }
    }

    fn status_service_provider<Req: Share, Res: Share>(
        db: DbPool,
    ) -> ServiceProvider<Req, Res, TdError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(database_status))
            .service(ServiceReturn)
            .into_service_provider()
    }

    pub async fn status_service(&self) -> TdBoxService<(), ApiStatus, TdError> {
        self.status_service_provider.make().await
    }
}

async fn database_status(
    extractors::Connection(connection): extractors::Connection,
) -> Result<ApiStatus, TdError> {
    let start = Instant::now();

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let database_status = match conn.ping().await {
        Ok(_) => HealthStatus::OK,
        Err(e) => HealthStatus::DatabaseError(e.to_string()),
    };
    let status = ApiStatus::new(database_status, start.elapsed().as_nanos());

    Ok(status)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use td_tower::extractors::ConnectionType;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_database_status_service() {
        let db = td_database::test_utils::db().await.unwrap();
        let status_logic = StatusLogic::new(db);

        let service = status_logic.status_service().await;
        let response = service.oneshot(()).await.unwrap();

        assert!(matches!(response.status, HealthStatus::OK));
        assert!(response.latency_as_nanos > 0);
    }

    #[tokio::test]
    async fn test_database_status_fn() {
        let db = td_database::test_utils::db().await.unwrap();
        let connection = db.acquire().await.unwrap();
        let connection = ConnectionType::PoolConnection(connection).into();
        let connection = extractors::Connection::new(connection);

        let response = database_status(connection).await.unwrap();

        assert!(matches!(response.status, HealthStatus::OK));
        assert!(response.latency_as_nanos > 0);
    }

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_status_service() {
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let provider = StatusLogic::status_service_provider(db);

        let service = provider.make().await;
        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<(), ApiStatus>(&[type_of_val(&database_status)]);
    }
}
