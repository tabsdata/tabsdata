//
// Copyright 2025. Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::sql::DaoQueries;
use td_objects::types::auth::UserInfo;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::ConnectionProvider;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UserInfoService {
    provider: ServiceProvider<ReadRequest<()>, UserInfo, TdError>,
}

impl UserInfoService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                ConnectionProvider::new(db)
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<()>, UserInfo, TdError> {
        self.provider.make().await
    }
}
