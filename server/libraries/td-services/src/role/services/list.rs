//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::try_map_list;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::sql::roles::RoleQueries;
use td_objects::types::role::{Role, RoleBuilder, RoleDBWithNames};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListRoleService {
    provider: ServiceProvider<ListRequest<()>, ListResponse<Role>, TdError>,
}

impl ListRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),

                ConnectionProvider::new(db),
                // from_fn(list::<(), RoleQueries, RoleDBWithNames>),

                from_fn(try_map_list::<(), RoleDBWithNames, RoleBuilder, Role>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ListRequest<()>, ListResponse<Role>, TdError> {
        self.provider.make().await
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use td_objects::crudl::{ListParams, RequestContext};
//     use td_objects::test_utils::seed_user::admin_user;
//     use td_tower::ctx_service::RawOneshot;
//
//     #[tokio::test]
//     async fn test() -> Result<(), TdError> {
//         let db = td_database::test_utils::db().await?;
//         let admin_id = admin_user(&db).await;
//
//         let service = ListRoleService::new(db.clone()).service().await;
//
//         let request = RequestContext::with(&admin_id, "r", true)
//             .await
//             .list((), ListParams::default());
//
//         let response = service.raw_oneshot(request).await;
//         let response = response?;
//         println!("{:?}", response);
//         Ok(())
//     }
// }
