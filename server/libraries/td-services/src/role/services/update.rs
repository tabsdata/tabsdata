//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::common::layers::sql::select_by_id_or_name;
use crate::common::layers::{build, extract, try_from, update_from};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::types::basic::RoleId;
use td_objects::types::role::{
    Role, RoleBuilder, RoleDBUpdate, RoleDBUpdateBuilder, RoleDBWithNames, RoleParam, RoleUpdate,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UpdateRoleService {
    provider: ServiceProvider<UpdateRequest<RoleParam, RoleUpdate>, Role, TdError>,
}

impl UpdateRoleService {
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
                from_fn(extract_req_context::<UpdateRequest<RoleParam, RoleUpdate>>),
                from_fn(extract_req_name::<UpdateRequest<RoleParam, RoleUpdate>, _>),
                from_fn(extract_req_dto::<UpdateRequest<RoleParam, RoleUpdate>, _>),

                from_fn(try_from::<RoleUpdate, RoleDBUpdateBuilder>),
                from_fn(update_from::<RequestContext, RoleDBUpdateBuilder>),
                from_fn(build::<RoleDBUpdateBuilder, RoleDBUpdate>),

                TransactionProvider::new(db),
                from_fn(select_by_id_or_name::<RoleQueries, RoleParam, _, _, RoleDBWithNames>),
                from_fn(extract::<RoleDBWithNames, RoleId>),
                // from_fn(update_by::<RoleQueries, RoleDBUpdate, RoleDB, RoleId>),

                from_fn(try_from::<RoleDBWithNames, RoleBuilder>),
                from_fn(build::<RoleBuilder, Role>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<RoleParam, RoleUpdate>, Role, TdError> {
        self.provider.make().await
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use td_objects::crudl::RequestContext;
//     use td_objects::test_utils::seed_user::admin_user;
//     use td_tower::ctx_service::RawOneshot;
//
//     #[tokio::test]
//     async fn test() -> Result<(), TdError> {
//         let db = td_database::test_utils::db().await?;
//         let admin_id = admin_user(&db).await;
//
//         let service = UpdateRoleService::new(db.clone()).service().await;
//
//         let update = RoleUpdate::builder()
//             .try_name("sys_admin_X")?
//             .try_description("new desc")?
//             .build()?;
//
//         let request = RequestContext::with(&admin_id, "r", true)
//             .await
//             .update(RoleParam::try_from("sys_admin")?, update);
//
//         let response = service.raw_oneshot(request).await;
//         let _response = response?;
//         Ok(())
//     }
// }
