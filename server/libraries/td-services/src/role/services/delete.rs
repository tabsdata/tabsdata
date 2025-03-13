//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extract;
use crate::common::layers::sql::{delete_by, select_by_id_or_name};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::types::basic::RoleId;
use td_objects::types::role::{RoleDB, RoleParam};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteRoleService {
    provider: ServiceProvider<DeleteRequest<RoleParam>, (), TdError>,
}

impl DeleteRoleService {
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
                from_fn(extract_req_context::<DeleteRequest<RoleParam>>),
                from_fn(extract_req_name::<DeleteRequest<RoleParam>, _>),

                TransactionProvider::new(db),
                from_fn(select_by_id_or_name::<RoleQueries, RoleParam, _, _, RoleDB>),
                from_fn(extract::<RoleDB, RoleId>),
                from_fn(delete_by::<RoleQueries, RoleDB, RoleId>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<RoleParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::role::services::CreateRoleService;
    use crate::role::services::RoleCreate;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::admin_user;
    use td_tower::ctx_service::RawOneshot;

    #[tokio::test]
    async fn test() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        //
        let service = CreateRoleService::new(db.clone()).service().await;

        let create = RoleCreate::builder()
            .try_name("test")?
            .try_description("test")?
            .build()?;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create((), create);

        let response = service.raw_oneshot(request).await;
        let _response = response?;
        //

        let service = DeleteRoleService::new(db.clone()).service().await;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .delete(RoleParam::try_from("test")?);

        let response = service.raw_oneshot(request).await;
        response?;
        Ok(())
    }
}
