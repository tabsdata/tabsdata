//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::common::layers::sql::{insert, select_by};
use crate::common::layers::{build, extract, try_from, update_from};
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::types::basic::RoleId;
use td_objects::types::role::{
    Role, RoleBuilder, RoleCreate, RoleDB, RoleDBBuilder, RoleDBWithNames,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateRoleService {
    provider: ServiceProvider<CreateRequest<(), RoleCreate>, Role, TdError>,
}

impl CreateRoleService {
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
                from_fn(extract_req_context::<CreateRequest<(), RoleCreate>>),
                from_fn(extract_req_dto::<CreateRequest<(), RoleCreate>, _>),

                from_fn(try_from::<RoleCreate, RoleDBBuilder>),
                from_fn(update_from::<RequestContext, RoleDBBuilder>),
                from_fn(build::<RoleDBBuilder, RoleDB>),

                TransactionProvider::new(db),
                from_fn(insert::<RoleQueries, RoleDB>),
                from_fn(extract::<RoleDB, RoleId>),
                from_fn(select_by::<RoleQueries, RoleDBWithNames, RoleId>),
                from_fn(try_from::<RoleDBWithNames, RoleBuilder>),
                from_fn(build::<RoleBuilder, Role>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<CreateRequest<(), RoleCreate>, Role, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_role() {
        use td_tower::metadata::{type_of_val, Metadata};
        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = CreateRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<(), RoleCreate>, Role>(&[
            type_of_val(&extract_req_context::<CreateRequest<(), RoleCreate>>),
            type_of_val(&extract_req_dto::<CreateRequest<(), RoleCreate>, _>),
            type_of_val(&try_from::<RoleCreate, RoleDBBuilder>),
            type_of_val(&update_from::<RequestContext, RoleDBBuilder>),
            type_of_val(&build::<RoleDBBuilder, RoleDB>),
            type_of_val(&insert::<RoleQueries, RoleDB>),
            type_of_val(&extract::<RoleDB, RoleId>),
            type_of_val(&select_by::<RoleQueries, RoleDBWithNames, RoleId>),
            type_of_val(&try_from::<RoleDBWithNames, RoleBuilder>),
            type_of_val(&build::<RoleBuilder, Role>),
        ]);
    }

    #[tokio::test]
    async fn test() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let service = CreateRoleService::new(db.clone()).service().await;
        let create = RoleCreate::builder()
            .try_name("test")?
            .try_description("test desc")?
            .build()?;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create((), create);

        let response = service.raw_oneshot(request).await;
        let response = response?;
        assert_eq!(*response.name(), RoleName::try_from("test")?);
        assert_eq!(*response.description(), Description::try_from("test desc")?);
        Ok(())
    }
}
