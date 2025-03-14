//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::extract_req_context;
use td_objects::tower_service::from::{
    BuildService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
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

                from_fn(With::<RoleCreate>::convert_to::<RoleDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<RoleDBBuilder, _>),
                from_fn(With::<RoleDBBuilder>::build::<RoleDB, _>),

                TransactionProvider::new(db),
                from_fn(insert::<RoleQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(By::<RoleId>::select::<RoleQueries, RoleDBWithNames>),
                from_fn(With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
                from_fn(With::<RoleBuilder>::build::<Role, _>),
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
    use td_objects::test_utils::seed_role::get_role;
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::RoleName;
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
            type_of_val(&With::<RoleCreate>::convert_to::<RoleDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<RoleDBBuilder, _>),
            type_of_val(&With::<RoleDBBuilder>::build::<RoleDB, _>),
            type_of_val(&insert::<RoleQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::select::<RoleQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
            type_of_val(&With::<RoleBuilder>::build::<Role, _>),
        ]);
    }

    #[tokio::test]
    async fn test_create_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let create = RoleCreate::builder()
            .try_name("test")?
            .try_description("test desc")?
            .build()?;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create((), create);

        let service = CreateRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_role(&db, &RoleName::try_from("test")?).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.name(), found.name());
        assert_eq!(response.description(), found.description());
        assert_eq!(response.created_on(), found.created_on());
        assert_eq!(response.created_by_id(), found.created_by_id());
        assert_eq!(response.modified_on(), found.modified_on());
        assert_eq!(response.modified_by_id(), found.modified_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
