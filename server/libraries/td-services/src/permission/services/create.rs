//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use crate::permission::layers::PermissionBuildService;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::permission::PermissionQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    BuildService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::SqlSelectIdOrNameService;
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::{PermissionId, RoleIdName};
use td_objects::types::permission::{
    Permission, PermissionBuilder, PermissionCreate, PermissionDB, PermissionDBBuilder,
    PermissionDBWithNames,
};
use td_objects::types::role::RoleDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreatePermissionService {
    provider: ServiceProvider<CreateRequest<RoleParam, PermissionCreate>, Permission, TdError>,
}

impl CreatePermissionService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(PermissionQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<PermissionQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_context::<CreateRequest<RoleParam, PermissionCreate>>),
                from_fn(extract_req_dto::<CreateRequest<RoleParam, PermissionCreate>, _>),
                from_fn(extract_req_name::<CreateRequest<RoleParam, PermissionCreate>, _>),

                TransactionProvider::new(db),
                from_fn(With::<PermissionCreate>::convert_to::<PermissionDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<PermissionDBBuilder, _>),

                from_fn(With::<RoleParam>::extract::<RoleIdName>),
                from_fn(By::<RoleIdName>::select::<PermissionQueries, RoleDB>),
                from_fn(With::<RoleDB>::update::<PermissionDBBuilder, _>),

                from_fn(With::<PermissionDBBuilder>::build_permission_db),

                from_fn(insert::<PermissionQueries, PermissionDB>),
                from_fn(With::<PermissionDB>::extract::<PermissionId>),
                from_fn(By::<PermissionId>::select::<PermissionQueries, PermissionDBWithNames>),
                from_fn(With::<PermissionDBWithNames>::convert_to::<PermissionBuilder, _>),
                from_fn(With::<PermissionBuilder>::build::<Permission, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<RoleParam, PermissionCreate>, Permission, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_permission::get_permission;
    use td_objects::test_utils::seed_user::admin_user;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_permission() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(PermissionQueries::new());
        let provider = CreatePermissionService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<RoleParam, PermissionCreate>, Permission>(&[
            type_of_val(&extract_req_context::<CreateRequest<RoleParam, PermissionCreate>>),
            type_of_val(&extract_req_dto::<CreateRequest<RoleParam, PermissionCreate>, _>),
            type_of_val(&extract_req_name::<CreateRequest<RoleParam, PermissionCreate>, _>),
            type_of_val(&With::<PermissionCreate>::convert_to::<PermissionDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<PermissionDBBuilder, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<PermissionQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::update::<PermissionDBBuilder, _>),
            type_of_val(&With::<PermissionDBBuilder>::build_permission_db),
            type_of_val(&insert::<PermissionQueries, PermissionDB>),
            type_of_val(&With::<PermissionDB>::extract::<PermissionId>),
            type_of_val(&By::<PermissionId>::select::<PermissionQueries, PermissionDBWithNames>),
            type_of_val(&With::<PermissionDBWithNames>::convert_to::<PermissionBuilder, _>),
            type_of_val(&With::<PermissionBuilder>::build::<Permission, _>),
        ]);
    }

    #[tokio::test]
    async fn test_create_permission() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let create = PermissionCreate::builder()
            .try_permission_type("sa")?
            .try_entity_name(None)
            .unwrap()
            .build()?;

        let request = RequestContext::with(&admin_id, "r", true).await.create(
            RoleParam::builder()
                .role(RoleIdName::try_from("sys_admin")?)
                .build()?,
            create,
        );

        let service = CreatePermissionService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_permission(&db, response.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.permission_type(), found.permission_type());
        assert_eq!(response.entity_type(), found.entity_type());
        assert_eq!(response.entity_id(), found.entity_id());
        assert_eq!(response.granted_by_id(), found.granted_by_id());
        assert_eq!(response.granted_on(), found.granted_on());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
