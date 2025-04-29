//
// Copyright 2025. Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin};
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{ExtractService, TryMapListService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::permission::{
    InterCollectionPermission, InterCollectionPermissionBuilder,
    InterCollectionPermissionDBWithNames,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListInterCollectionPermissionService {
    provider: ServiceProvider<
        ListRequest<CollectionParam>,
        ListResponse<InterCollectionPermission>,
        TdError,
    >,
}

impl ListInterCollectionPermissionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                TransactionProvider::new(db),
                SrvCtxProvider::new(authz_context),

                from_fn(extract_req_context::<ListRequest<CollectionParam>>),
                from_fn(extract_req_name::<ListRequest<CollectionParam>, _>),

                // find collection ID
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),

                // check requester is sec_admin or coll_admin for the collection
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                // get list of permissions
                from_fn(By::<CollectionId>::list::<CollectionParam, DaoQueries, InterCollectionPermissionDBWithNames>),

                // map DAOs to DTOs
                from_fn(With::<InterCollectionPermissionDBWithNames>::try_map_list::<CollectionParam, InterCollectionPermissionBuilder, InterCollectionPermission, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<CollectionParam>, ListResponse<InterCollectionPermission>, TdError>
    {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::inter_coll_permission::services::list::ListInterCollectionPermissionService;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_error::{assert_service_error, TdError};
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection2::seed_collection;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, RoleId, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_list_inter_collection_permission_service() {
        use crate::inter_coll_permission::services::list::ListInterCollectionPermissionService;
        use std::sync::Arc;
        use td_authz::Authz;
        use td_authz::AuthzContext;
        use td_objects::crudl::{ListRequest, ListResponse};
        use td_objects::rest_urls::CollectionParam;
        use td_objects::sql::DaoQueries;
        use td_objects::tower_service::authz::CollAdmin;
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin};
        use td_objects::tower_service::extractor::extract_req_context;
        use td_objects::tower_service::extractor::extract_req_name;
        use td_objects::tower_service::from::TryMapListService;
        use td_objects::tower_service::from::{ExtractService, With};
        use td_objects::tower_service::sql::By;
        use td_objects::tower_service::sql::SqlListService;
        use td_objects::tower_service::sql::SqlSelectIdOrNameService;
        use td_objects::types::basic::CollectionId;
        use td_objects::types::basic::CollectionIdName;
        use td_objects::types::collection::CollectionDB;
        use td_objects::types::permission::{
            InterCollectionPermission, InterCollectionPermissionBuilder,
            InterCollectionPermissionDBWithNames,
        };
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let provider = ListInterCollectionPermissionService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<CollectionParam>, ListResponse<InterCollectionPermission>>(&[
            type_of_val(&extract_req_context::<ListRequest<CollectionParam>>),
            type_of_val(&extract_req_name::<ListRequest<CollectionParam>, _>),

            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),

            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

            type_of_val(&By::<CollectionId>::list::<CollectionParam, DaoQueries, InterCollectionPermissionDBWithNames>),

            type_of_val(&With::<InterCollectionPermissionDBWithNames>::try_map_list::<CollectionParam, InterCollectionPermissionBuilder, InterCollectionPermission, _>),
        ]);
    }

    #[tokio::test]
    async fn test_list_permission_ok() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let authz_context = Arc::new(AuthzContext::default());
        let service = ListInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        seed_inter_collection_permission(&db, c0.id(), &(**c1.id()).into()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .list(
            CollectionParam::builder()
                .collection(CollectionIdName::try_from("c0")?)
                .build()?,
            ListParams::default(),
        );

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let response = response?;
        assert_eq!(response.data().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_permission_authz_err() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let authz_context = Arc::new(AuthzContext::default());
        let service = ListInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list(
            CollectionParam::builder()
                .collection(CollectionIdName::try_from("c1")?)
                .build()?,
            ListParams::default(),
        );

        assert_service_error(service, request, |err| match err {
            AuthzError::UnAuthorized(_) => {}
            other => panic!("Expected 'UnAuthorized', got {:?}", other),
        })
        .await;
        Ok(())
    }
}
