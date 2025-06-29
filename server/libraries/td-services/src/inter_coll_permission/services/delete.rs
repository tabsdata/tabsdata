//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::layers::assert_collection_in_permission;
use std::sync::Arc;
use td_authz::{refresh_authz_context, Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::InterCollectionPermissionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, InterCollectionPermissionId, InterCollectionPermissionIdName,
};
use td_objects::types::permission::{
    InterCollectionPermissionDB, InterCollectionPermissionDBWithNames,
};
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct DeleteInterCollectionPermissionService {
    provider: ServiceProvider<DeleteRequest<InterCollectionPermissionParam>, (), TdError>,
}

impl DeleteInterCollectionPermissionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                TransactionProvider::new(db),
                SrvCtxProvider::new(authz_context),

                from_fn(With::<DeleteRequest<InterCollectionPermissionParam>>::extract::<RequestContext>),
                from_fn(With::<DeleteRequest<InterCollectionPermissionParam>>::extract_name::<InterCollectionPermissionParam>),

                // check requester is sec_admin or coll_admin, early pre-check
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                // find permission
                from_fn(With::<InterCollectionPermissionParam>::extract::<InterCollectionPermissionIdName>),
                from_fn(By::<InterCollectionPermissionIdName>::select::<DaoQueries, InterCollectionPermissionDBWithNames>),

                // Check the role in the request matches the role in permission
                from_fn(With::<InterCollectionPermissionParam>::extract::<CollectionIdName>),
                from_fn(assert_collection_in_permission),

                // check the request is sec_admin or coll_admin for the collection in the permission
                from_fn(With::<InterCollectionPermissionDBWithNames>::extract::<CollectionId>),
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                // delete permission from DB
                from_fn(With::<InterCollectionPermissionDBWithNames>::extract::<InterCollectionPermissionId>),
                from_fn(By::<InterCollectionPermissionId>::delete::<DaoQueries, InterCollectionPermissionDB>),

                // refresh the inter collections authz cache
                from_fn(refresh_authz_context),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<DeleteRequest<InterCollectionPermissionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_error::{assert_service_error, TdError};
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::InterCollectionPermissionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_inter_collection_permission::{
        get_inter_collection_permissions, seed_inter_collection_permission,
    };
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, InterCollectionPermissionIdName, RoleId,
        UserId,
    };
    use td_objects::types::IdOrName;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_inter_collection_permission_service(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let provider = DeleteInterCollectionPermissionService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<InterCollectionPermissionParam>, ()>(&[
            type_of_val(&With::<DeleteRequest<InterCollectionPermissionParam>>::extract::<RequestContext>),
            type_of_val(&With::<DeleteRequest<InterCollectionPermissionParam>>::extract_name::<InterCollectionPermissionParam>),

            // check requester is sec_admin or coll_admin, early pre-check
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

            // find permission
            type_of_val(&With::<InterCollectionPermissionParam>::extract::<InterCollectionPermissionIdName>),
            type_of_val(&By::<InterCollectionPermissionIdName>::select::<DaoQueries, InterCollectionPermissionDBWithNames>),

            // Check the role in the request matches the role in permission
            type_of_val(&With::<InterCollectionPermissionParam>::extract::<CollectionIdName>),
            type_of_val(&assert_collection_in_permission),

            // check the request is sec_admin or coll_admin for the collection in the permission
            type_of_val(&With::<InterCollectionPermissionDBWithNames>::extract::<CollectionId>),
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

            // delete permission from DB
            type_of_val(&With::<InterCollectionPermissionDBWithNames>::extract::<InterCollectionPermissionId>),
            type_of_val(&By::<InterCollectionPermissionId>::delete::<DaoQueries, InterCollectionPermissionDB>),

            // refresh the inter collections authz cache
            type_of_val(&refresh_authz_context),
        ]);
    }

    #[tokio::test]
    async fn test_delete_permission_ok() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let authz_context = Arc::new(AuthzContext::default());
        let service = DeleteInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        let p = seed_inter_collection_permission(&db, c0.id(), &(**c1.id()).into()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .delete(
            InterCollectionPermissionParam::builder()
                .collection(CollectionIdName::try_from("c0")?)
                .permission(InterCollectionPermissionIdName::from_id(p.id()))
                .build()?,
        );

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let permissions = get_inter_collection_permissions(&db, c0.id()).await?;
        assert_eq!(permissions.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_permission_unauthz_err() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let authz_context = Arc::new(AuthzContext::default());
        let service = DeleteInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        let p = seed_inter_collection_permission(&db, c0.id(), &(**c1.id()).into()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            InterCollectionPermissionParam::builder()
                .collection(CollectionIdName::try_from("c0")?)
                .permission(InterCollectionPermissionIdName::from_id(p.id()))
                .build()?,
        );

        assert_service_error(service, request, |err| match err {
            AuthzError::UnAuthorized(_) => {}
            other => panic!("Expected 'UnAuthorized', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
