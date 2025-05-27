//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::layers::assert_collection_and_to_collection_are_different;
use std::sync::Arc;
use td_authz::{refresh_authz_context, Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    builder, BuildService, ExtractDataService, ExtractNameService, ExtractService, SetService,
    TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionName, FromCollectionId, InterCollectionPermissionId,
    ToCollectionId, ToCollectionName,
};
use td_objects::types::collection::CollectionDB;
use td_objects::types::permission::{
    InterCollectionPermission, InterCollectionPermissionBuilder, InterCollectionPermissionCreate,
    InterCollectionPermissionDB, InterCollectionPermissionDBBuilder,
    InterCollectionPermissionDBWithNames,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateInterCollectionPermissionService {
    provider: ServiceProvider<
        CreateRequest<CollectionParam, InterCollectionPermissionCreate>,
        InterCollectionPermission,
        TdError,
    >,
}

impl CreateInterCollectionPermissionService {
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

                from_fn(With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract::<RequestContext>),
                from_fn(With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_name::<CollectionParam>),
                from_fn(With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_data::<InterCollectionPermissionCreate>),

                // check requester is sec_admin or coll_admin, early pre-check
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                // find collection ID for the FROM collection
                from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
                from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionId>::convert_to::<FromCollectionId, _>), //stashing the collection ID we are adding the permission

                // check request is sec_admin or coll_admin for the FROM collection
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                // find collection ID for the TO collection
                from_fn(With::<InterCollectionPermissionCreate>::extract::<ToCollectionName>),
                from_fn(With::<ToCollectionName>::convert_to::<CollectionName, _>),
                from_fn(By::<CollectionName>::select::<DaoQueries, CollectionDB>),
                from_fn(With::<CollectionDB>::extract::<CollectionId>),
                from_fn(With::<CollectionId>::convert_to::<ToCollectionId, _>),

                // restore CollectionId from request name as we dropped it get the ToCollectionId
                from_fn(With::<FromCollectionId>::convert_to::<CollectionId, _>),

                from_fn(assert_collection_and_to_collection_are_different),

                // create permission DAO
                from_fn(builder::<InterCollectionPermissionDBBuilder>),
                from_fn(With::<RequestContext>::update::<InterCollectionPermissionDBBuilder, _>),
                from_fn(With::<CollectionId>::set::<InterCollectionPermissionDBBuilder>),
                from_fn(With::<ToCollectionId>::set::<InterCollectionPermissionDBBuilder>),
                from_fn(With::<InterCollectionPermissionDBBuilder>::build::<InterCollectionPermissionDB, _>),

                // insert DAO in DB
                from_fn(insert::<DaoQueries, InterCollectionPermissionDB>),

                // Fetch DAO with Names and create DTO response
                from_fn(With::<InterCollectionPermissionDB>::extract::<InterCollectionPermissionId>),
                from_fn(By::<InterCollectionPermissionId>::select::<DaoQueries, InterCollectionPermissionDBWithNames>),
                from_fn(With::<InterCollectionPermissionDBWithNames>::convert_to::<InterCollectionPermissionBuilder, _>),
                from_fn(With::<InterCollectionPermissionBuilder>::build::<InterCollectionPermission, _>),

                // refresh the inter collections authz cache
                from_fn(refresh_authz_context),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<
        CreateRequest<CollectionParam, InterCollectionPermissionCreate>,
        InterCollectionPermission,
        TdError,
    > {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inter_coll_permission::InterCollectionPermissionError;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_error::{assert_service_error, TdError};
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_inter_collection_permission::get_inter_collection_permissions;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, RoleId, ToCollectionId, UserId,
    };
    use td_objects::types::permission::InterCollectionPermissionCreate;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_create_inter_collection_permission_service(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let provider = CreateInterCollectionPermissionService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>, InterCollectionPermission>(&[
            type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract::<RequestContext>),
            type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_name::<CollectionParam>),
            type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_data::<InterCollectionPermissionCreate>),

            // check requester is sec_admin or coll_admin, early pre-check
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

            // find collection ID for the FROM collection
            type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            type_of_val(&With::<CollectionId>::convert_to::<FromCollectionId, _>), //stashing the collection ID we are adding the permission

            // check request is sec_admin or coll_admin for the FROM collection
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

            // find collection ID for the TO collection
            type_of_val(&With::<InterCollectionPermissionCreate>::extract::<ToCollectionName>),
            type_of_val(&With::<ToCollectionName>::convert_to::<CollectionName, _>),
            type_of_val(&By::<CollectionName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            type_of_val(&With::<CollectionId>::convert_to::<ToCollectionId, _>),

            // restore CollectionId from request name as we dropped it get the ToCollectionId
            type_of_val(&With::<FromCollectionId>::convert_to::<CollectionId, _>),

            type_of_val(&assert_collection_and_to_collection_are_different),

            // create permission DAO
            type_of_val(&builder::<InterCollectionPermissionDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<InterCollectionPermissionDBBuilder, _>),
            type_of_val(&With::<CollectionId>::set::<InterCollectionPermissionDBBuilder>),
            type_of_val(&With::<ToCollectionId>::set::<InterCollectionPermissionDBBuilder>),
            type_of_val(&With::<InterCollectionPermissionDBBuilder>::build::<InterCollectionPermissionDB, _>),
            // insert DAO in DB
            type_of_val(&insert::<DaoQueries, InterCollectionPermissionDB>),

            // Fetch DAO with Names and create DTO response
            type_of_val(&With::<InterCollectionPermissionDB>::extract::<InterCollectionPermissionId>),
            type_of_val(&By::<InterCollectionPermissionId>::select::<DaoQueries, InterCollectionPermissionDBWithNames>),
            type_of_val(&With::<InterCollectionPermissionDBWithNames>::convert_to::<InterCollectionPermissionBuilder, _>),
            type_of_val(&With::<InterCollectionPermissionBuilder>::build::<InterCollectionPermission, _>),

            // refresh the inter collections authz cache
            type_of_val(&refresh_authz_context),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_create_permission_ok(db: DbPool) -> Result<(), TdError> {
        let authz_context = Arc::new(AuthzContext::default());
        let service = CreateInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c1")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .create(
            CollectionParam::builder()
                .collection(CollectionIdName::try_from("c0")?)
                .build()?,
            create,
        );

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let found = get_inter_collection_permissions(&db, c0.id()).await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].from_collection_id(), c0.id());
        assert_eq!(
            found[0].to_collection_id(),
            &ToCollectionId::try_from(c1.id())?
        );
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_create_permission_same_collection_err(db: DbPool) -> Result<(), TdError> {
        let authz_context = Arc::new(AuthzContext::default());
        let service = CreateInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c0")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .create(
            CollectionParam::builder()
                .collection(CollectionIdName::try_from("c0")?)
                .build()?,
            create,
        );

        assert_service_error(service, request, |err| match err {
            InterCollectionPermissionError::CannotGivePermissionToItself => {}
            other => panic!("Expected 'CannotGivePermissionToItself', got {:?}", other),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_create_permission_authz_err(db: DbPool) -> Result<(), TdError> {
        let authz_context = Arc::new(AuthzContext::default());
        let service = CreateInterCollectionPermissionService::new(db.clone(), authz_context)
            .service()
            .await;

        seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c0")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .create(
            CollectionParam::builder()
                .collection(CollectionIdName::try_from("c1")?)
                .build()?,
            create,
        );

        assert_service_error(service, request, |err| match err {
            AuthzError::UnAuthorized(_) => {}
            other => panic!("Expected 'UnAuthorized', got {:?}", other),
        })
        .await;
        Ok(())
    }
}
