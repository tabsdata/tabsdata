//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::CollectionDB;
use td_objects::types::permission::InterCollectionPermission;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListInterCollectionPermissionService,
    request = ListRequest<CollectionParam>,
    response = ListResponse<InterCollectionPermission>,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>),
        // find collection ID
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        // check requester is sec_admin or coll_admin for the collection
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // get list of permissions
        from_fn(
            By::<CollectionId>::list::<CollectionParam, NoListFilter, InterCollectionPermission>
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, RoleId, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_inter_collection_permission_service(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ListInterCollectionPermissionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<CollectionParam>, ListResponse<InterCollectionPermission>>(&[
                type_of_val(&With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>),

                // find collection ID
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),

                // check requester is sec_admin or coll_admin for the collection
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

                // get list of permissions
                type_of_val(&By::<CollectionId>::list::<CollectionParam, NoListFilter, InterCollectionPermission>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_permission_ok(db: DbPool) -> Result<(), TdError> {
        let service = ListInterCollectionPermissionService::with_defaults(db.clone())
            .service()
            .await;

        let c0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let c1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        seed_inter_collection_permission(&db, c0.id(), &(**c1.id()).into()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
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

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_permission_authz_err(db: DbPool) -> Result<(), TdError> {
        let service = ListInterCollectionPermissionService::with_defaults(db.clone())
            .service()
            .await;

        seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionParam::builder()
                    .collection(CollectionIdName::try_from("c1")?)
                    .build()?,
                ListParams::default(),
            );

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
