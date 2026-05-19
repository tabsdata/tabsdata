//
// Copyright 2025. Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin};
use td_authz::{Authz, AuthzContext};
use td_objects::NoFilter;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::inter_collection_permission::InterCollectionPermission;
use td_objects::list::params::ListQueryParams;
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListInterCollectionPermissionService,
    request = ListRequest<CollectionParam>,
    response = ListResponse<InterCollectionPermission>,
    connection = TransactionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>),
        // find collection ID
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        // check requester is sec_admin or coll_admin for the collection
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // get list of permissions
        from_fn(With::<ListRequest<CollectionParam>>::extract::<ListParams>),
        from_fn(
            With::<ListParams>::try_convert_to::<ListQueryParams<InterCollectionPermission>, _>
        ),
        from_fn(By::<CollectionId>::list::<NoFilter, InterCollectionPermission>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_authz::tower::AuthzError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_inter_collection_permission(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ListInterCollectionPermissionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<CollectionParam>, ListResponse<InterCollectionPermission>>(&[
                type_of_val(&With::<ListRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&With::<ListRequest<CollectionParam>>::extract_name::<CollectionParam>),

                // find collection ID
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),

                // check requester is sec_admin or coll_admin for the collection
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

                // get list of permissions
                type_of_val(&With::<ListRequest<CollectionParam>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<InterCollectionPermission>, _>),
                type_of_val(&By::<CollectionId>::list::<NoFilter, InterCollectionPermission>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_permission_ok(db: TdPool) -> Result<(), TdError> {
        let service = ListInterCollectionPermissionService::with_defaults(db.clone()).service();

        let c0 = db
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = db
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        db.seed()
            .inter_collection_permission(&c0.id, &(*c1.id).into())
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .list(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str("c0")?)
                .build()?,
            ListParams::default(),
        );

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let response = response?;
        assert_eq!(response.data.len(), 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_permission_authz_err(db: TdPool) -> Result<(), TdError> {
        let service = ListInterCollectionPermissionService::with_defaults(db.clone()).service();

        db.seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        db.seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).list(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str("c1")?)
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
