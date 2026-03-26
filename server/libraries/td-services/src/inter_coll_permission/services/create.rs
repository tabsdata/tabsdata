//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::layers::assert_collection_and_to_collection_are_different;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext, refresh_authz_context};
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::inter_collection_permission::{
    InterCollectionPermission, InterCollectionPermissionBuilder, InterCollectionPermissionCreate,
    InterCollectionPermissionDB, InterCollectionPermissionDBBuilder,
    InterCollectionPermissionDBWithNames,
};
use td_objects::rest_urls::CollectionParam;
use td_objects::tower_service::with::{With, builder};
use td_objects::types::basic::{
    CollectionId, CollectionIdName, CollectionName, FromCollectionId, InterCollectionPermissionId,
    ToCollectionId, ToCollectionName,
};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateInterCollectionPermissionService,
    request = CreateRequest<CollectionParam, InterCollectionPermissionCreate>,
    response = InterCollectionPermission,
    connection = TransactionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(
            With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract::<
                RequestContext,
            >
        ),
        from_fn(
            With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_name::<
                CollectionParam,
            >
        ),
        from_fn(
            With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_data::<
                InterCollectionPermissionCreate,
            >
        ),
        // check requester is sec_admin or coll_admin, early pre-check
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // find collection ID for the FROM collection
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(With::<CollectionId>::try_convert_to::<FromCollectionId, _>), //stashing the collection ID we are adding the permission
        // check request is sec_admin or coll_admin for the FROM collection
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // find collection ID for the TO collection
        from_fn(With::<InterCollectionPermissionCreate>::extract::<ToCollectionName>),
        from_fn(With::<ToCollectionName>::try_convert_to::<CollectionName, _>),
        from_fn(By::<CollectionName>::fetch_one::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(With::<CollectionId>::try_convert_to::<ToCollectionId, _>),
        // restore CollectionId from request name as we dropped it get the ToCollectionId
        from_fn(With::<FromCollectionId>::try_convert_to::<CollectionId, _>),
        from_fn(assert_collection_and_to_collection_are_different),
        // create permission DAO
        from_fn(builder::<InterCollectionPermissionDB>),
        from_fn(With::<RequestContext>::update::<InterCollectionPermissionDBBuilder, _>),
        from_fn(With::<CollectionId>::set::<InterCollectionPermissionDBBuilder>),
        from_fn(With::<ToCollectionId>::set::<InterCollectionPermissionDBBuilder>),
        from_fn(With::<InterCollectionPermissionDBBuilder>::build::<InterCollectionPermissionDB>),
        // insert DAO in DB
        from_fn(insert::<InterCollectionPermissionDB>),
        // Fetch DAO with Names and create DTO response
        from_fn(With::<InterCollectionPermissionDB>::extract::<InterCollectionPermissionId>),
        from_fn(
            By::<InterCollectionPermissionId>::fetch_one::<InterCollectionPermissionDBWithNames>
        ),
        from_fn(
            With::<InterCollectionPermissionDBWithNames>::try_convert_to::<
                InterCollectionPermissionBuilder,
                _,
            >
        ),
        from_fn(With::<InterCollectionPermissionBuilder>::build::<InterCollectionPermission>),
        // refresh the inter collections authz cache
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inter_coll_permission::InterCollectionPermissionError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_authz::tower::AuthzError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserId};
    use td_queries::BasicQueries;
    use td_queries::test_utils::SeedQueries;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_create_inter_collection_permission(db: TdPool) {
        use td_tower::metadata::type_of_val;

        CreateInterCollectionPermissionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>, InterCollectionPermission>(&[
                type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract::<RequestContext>),
                type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_name::<CollectionParam>),
                type_of_val(&With::<CreateRequest<CollectionParam, InterCollectionPermissionCreate>>::extract_data::<InterCollectionPermissionCreate>),

                // check requester is sec_admin or coll_admin, early pre-check
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

                // find collection ID for the FROM collection
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionId>::try_convert_to::<FromCollectionId, _>), //stashing the collection ID we are adding the permission

                // check request is sec_admin or coll_admin for the FROM collection
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),

                // find collection ID for the TO collection
                type_of_val(&With::<InterCollectionPermissionCreate>::extract::<ToCollectionName>),
                type_of_val(&With::<ToCollectionName>::try_convert_to::<CollectionName, _>),
                type_of_val(&By::<CollectionName>::fetch_one::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&With::<CollectionId>::try_convert_to::<ToCollectionId, _>),

                // restore CollectionId from request name as we dropped it get the ToCollectionId
                type_of_val(&With::<FromCollectionId>::try_convert_to::<CollectionId, _>),

                type_of_val(&assert_collection_and_to_collection_are_different),

                // create permission DAO
                type_of_val(&builder::<InterCollectionPermissionDB>),
                type_of_val(&With::<RequestContext>::update::<InterCollectionPermissionDBBuilder, _>),
                type_of_val(&With::<CollectionId>::set::<InterCollectionPermissionDBBuilder>),
                type_of_val(&With::<ToCollectionId>::set::<InterCollectionPermissionDBBuilder>),
                type_of_val(&With::<InterCollectionPermissionDBBuilder>::build::<InterCollectionPermissionDB>),
                // insert DAO in DB
                type_of_val(&insert::<InterCollectionPermissionDB>),

                // Fetch DAO with Names and create DTO response
                type_of_val(&With::<InterCollectionPermissionDB>::extract::<InterCollectionPermissionId>),
                type_of_val(&By::<InterCollectionPermissionId>::fetch_one::<InterCollectionPermissionDBWithNames>),
                type_of_val(&With::<InterCollectionPermissionDBWithNames>::try_convert_to::<InterCollectionPermissionBuilder, _>),
                type_of_val(&With::<InterCollectionPermissionBuilder>::build::<InterCollectionPermission>),

                // refresh the inter collections authz cache
                type_of_val(&refresh_authz_context),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_ok(db: TdPool) -> Result<(), TdError> {
        let service = CreateInterCollectionPermissionService::with_defaults(db.clone()).service();

        let c0 = db
            .seed_collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = db
            .seed_collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c1")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str("c0")?)
                .build()?,
            create,
        );

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let found = db.fetch_all::<InterCollectionPermissionDB>(&c0.id).await?;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].from_collection_id, c0.id);
        assert_eq!(found[0].to_collection_id, ToCollectionId::try_from(c1.id)?);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_same_collection_err(db: TdPool) -> Result<(), TdError> {
        let service = CreateInterCollectionPermissionService::with_defaults(db.clone()).service();

        db.seed_collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c0")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create(
            CollectionParam::builder()
                .collection(CollectionIdName::from_str("c0")?)
                .build()?,
            create,
        );

        assert_service_error(service, request, |err| match err {
            InterCollectionPermissionError::CannotGivePermissionToItself => {}
            other => panic!("Expected 'CannotGivePermissionToItself', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_authz_err(db: TdPool) -> Result<(), TdError> {
        let service = CreateInterCollectionPermissionService::with_defaults(db.clone()).service();

        db.seed_collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        db.seed_collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;

        let create = InterCollectionPermissionCreate::builder()
            .try_to_collection("c0")?
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).create(
                CollectionParam::builder()
                    .collection(CollectionIdName::from_str("c1")?)
                    .build()?,
                create,
            );

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
