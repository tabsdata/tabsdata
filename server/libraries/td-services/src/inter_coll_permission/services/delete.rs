//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::layers::assert_collection_in_permission;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext, refresh_authz_context};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::inter_collection_permission::{
    InterCollectionPermissionDB, InterCollectionPermissionDBWithNames,
};
use td_objects::rest_urls::InterCollectionPermissionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    CollectionId, CollectionIdName, InterCollectionPermissionId, InterCollectionPermissionIdName,
};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteInterCollectionPermissionService,
    request = DeleteRequest<InterCollectionPermissionParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<InterCollectionPermissionParam>>::extract::<RequestContext>),
        from_fn(
            With::<DeleteRequest<InterCollectionPermissionParam>>::extract_name::<
                InterCollectionPermissionParam,
            >
        ),
        // check requester is sec_admin or coll_admin, early pre-check
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // find permission
        from_fn(With::<InterCollectionPermissionParam>::extract::<InterCollectionPermissionIdName>),
        from_fn(
            By::<InterCollectionPermissionIdName>::fetch_one::<InterCollectionPermissionDBWithNames>
        ),
        // Check the role in the request matches the role in permission
        from_fn(With::<InterCollectionPermissionParam>::extract::<CollectionIdName>),
        from_fn(assert_collection_in_permission),
        // check the request is sec_admin or coll_admin for the collection in the permission
        from_fn(With::<InterCollectionPermissionDBWithNames>::extract::<CollectionId>),
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        // delete permission from DB
        from_fn(
            With::<InterCollectionPermissionDBWithNames>::extract::<InterCollectionPermissionId>
        ),
        from_fn(By::<InterCollectionPermissionId>::delete::<InterCollectionPermissionDB>),
        // refresh the inter collections authz cache
        from_fn(refresh_authz_context),
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
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::rest_urls::InterCollectionPermissionParam;
    use td_objects::types::basic::{AccessTokenId, CollectionName, ProjectId, RoleId, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_database::scoped::InProject;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_inter_collection_permission(db: TdPool) {
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;
        use td_database::scoped::Scoped;

        DeleteInterCollectionPermissionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<DeleteRequest<InterCollectionPermissionParam>, ProjectId>, ()>(
                &[
                    type_of_val(
                        &With::<DeleteRequest<InterCollectionPermissionParam>>::extract::<
                            RequestContext,
                        >,
                    ),
                    type_of_val(
                        &With::<DeleteRequest<InterCollectionPermissionParam>>::extract_name::<
                            InterCollectionPermissionParam,
                        >,
                    ),
                    // check requester is sec_admin or coll_admin, early pre-check
                    type_of_val(&AuthzOn::<System>::set),
                    type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                    // find permission
                    type_of_val(
                        &With::<InterCollectionPermissionParam>::extract::<
                            InterCollectionPermissionIdName,
                        >,
                    ),
                    type_of_val(
                        &By::<InterCollectionPermissionIdName>::fetch_one::<
                            InterCollectionPermissionDBWithNames,
                        >,
                    ),
                    // Check the role in the request matches the role in permission
                    type_of_val(
                        &With::<InterCollectionPermissionParam>::extract::<CollectionIdName>,
                    ),
                    type_of_val(&assert_collection_in_permission),
                    // check the request is sec_admin or coll_admin for the collection in the permission
                    type_of_val(
                        &With::<InterCollectionPermissionDBWithNames>::extract::<CollectionId>,
                    ),
                    type_of_val(&AuthzOn::<CollectionId>::set),
                    type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                    // delete permission from DB
                    type_of_val(
                        &With::<InterCollectionPermissionDBWithNames>::extract::<
                            InterCollectionPermissionId,
                        >,
                    ),
                    type_of_val(
                        &By::<InterCollectionPermissionId>::delete::<InterCollectionPermissionDB>,
                    ),
                    // refresh the inter collections authz cache
                    type_of_val(&refresh_authz_context),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_delete_permission_ok(db: TdPool) -> Result<(), TdError> {
        let service = DeleteInterCollectionPermissionService::with_defaults(db.clone()).service();

        let c0 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        let p = db
            .scoped(ProjectId::sandbox())
            .seed()
            .inter_collection_permission(&c0.id, &(*c1.id).into())
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            InterCollectionPermissionParam::builder()
                .collection(CollectionIdName::from_str("c0")?)
                .permission(InterCollectionPermissionIdName::Id(p.id))
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let permissions = db
            .live()
            .fetch_all::<InterCollectionPermissionDB>(&c0.id)
            .await?;
        assert_eq!(permissions.len(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_permission_unauthz_err(db: TdPool) -> Result<(), TdError> {
        let service = DeleteInterCollectionPermissionService::with_defaults(db.clone()).service();

        let c0 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let c1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        let p = db
            .scoped(ProjectId::sandbox())
            .seed()
            .inter_collection_permission(&c0.id, &(*c1.id).into())
            .await?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .delete(
                    InterCollectionPermissionParam::builder()
                        .collection(CollectionIdName::from_str("c0")?)
                        .permission(InterCollectionPermissionIdName::Id(p.id))
                        .build()?,
                )
                .in_project(ProjectId::sandbox());

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
