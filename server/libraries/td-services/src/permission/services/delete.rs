//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::layers::{
    assert_permission_is_not_fixed, assert_role_in_permission,
    is_permission_with_names_on_a_single_collection,
};
use td_authz::{Authz, AuthzContext, refresh_authz_context};
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::RolePermissionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    ExtractNameService, ExtractService, TryIntoService, UnwrapService, With,
};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{
    CollectionId, EntityId, PermissionId, PermissionIdName, RoleIdName,
};
use td_objects::types::permission::{PermissionDB, PermissionDBWithNames};
use td_tower::default_services::{Do, Else, If, TransactionProvider, conditional};
use td_tower::from_fn::from_fn;
use td_tower::{layers, service, service_factory};

#[service_factory(
    name = DeletePermissionService,
    request = DeleteRequest<RolePermissionParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<RolePermissionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<DeleteRequest<RolePermissionParam>>::extract_name::<RolePermissionParam>),
        from_fn(With::<RolePermissionParam>::extract::<PermissionIdName>),
        from_fn(By::<PermissionIdName>::select::<PermissionDBWithNames>),
        // Check the role in the request matches the role in permission
        from_fn(With::<RolePermissionParam>::extract::<RoleIdName>),
        from_fn(assert_role_in_permission),
        conditional(
            If(service!(layers!(from_fn(
                is_permission_with_names_on_a_single_collection
            )))),
            Do(service!(layers!(
                // a permission on a single collection can also be deleted by a collection admin
                from_fn(With::<PermissionDBWithNames>::extract::<Option<EntityId>>),
                from_fn(With::<EntityId>::unwrap_option),
                from_fn(With::<EntityId>::convert_to::<CollectionId, _>),
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),
            ))),
            Else(service!(layers!(
                // a permission on a all collections can be deleted by a sec_admin only
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),
            )))
        ),
        from_fn(assert_permission_is_not_fixed),
        from_fn(With::<PermissionDBWithNames>::extract::<PermissionId>),
        from_fn(By::<PermissionId>::delete::<PermissionDB>),
        // refresh the permissions authz cache
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::PermissionError;
    use crate::permission::services::create::CreatePermissionService;
    use std::collections::HashSet;
    use td_database::sql::DbPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::RoleParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_permission::{get_permission, seed_permission};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::IdOrName;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, EntityName, PermissionType, RoleId, RoleIdName,
        RoleName, UserId,
    };
    use td_objects::types::permission::PermissionCreate;
    use td_security::{
        ENCODED_ID_CA_ALL_SEC_ADMIN, ENCODED_ID_SA_SYS_ADMIN, ENCODED_ID_SS_SEC_ADMIN,
    };
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_delete_permission(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeletePermissionService::with_defaults(db).metadata().await.assert_service::<DeleteRequest<RolePermissionParam>, ()>(&[
            type_of_val(&With::<DeleteRequest<RolePermissionParam>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(
                &With::<DeleteRequest<RolePermissionParam>>::extract_name::<RolePermissionParam>,
            ),
            type_of_val(&With::<RolePermissionParam>::extract::<PermissionIdName>),
            type_of_val(&By::<PermissionIdName>::select::<PermissionDBWithNames>),
            type_of_val(&With::<RolePermissionParam>::extract::<RoleIdName>),
            type_of_val(&assert_role_in_permission),
            type_of_val(&is_permission_with_names_on_a_single_collection),
            type_of_val(&With::<PermissionDBWithNames>::extract::<Option<EntityId>>),
            type_of_val(&With::<EntityId>::unwrap_option),
            type_of_val(&With::<EntityId>::convert_to::<CollectionId, _>),
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&assert_permission_is_not_fixed),
            type_of_val(&With::<PermissionDBWithNames>::extract::<PermissionId>),
            type_of_val(&By::<PermissionId>::delete::<PermissionDB>),
            type_of_val(&refresh_authz_context),
        ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_permission(db: DbPool) -> Result<(), TdError> {
        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let seeded = seed_permission(&db, PermissionType::SysAdmin, None, None, &role).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            RolePermissionParam::builder()
                .role(RoleIdName::try_from("king")?)
                .permission(PermissionIdName::try_from(seeded.id().to_string())?)
                .build()?,
        );

        let service = DeletePermissionService::with_defaults(db.clone())
            .service()
            .await;
        service.raw_oneshot(request).await?;

        let not_found = get_permission(&db, seeded.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_on_collection_by_coll_admin_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let coll_admin_role =
            seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = coll0.id().try_into()?;
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &coll_admin_role,
        )
        .await;

        let role = seed_role(&db, RoleName::try_from("r1")?, Description::try_from("d")?).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            coll_admin_role.id(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::from_id(role.id()))
                .build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::CollectionDev)
                .entity_name(EntityName::try_from("c0")?)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone())
            .service()
            .await;
        assert!(service.raw_oneshot(request).await.is_ok());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_on_other_collection_by_coll_admin_unauthz(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let coll_admin_role =
            seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = coll0.id().try_into()?;
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &coll_admin_role,
        )
        .await;

        let role = seed_role(&db, RoleName::try_from("r1")?, Description::try_from("d")?).await;
        let _ = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            coll_admin_role.id(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::from_id(role.id()))
                .build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::CollectionDev)
                .entity_name(EntityName::try_from("c1")?)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone())
            .service()
            .await;
        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_on_all_collection_by_coll_admin_unauthz(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let coll_admin_role =
            seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = coll0.id().try_into()?;
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &coll_admin_role,
        )
        .await;
        let _ = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            coll_admin_role.id(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::from_id(coll_admin_role.id()))
                .build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::CollectionDev)
                .entity_name(None)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone())
            .service()
            .await;
        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_permission_on_collection_by_coll_admin_ok(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let role = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = coll0.id().try_into()?;
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;
        let coll_dev_perm = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id())
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::try_from("r0")?)
                    .permission(PermissionIdName::try_from(coll_dev_perm.id().to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone())
            .service()
            .await;
        assert!(service.raw_oneshot(request).await.is_ok());

        let not_found = get_permission(&db, coll_dev_perm.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_permission_incorrect_role_err(db: DbPool) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let role = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = coll0.id().try_into()?;
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;
        let coll_dev_perm = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id())
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::try_from("r_incorrect")?)
                    .permission(PermissionIdName::try_from(coll_dev_perm.id().to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone())
            .service()
            .await;

        assert_service_error(service, request, |err| match err {
            PermissionError::RolePermissionMismatch => {}
            other => panic!("Expected 'RolePermissionMismatch', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_permission_on_diff_collection_by_coll_admin_unauthz(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let coll1 = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
        let role0 = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let role1 = seed_role(&db, RoleName::try_from("r1")?, Description::try_from("d")?).await;
        seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            Some(coll0.id().try_into()?),
            &role0,
        )
        .await;
        let perm1 = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c1")?),
            Some(coll1.id().try_into()?),
            &role1,
        )
        .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role0.id())
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::try_from("r1")?)
                    .permission(PermissionIdName::try_from(perm1.id().to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone())
            .service()
            .await;

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_permission_on_all_collection_by_coll_admin_unauthz(
        db: DbPool,
    ) -> Result<(), TdError> {
        let _ = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let role = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let coll_dev_perm = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            None,
            &role,
        )
        .await;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id())
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::try_from("r0")?)
                    .permission(PermissionIdName::try_from(coll_dev_perm.id().to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone())
            .service()
            .await;

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_delete_system_permissions(db: DbPool) -> Result<(), TdError> {
        let fixed_permissions = HashSet::from([
            ENCODED_ID_SA_SYS_ADMIN,
            ENCODED_ID_SS_SEC_ADMIN,
            ENCODED_ID_CA_ALL_SEC_ADMIN,
        ]);

        let permissions: Vec<PermissionDB> = sqlx::query_as("SELECT * FROM permissions")
            .fetch_all(&db)
            .await
            .unwrap();

        for permission in permissions {
            let service = DeletePermissionService::with_defaults(db.clone())
                .service()
                .await;

            let request: DeleteRequest<RolePermissionParam> = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sec_admin(),
            )
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::from_id(permission.role_id()))
                    .permission(PermissionIdName::from_id(permission.id()))
                    .build()?,
            );

            if fixed_permissions.contains(permission.id().to_string().as_str()) {
                assert_service_error(service, request, |err| match err {
                    PermissionError::PermissionIsFixed => {}
                    other => panic!("Expected 'PermissionIsFixed', got {other:?}"),
                })
                .await;
            } else {
                assert!(service.oneshot(request).await.is_ok());
            }
        }

        Ok(())
    }
}
