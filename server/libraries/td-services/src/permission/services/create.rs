//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::layers::{PermissionBuildService, is_permission_on_a_single_collection};
use td_authz::{Authz, AuthzContext, refresh_authz_context};
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::basic::{CollectionId, EntityId, PermissionId, RoleIdName};
use td_objects::types::permission::{
    Permission, PermissionBuilder, PermissionCreate, PermissionDB, PermissionDBBuilder,
    PermissionDBWithNames,
};
use td_objects::types::role::RoleDB;
use td_tower::default_services::{Do, Else, If, TransactionProvider, conditional};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider, service};

#[provider(
    name = CreatePermissionService,
    request = CreateRequest<RoleParam, PermissionCreate>,
    response = Permission,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<CreateRequest<RoleParam, PermissionCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<CreateRequest<RoleParam, PermissionCreate>>::extract_name::<RoleParam>),
        from_fn(
            With::<CreateRequest<RoleParam, PermissionCreate>>::extract_data::<PermissionCreate>
        ),
        from_fn(With::<PermissionCreate>::convert_to::<PermissionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<PermissionDBBuilder, _>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::select::<RoleDB>),
        from_fn(With::<RoleDB>::update::<PermissionDBBuilder, _>),
        from_fn(With::<PermissionDBBuilder>::build_permission_db),
        conditional(
            If(service!(layers!(from_fn(
                is_permission_on_a_single_collection
            )))),
            Do(service!(layers!(
                // a permission on a single collection can also be created by a collection admin
                from_fn(With::<PermissionDB>::extract::<EntityId>),
                from_fn(With::<EntityId>::convert_to::<CollectionId, _>),
                from_fn(AuthzOn::<CollectionId>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),
            ))),
            Else(service!(layers!(
                // a permission on a all collections can be created by a sec_admin only
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),
            )))
        ),
        from_fn(insert::<PermissionDB>),
        from_fn(With::<PermissionDB>::extract::<PermissionId>),
        from_fn(By::<PermissionId>::select::<PermissionDBWithNames>),
        from_fn(With::<PermissionDBWithNames>::convert_to::<PermissionBuilder, _>),
        from_fn(With::<PermissionBuilder>::build::<Permission, _>),
        // refresh the permissions authz cache
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_permission::{get_permission, seed_permission};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, EntityName, PermissionType, RoleId, RoleName,
        UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_create_permission(db: DbPool) {
        use td_tower::metadata::type_of_val;

        CreatePermissionService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<CreateRequest<RoleParam, PermissionCreate>, Permission>(&[
                type_of_val(
                    &With::<CreateRequest<RoleParam, PermissionCreate>>::extract::<RequestContext>,
                ),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(
                    &With::<CreateRequest<RoleParam, PermissionCreate>>::extract_name::<RoleParam>,
                ),
                type_of_val(
                    &With::<CreateRequest<RoleParam, PermissionCreate>>::extract_data::<
                        PermissionCreate,
                    >,
                ),
                type_of_val(&With::<PermissionCreate>::convert_to::<PermissionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<PermissionDBBuilder, _>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::select::<RoleDB>),
                type_of_val(&With::<RoleDB>::update::<PermissionDBBuilder, _>),
                type_of_val(&With::<PermissionDBBuilder>::build_permission_db),
                type_of_val(&is_permission_on_a_single_collection),
                type_of_val(&With::<PermissionDB>::extract::<EntityId>),
                type_of_val(&With::<EntityId>::convert_to::<CollectionId, _>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&insert::<PermissionDB>),
                type_of_val(&With::<PermissionDB>::extract::<PermissionId>),
                type_of_val(&By::<PermissionId>::select::<PermissionDBWithNames>),
                type_of_val(&With::<PermissionDBWithNames>::convert_to::<PermissionBuilder, _>),
                type_of_val(&With::<PermissionBuilder>::build::<Permission, _>),
                type_of_val(&refresh_authz_context),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission(db: DbPool) -> Result<(), TdError> {
        let create = PermissionCreate::builder()
            .permission_type(PermissionType::SecAdmin)
            .try_entity_name(None)
            .unwrap()
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::try_from(RoleName::user().to_string())?)
                .build()?,
            create,
        );

        let service = CreatePermissionService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_permission(&db, response.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.permission_type(), found.permission_type());
        assert_eq!(response.entity_type(), found.entity_type());
        assert_eq!(
            response.entity_id().unwrap_or(EntityId::all_entities()),
            *found.entity_id()
        );
        assert_eq!(response.granted_by_id(), found.granted_by_id());
        assert_eq!(response.granted_on(), found.granted_on());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_on_collection_by_coll_admin_ok(
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

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::CollectionRead)
            .try_entity_name(Some(EntityName::try_from("c0")?))
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id())
            .create(
                RoleParam::builder()
                    .role(RoleIdName::try_from("r0")?)
                    .build()?,
                create,
            );

        let service = CreatePermissionService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_permission(&db, response.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.permission_type(), found.permission_type());
        assert_eq!(response.entity_type(), found.entity_type());
        assert_eq!(
            response.entity_id().unwrap_or(EntityId::all_entities()),
            *found.entity_id()
        );
        assert_eq!(response.granted_by_id(), found.granted_by_id());
        assert_eq!(response.granted_on(), found.granted_on());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_permission_on_collection_by_coll_admin_unauthz(
        db: DbPool,
    ) -> Result<(), TdError> {
        let coll0 = seed_collection(&db, &CollectionName::try_from("c0")?, &UserId::admin()).await;
        let _ = seed_collection(&db, &CollectionName::try_from("c1")?, &UserId::admin()).await;
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

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::CollectionRead)
            .try_entity_name(Some(EntityName::try_from("c1")?))
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id())
            .create(
                RoleParam::builder()
                    .role(RoleIdName::try_from("r0")?)
                    .build()?,
                create,
            );

        let service = CreatePermissionService::with_defaults(db.clone())
            .await
            .service()
            .await;

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
