//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::layers::{is_permission_on_a_single_collection, PermissionBuildService};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{refresh_authz_context, Authz, AuthzContext};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::permission::{
    Permission, PermissionBuilder, PermissionCreate, PermissionDB, PermissionDBBuilder,
    PermissionDBWithNames,
};
use td_objects::dxo::role::RoleDB;
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, EntityId, PermissionId, RoleIdName};
use td_queries::insert;
use td_queries::By;
use td_tower::default_services::{conditional, Do, Else, If, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::{layers, service};

#[service_factory(
    name = CreatePermissionService,
    request = CreateRequest<RoleParam, PermissionCreate>,
    response = Permission,
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<RoleParam, PermissionCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<CreateRequest<RoleParam, PermissionCreate>>::extract_name::<RoleParam>),
        from_fn(
            With::<CreateRequest<RoleParam, PermissionCreate>>::extract_data::<PermissionCreate>
        ),
        from_fn(With::<PermissionCreate>::try_convert_to::<PermissionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<PermissionDBBuilder, _>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        from_fn(With::<RoleDB>::update::<PermissionDBBuilder, _>),
        from_fn(With::<PermissionDBBuilder>::build_permission_db),
        conditional(
            If(service!(layers!(from_fn(
                is_permission_on_a_single_collection
            )))),
            Do(service!(layers!(
                // a permission on a single collection can also be created by a collection admin
                from_fn(With::<PermissionDB>::extract::<EntityId>),
                from_fn(With::<EntityId>::try_convert_to::<CollectionId, _>),
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
        from_fn(By::<PermissionId>::fetch_one::<PermissionDBWithNames>),
        from_fn(With::<PermissionDBWithNames>::try_convert_to::<PermissionBuilder, _>),
        from_fn(With::<PermissionBuilder>::build::<Permission>),
        // refresh the permissions authz cache
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
    use td_error::{assert_service_error, TdError};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, Description, EntityName, PermissionType, ProjectId, RoleId,
        RoleName, UserId,
    };
    use td_objects::Buildable;
    use td_queries::test_utils::SeedEntry;
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_create_permission(db: TdPool) {
        use td_tower::metadata::type_of_val;

        CreatePermissionService::with_defaults(db)
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
                type_of_val(&With::<PermissionCreate>::try_convert_to::<PermissionDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<PermissionDBBuilder, _>),
                type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
                type_of_val(&By::<RoleIdName>::fetch_one::<RoleDB>),
                type_of_val(&With::<RoleDB>::update::<PermissionDBBuilder, _>),
                type_of_val(&With::<PermissionDBBuilder>::build_permission_db),
                type_of_val(&is_permission_on_a_single_collection),
                type_of_val(&With::<PermissionDB>::extract::<EntityId>),
                type_of_val(&With::<EntityId>::try_convert_to::<CollectionId, _>),
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&insert::<PermissionDB>),
                type_of_val(&With::<PermissionDB>::extract::<PermissionId>),
                type_of_val(&By::<PermissionId>::fetch_one::<PermissionDBWithNames>),
                type_of_val(&With::<PermissionDBWithNames>::try_convert_to::<PermissionBuilder, _>),
                type_of_val(&With::<PermissionBuilder>::build::<Permission>),
                type_of_val(&refresh_authz_context),
            ]);
    }

    #[td_test::test(db)]
    async fn test_create_permission(db: TdPool) -> Result<(), TdError> {
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
                .role(RoleIdName::from_str(&RoleName::user().to_string())?)
                .build()?,
            create,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = db.live().fetch_one::<PermissionDB>(&response.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.permission_type, found.permission_type);
        assert_eq!(response.entity_type, found.entity_type);
        assert_eq!(
            response.entity_id.unwrap_or(EntityId::all_entities()),
            found.entity_id
        );
        assert_eq!(response.granted_by_id, found.granted_by_id);
        assert_eq!(response.granted_on, found.granted_on);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_permission_on_collection_by_coll_admin_ok(
        db: TdPool,
    ) -> Result<(), TdError> {
        let coll0 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = coll0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::CollectionAdmin,
                Some(EntityName::try_from("c0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::CollectionRead)
            .try_entity_name(Some(EntityName::try_from("c0")?))
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id)
            .create(
                RoleParam::builder()
                    .role(RoleIdName::from_str("r0")?)
                    .build()?,
                create,
            );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = db.live().fetch_one::<PermissionDB>(&response.id).await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.role_id, found.role_id);
        assert_eq!(response.permission_type, found.permission_type);
        assert_eq!(response.entity_type, found.entity_type);
        assert_eq!(
            response.entity_id.unwrap_or(EntityId::all_entities()),
            found.entity_id
        );
        assert_eq!(response.granted_by_id, found.granted_by_id);
        assert_eq!(response.granted_on, found.granted_on);
        assert_eq!(response.fixed, found.fixed);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_permission_on_collection_by_coll_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let coll0 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0")?, &UserId::admin())
            .await?;
        let _ = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c1")?, &UserId::admin())
            .await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = coll0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::CollectionAdmin,
                Some(EntityName::try_from("c0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::CollectionRead)
            .try_entity_name(Some(EntityName::try_from("c1")?))
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id)
            .create(
                RoleParam::builder()
                    .role(RoleIdName::from_str("r0")?)
                    .build()?,
                create,
            );

        let service = CreatePermissionService::with_defaults(db.clone()).service();

        assert_service_error(service, request, |err| match err {
            AuthzError::Forbidden(_) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
