//
// Copyright 2025 Tabsdata Inc.
//

use crate::permission::layers::{
    PermissionBuildService, assert_role_is_not_fixed, authorize_permission_target,
};
use ta_services::factory::service_factory;
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::permission::{
    Permission, PermissionBuilder, PermissionCreate, PermissionDB, PermissionDBBuilder,
    PermissionDBWithNames,
};
use td_objects::dxo::role::RoleDB;
use td_objects::rest_urls::RoleParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{PermissionId, RoleIdName};
use td_queries::By;
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

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
        from_fn(With::<CreateRequest<RoleParam, PermissionCreate>>::extract_name::<RoleParam>),
        from_fn(
            With::<CreateRequest<RoleParam, PermissionCreate>>::extract_data::<PermissionCreate>
        ),
        from_fn(With::<PermissionCreate>::try_convert_to::<PermissionDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<PermissionDBBuilder, _>),
        from_fn(With::<RoleParam>::extract::<RoleIdName>),
        from_fn(By::<RoleIdName>::fetch_one::<RoleDB>),
        // A system-managed (fixed) role's permission set is immutable; reject
        // adding a permission to it from the user surface.
        from_fn(assert_role_is_not_fixed),
        from_fn(With::<RoleDB>::update::<PermissionDBBuilder, _>),
        // build_permission_db derives entity_type from the authorization
        // registry, so an unregistered permission is already rejected here.
        from_fn(With::<PermissionDBBuilder>::build_permission_db),
        // Authorize by the permission's target: manage_access on the system, or
        // manage_project on the target project (single-project permissions only).
        from_fn(authorize_permission_target::<PermissionDB>),
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
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, EntityName, PermissionType, ProjectName, RoleName,
        UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
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
                type_of_val(&assert_role_is_not_fixed),
                type_of_val(&With::<RoleDB>::update::<PermissionDBBuilder, _>),
                type_of_val(&With::<PermissionDBBuilder>::build_permission_db),
                type_of_val(&authorize_permission_target::<PermissionDB>),
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
            .permission_type(PermissionType::manage_access())
            .try_entity_name(None)
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam::builder()
                .role(RoleIdName::from_str(&RoleName::default_role().to_string())?)
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
    async fn test_create_permission_on_project_by_project_admin_ok(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::view_project())
            .try_entity_name(Some(EntityName::try_from("p0")?))
            .unwrap()
            .build()?;

        // `admin` holds manage_project on `__all__`, so it can grant on any project.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
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
    async fn test_create_permission_on_project_by_project_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let _ = db.seed().project(ProjectName::try_from("p1")?).await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;
        // A non-admin user assigned only to `role` (manage_project on p0 only):
        // admin can never be forbidden, so the requester must be this user.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&user.id, &role.id).await?;

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::view_project())
            .try_entity_name(Some(EntityName::try_from("p1")?))
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), user.id).create(
            RoleParam::builder()
                .role(RoleIdName::from_str("r0")?)
                .build()?,
            create,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();

        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        Ok(())
    }

    /// A system-managed (fixed) role's permission set is immutable: even
    /// `admin` (who passes the RBAC check) cannot add a permission to the
    /// seeded `sys_admin` role.
    #[td_test::test(db)]
    async fn test_create_permission_on_fixed_role_rejected(db: TdPool) -> Result<(), TdError> {
        use crate::permission::PermissionError;

        let create = PermissionCreate::builder()
            .permission_type(PermissionType::create_projects())
            .try_entity_name(None)
            .unwrap()
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam::builder()
                .role(RoleIdName::from_str(&RoleName::sys_admin().to_string())?)
                .build()?,
            create,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            PermissionError::RoleIsFixed(_) => {}
            other => panic!("Expected 'RoleIsFixed', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
