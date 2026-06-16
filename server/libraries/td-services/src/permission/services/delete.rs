//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::layers::{
    assert_permission_is_not_fixed, assert_role_in_permission,
    is_permission_with_names_on_a_single_project,
};
use ta_services::factory::service_factory;
use td_authz::rbac::{
    ManageAccess, ManageProject, OnAnyProject, Project, Rbac, Scope, System,
};
use td_authz::{refresh_authz_context, AuthzContext};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::permission::{PermissionDB, PermissionDBWithNames};
use td_objects::rest_urls::RolePermissionParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    EntityId, PermissionId, PermissionIdName, ProjectId, RoleIdName,
};
use td_queries::By;
use td_tower::default_services::{Do, Else, If, TransactionProvider, conditional};
use td_tower::from_fn::from_fn;
use td_tower::{layers, service};

#[service_factory(
    name = DeletePermissionService,
    request = DeleteRequest<RolePermissionParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<RolePermissionParam>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>::check),
        from_fn(With::<DeleteRequest<RolePermissionParam>>::extract_name::<RolePermissionParam>),
        from_fn(With::<RolePermissionParam>::extract::<PermissionIdName>),
        from_fn(By::<PermissionIdName>::fetch_one::<PermissionDBWithNames>),
        // Check the role in the request matches the role in permission
        from_fn(With::<RolePermissionParam>::extract::<RoleIdName>),
        from_fn(assert_role_in_permission),
        conditional(
            If(service!(layers!(from_fn(
                is_permission_with_names_on_a_single_project
            )))),
            Do(service!(layers!(
                // a permission on a single project can also be deleted by a project admin
                from_fn(With::<PermissionDBWithNames>::extract::<Option<EntityId>>),
                from_fn(With::<Option<EntityId>>::unwrap),
                from_fn(With::<EntityId>::try_convert_to::<ProjectId, _>),
                from_fn(Scope::<Project>::set),
                from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check),
            ))),
            Else(service!(layers!(
                // a permission on all projects can be deleted only by an
                // identity holding manage_access on the system
                from_fn(Scope::<System>::system),
                from_fn(Rbac::<System, ManageAccess>::check),
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionCreate;
    use td_objects::rest_urls::RoleParam;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityName, PermissionType, ProjectName, RoleId, RoleName,
        UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_security::{
        ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
        ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT, ENCODED_ID_PERM_SYS_ADMIN_ROLE_MGNT,
    };
    use td_tower::ctx_service::RawOneshot;
    use tower::ServiceExt;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_permission(db: TdPool) {
        use td_tower::metadata::type_of_val;

        DeletePermissionService::with_defaults(db).metadata().await.assert_service::<DeleteRequest<RolePermissionParam>, ()>(&[
            type_of_val(&With::<DeleteRequest<RolePermissionParam>>::extract::<RequestContext>),
            type_of_val(&Scope::<System>::system),
            type_of_val(&Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>::check),
            type_of_val(
                &With::<DeleteRequest<RolePermissionParam>>::extract_name::<RolePermissionParam>,
            ),
            type_of_val(&With::<RolePermissionParam>::extract::<PermissionIdName>),
            type_of_val(&By::<PermissionIdName>::fetch_one::<PermissionDBWithNames>),
            type_of_val(&With::<RolePermissionParam>::extract::<RoleIdName>),
            type_of_val(&assert_role_in_permission),
            type_of_val(&is_permission_with_names_on_a_single_project),
            type_of_val(&With::<PermissionDBWithNames>::extract::<Option<EntityId>>),
            type_of_val(&With::<Option<EntityId>>::unwrap),
            type_of_val(&With::<EntityId>::try_convert_to::<ProjectId, _>),
            type_of_val(&Scope::<Project>::set),
            type_of_val(&Rbac::<Project, (ManageAccess, ManageProject)>::check),
            type_of_val(&Scope::<System>::system),
            type_of_val(&Rbac::<System, ManageAccess>::check),
            type_of_val(&assert_permission_is_not_fixed),
            type_of_val(&With::<PermissionDBWithNames>::extract::<PermissionId>),
            type_of_val(&By::<PermissionId>::delete::<PermissionDB>),
            type_of_val(&refresh_authz_context),
        ]);
    }

    #[td_test::test(db)]
    async fn test_delete_permission(db: TdPool) -> Result<(), TdError> {
        let role = db
            .seed()
            .role(
                RoleName::try_from("king")?,
                Description::try_from("super user")?,
            )
            .await?;
        let seeded = db
            .seed()
            .permission(PermissionType::CreateProjects, None, None, &role)
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            RolePermissionParam::builder()
                .role(RoleIdName::from_str("king")?)
                .permission(PermissionIdName::from_str(&seeded.id.to_string())?)
                .build()?,
        );

        let service = DeletePermissionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        let not_found = db.live().fetch_one::<PermissionDB>(&seeded.id).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_permission_on_project_by_project_admin_ok(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let project_admin_role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &project_admin_role,
            )
            .await?;

        let role = db
            .seed()
            .role(RoleName::try_from("r1")?, Description::try_from("d")?)
            .await?;

        // `admin` holds manage_project on `__all__`, so it can grant on any project.
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            project_admin_role.id,
        )
        .create(
            RoleParam::builder().role(RoleIdName::Id(role.id)).build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::DevelopProject)
                .entity_name(EntityName::try_from("p0")?)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        assert!(service.raw_oneshot(request).await.is_ok());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_permission_on_other_project_by_project_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let project_admin_role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &project_admin_role,
            )
            .await?;

        let role = db
            .seed()
            .role(RoleName::try_from("r1")?, Description::try_from("d")?)
            .await?;
        let _ = db.seed().project(ProjectName::try_from("p1")?).await?;
        // A non-admin user assigned only to the project_admin_role (manage_project
        // on p0 only): admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::from(true))
            .await?;
        db.seed()
            .user_role(&user.id, &project_admin_role.id)
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            user.id,
            project_admin_role.id,
        )
        .create(
            RoleParam::builder().role(RoleIdName::Id(role.id)).build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::DevelopProject)
                .entity_name(EntityName::try_from("p1")?)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_create_permission_on_all_project_by_project_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let project_admin_role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &project_admin_role,
            )
            .await?;
        let _ = db.seed().project(ProjectName::try_from("p1")?).await?;
        // A non-admin user assigned only to the project_admin_role (manage_project
        // on p0 only): admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::from(true))
            .await?;
        db.seed()
            .user_role(&user.id, &project_admin_role.id)
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            user.id,
            project_admin_role.id,
        )
        .create(
            RoleParam::builder()
                .role(RoleIdName::Id(project_admin_role.id))
                .build()?,
            PermissionCreate::builder()
                .permission_type(PermissionType::DevelopProject)
                .entity_name(None)
                .build()?,
        );

        let service = CreatePermissionService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_permission_on_project_by_project_admin_ok(
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
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;
        let project_dev_perm = db
            .seed()
            .permission(
                PermissionType::DevelopProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        // `admin` holds manage_project on `__all__`, so it can delete on any project.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id)
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::from_str("r0")?)
                    .permission(PermissionIdName::from_str(&project_dev_perm.id.to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone()).service();
        assert!(service.raw_oneshot(request).await.is_ok());

        let not_found = db.live().fetch_one::<PermissionDB>(project_dev_perm.id).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_permission_incorrect_role_err(db: TdPool) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;
        let project_dev_perm = db
            .seed()
            .permission(
                PermissionType::DevelopProject,
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id)
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::from_str("r_incorrect")?)
                    .permission(PermissionIdName::from_str(&project_dev_perm.id.to_string())?)
                    .build()?,
            );

        let service = DeletePermissionService::with_defaults(db.clone()).service();

        assert_service_error(service, request, |err| match err {
            PermissionError::RolePermissionMismatch => {}
            other => panic!("Expected 'RolePermissionMismatch', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_permission_on_diff_project_by_project_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let p1 = db.seed().project(ProjectName::try_from("p1")?).await?;
        let role0 = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        let role1 = db
            .seed()
            .role(RoleName::try_from("r1")?, Description::try_from("d")?)
            .await?;
        // requester user holds manage_project on p0 only (via role0).
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(p0.id.try_into()?),
                &role0,
            )
            .await?;
        let perm1 = db
            .seed()
            .permission(
                PermissionType::DevelopProject,
                Some(EntityName::try_from("p1")?),
                Some(p1.id.try_into()?),
                &role1,
            )
            .await?;
        // A non-admin user assigned only to role0 (manage_project on p0 only):
        // admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&user.id, &role0.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id, role0.id).delete(
            RolePermissionParam::builder()
                .role(RoleIdName::from_str("r1")?)
                .permission(PermissionIdName::from_str(&perm1.id.to_string())?)
                .build()?,
        );

        let service = DeletePermissionService::with_defaults(db.clone()).service();

        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_permission_on_all_project_by_project_admin_unauthz(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let role = db
            .seed()
            .role(RoleName::try_from("r0")?, Description::try_from("d")?)
            .await?;
        // requester user holds manage_project on p0 only.
        db.seed()
            .permission(
                PermissionType::ManageProject,
                Some(EntityName::try_from("p0")?),
                Some(p0.id.try_into()?),
                &role,
            )
            .await?;
        // permission to delete targets ALL projects (entity_id = __all__).
        let all_project_dev_perm = db
            .seed()
            .permission(
                PermissionType::DevelopProject,
                Some(EntityName::try_from("p0")?),
                None,
                &role,
            )
            .await?;
        // A non-admin user assigned only to role (manage_project on p0 only):
        // admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id, role.id).delete(
            RolePermissionParam::builder()
                .role(RoleIdName::from_str("r0")?)
                .permission(PermissionIdName::from_str(
                    &all_project_dev_perm.id.to_string(),
                )?)
                .build()?,
        );

        let service = DeletePermissionService::with_defaults(db.clone()).service();

        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_system_permissions(db: TdPool) -> Result<(), TdError> {
        let fixed_permissions = HashSet::from([
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
            ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
            ENCODED_ID_PERM_SYS_ADMIN_ROLE_MGNT,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
        ]);

        let permissions = db.live().fetch_all::<PermissionDB>(()).await?;

        for permission in permissions {
            let service = DeletePermissionService::with_defaults(db.clone()).service();

            let request: DeleteRequest<RolePermissionParam> = RequestContext::with(
                AccessTokenId::default(),
                UserId::admin(),
                RoleId::sys_admin(),
            )
            .delete(
                RolePermissionParam::builder()
                    .role(RoleIdName::Id(permission.role_id))
                    .permission(PermissionIdName::Id(permission.id))
                    .build()?,
            );

            if fixed_permissions.contains(permission.id.to_string().as_str()) {
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
