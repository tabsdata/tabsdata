//
// Copyright 2025 Tabsdata Inc.
//

use crate::permission::{
    assert_permission_is_not_fixed, assert_role_in_permission, authorize_permission_target,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::RbacSource;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::permission::{PermissionDB, PermissionDBWithNames, PermissionParam};
use td_objects::dxo::role::RoleParam;
use td_queries::Queries;

#[service_factory]
async fn delete_permission(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<(RoleParam, PermissionParam)>,
) -> Result<(), TdError> {
    let (ctx, (role_param, permission_param)) = request.into_parts();
    let grant = authz.none();
    let mut trx = db.begin().await?;
    let conn = trx.get(grant);

    let permission: PermissionDBWithNames = conn.fetch_one(&permission_param.permission).await?;
    assert_role_in_permission(&role_param.role, &permission)?;
    // Authorize by the permission's target: manage_access on the system, or
    // manage_project on the target project (single-project permissions only).
    authorize_permission_target(&authz, conn, &ctx, &permission).await?;
    assert_permission_is_not_fixed(&permission)?;

    conn.delete::<PermissionDB>(permission.id).await?;
    trx.commit().await?;

    // Revoked a permission; with the transaction committed, invalidate the
    // cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::permission::PermissionError;
    use std::collections::HashSet;
    use std::str::FromStr;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionCreate;
    use td_objects::dxo::role::RoleParam;
    use td_objects::types::basic::RoleIdName;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, EntityName, PermissionType, ProjectName, RoleName,
        UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;
    use td_security::{
        ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS, ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
        ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS, ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
        ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
    };

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
            .permission(PermissionType::create_projects(), None, None, &role)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
            RoleParam {
                role: RoleIdName::from_str("king")?,
            },
            PermissionParam {
                permission: seeded.id,
            },
        ));

        let service = Services::with_defaults(db.clone());
        service.delete_permission(request).await?;

        let not_found = db.fetch_one::<PermissionDB>(&seeded.id).await;
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
                PermissionType::manage_project(),
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
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::Id(role.id),
            },
            PermissionCreate {
                permission_type: PermissionType::develop_project(),
                entity_name: Some(EntityName::try_from("p0")?),
            },
        );

        let service = Services::with_defaults(db.clone());
        assert!(service.create_permission(request).await.is_ok());
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
                PermissionType::manage_project(),
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
            .user(&UserName::try_from("u0")?, &UserEnabled::TRUE)
            .await?;
        db.seed()
            .user_role(&user.id, &project_admin_role.id)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id).create(
            RoleParam {
                role: RoleIdName::Id(role.id),
            },
            PermissionCreate {
                permission_type: PermissionType::develop_project(),
                entity_name: Some(EntityName::try_from("p1")?),
            },
        );

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.create_permission(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
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
                PermissionType::manage_project(),
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
            .user(&UserName::try_from("u0")?, &UserEnabled::TRUE)
            .await?;
        db.seed()
            .user_role(&user.id, &project_admin_role.id)
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id).create(
            RoleParam {
                role: RoleIdName::Id(project_admin_role.id),
            },
            PermissionCreate {
                permission_type: PermissionType::develop_project(),
                entity_name: None,
            },
        );

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.create_permission(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
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
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;
        let project_dev_perm = db
            .seed()
            .permission(
                PermissionType::develop_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        // `admin` holds manage_project on `__all__`, so it can delete on any project.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
            RoleParam {
                role: RoleIdName::from_str("r0")?,
            },
            PermissionParam {
                permission: project_dev_perm.id,
            },
        ));

        let service = Services::with_defaults(db.clone());
        assert!(service.delete_permission(request).await.is_ok());

        let not_found = db.fetch_one::<PermissionDB>(project_dev_perm.id).await;
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
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;
        let project_dev_perm = db
            .seed()
            .permission(
                PermissionType::develop_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &role,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
            RoleParam {
                role: RoleIdName::from_str("r_incorrect")?,
            },
            PermissionParam {
                permission: project_dev_perm.id,
            },
        ));

        let service = Services::with_defaults(db.clone());

        assert_service_error(service.delete_permission(request).await, |err| match err {
            PermissionError::RolePermissionMismatch => {}
            other => panic!("Expected 'RolePermissionMismatch', got {other:?}"),
        });
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
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(p0.id.try_into()?),
                &role0,
            )
            .await?;
        let perm1 = db
            .seed()
            .permission(
                PermissionType::develop_project(),
                Some(EntityName::try_from("p1")?),
                Some(p1.id.try_into()?),
                &role1,
            )
            .await?;
        // A non-admin user assigned only to role0 (manage_project on p0 only):
        // admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&user.id, &role0.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id).delete((
            RoleParam {
                role: RoleIdName::from_str("r1")?,
            },
            PermissionParam {
                permission: perm1.id,
            },
        ));

        let service = Services::with_defaults(db.clone());

        assert_service_error(service.delete_permission(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
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
                PermissionType::manage_project(),
                Some(EntityName::try_from("p0")?),
                Some(p0.id.try_into()?),
                &role,
            )
            .await?;
        // permission to delete targets ALL projects (entity_id = __all__).
        let all_project_dev_perm = db
            .seed()
            .permission(
                PermissionType::develop_project(),
                Some(EntityName::try_from("p0")?),
                None,
                &role,
            )
            .await?;
        // A non-admin user assigned only to role (manage_project on p0 only):
        // admin can never be forbidden.
        let user = db
            .seed()
            .user(&UserName::try_from("u0")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&user.id, &role.id).await?;

        let request = RequestContext::with(AccessTokenId::default(), user.id).delete((
            RoleParam {
                role: RoleIdName::from_str("r0")?,
            },
            PermissionParam {
                permission: all_project_dev_perm.id,
            },
        ));

        let service = Services::with_defaults(db.clone());

        assert_service_error(service.delete_permission(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_system_permissions(db: TdPool) -> Result<(), TdError> {
        let fixed_permissions = HashSet::from([
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ACCESS,
            ENCODED_ID_PERM_SYS_ADMIN_CREATE_PROJECTS,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_ROLES,
            ENCODED_ID_PERM_SYS_ADMIN_MANAGE_PROJECT,
            // project_creator's create_projects row is system-managed (fixed).
            ENCODED_ID_PERM_PROJECT_CREATOR_CREATE_PROJECTS,
        ]);

        let permissions = db.fetch_all::<PermissionDB>(()).await?;

        for permission in permissions {
            let service = Services::with_defaults(db.clone());

            let request: DeleteRequest<(RoleParam, PermissionParam)> =
                RequestContext::with(AccessTokenId::default(), UserId::admin()).delete((
                    RoleParam {
                        role: RoleIdName::Id(permission.role_id),
                    },
                    PermissionParam {
                        permission: permission.id,
                    },
                ));

            if fixed_permissions.contains(permission.id.to_string().as_str()) {
                assert_service_error(service.delete_permission(request).await, |err| match err {
                    PermissionError::PermissionIsFixed => {}
                    other => panic!("Expected 'PermissionIsFixed', got {other:?}"),
                });
            } else {
                assert!(service.delete_permission(request).await.is_ok());
            }
        }

        Ok(())
    }
}
