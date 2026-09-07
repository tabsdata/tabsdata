//
// Copyright 2025 Tabsdata Inc.
//

use crate::permission::{
    assert_role_is_not_fixed, authorize_permission_target, build_permission_db,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::RbacSource;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::permission::{Permission, PermissionCreate, PermissionDBWithNames};
use td_objects::dxo::role::{RoleDB, RoleParam};
use td_queries::Queries;

#[service_factory]
async fn create_permission(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<PermissionCreate, RoleParam>,
) -> Result<Permission, TdError> {
    let (ctx, param, create) = request.into_parts();
    let grant = authz.none();
    let mut trx = db.begin().await?;
    let conn = trx.get(grant);

    let role: RoleDB = conn.fetch_one(&param.role).await?;
    // A system-managed (fixed) role's permission set is immutable; reject
    // adding a permission to it from the user surface.
    assert_role_is_not_fixed(&role)?;

    // build_permission_db derives entity_type from the authorization
    // registry, so an unregistered permission is already rejected here.
    let permission = build_permission_db(&authz, conn, &ctx, &role, &create).await?;
    // Authorize by the permission's target: manage_access on the system, or
    // manage_project on the target project (single-project permissions only).
    authorize_permission_target(&authz, conn, &ctx, &permission).await?;
    conn.insert(&permission).await?;

    let with_names: PermissionDBWithNames = conn.fetch_one(&permission.id).await?;
    let read = Permission::from_dao(with_names)?;
    trx.commit().await?;

    // Gated a permission; with the transaction committed, invalidate the
    // cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::str::FromStr;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionDB;
    use td_objects::types::basic::RoleIdName;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, EntityName, PermissionType, ProjectName, RoleName,
        UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_create_permission(db: TdPool) -> Result<(), TdError> {
        let create = PermissionCreate {
            permission_type: PermissionType::manage_access(),
            entity_name: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::from_str(&RoleName::default_role().to_string())?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());
        let response = service.create_permission(request).await;
        let response = response?;

        let found = db.fetch_one::<PermissionDB>(&response.id).await?;
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

        let create = PermissionCreate {
            permission_type: PermissionType::view_project(),
            entity_name: Some(EntityName::try_from("p0")?),
        };

        // `admin` holds manage_project on `__all__`, so it can grant on any project.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::from_str("r0")?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());
        let response = service.create_permission(request).await;
        let response = response?;

        let found = db.fetch_one::<PermissionDB>(&response.id).await?;
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
            .user(&UserName::try_from("u0")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&user.id, &role.id).await?;

        let create = PermissionCreate {
            permission_type: PermissionType::view_project(),
            entity_name: Some(EntityName::try_from("p1")?),
        };

        let request = RequestContext::with(AccessTokenId::default(), user.id).create(
            RoleParam {
                role: RoleIdName::from_str("r0")?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());

        assert_service_error(service.create_permission(request).await, |err| match err {
            RbacError::Forbidden(_, _) => {}
        });
        Ok(())
    }

    /// A system-managed (fixed) role's permission set is immutable: even
    /// `admin` (who passes the RBAC check) cannot add a permission to the
    /// seeded `sys_admin` role.
    #[td_test::test(db)]
    async fn test_create_permission_on_fixed_role_rejected(db: TdPool) -> Result<(), TdError> {
        use crate::permission::PermissionError;

        let create = PermissionCreate {
            permission_type: PermissionType::create_projects(),
            entity_name: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).create(
            RoleParam {
                role: RoleIdName::from_str(&RoleName::sys_admin().to_string())?,
            },
            create,
        );

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.create_permission(request).await, |err| match err {
            PermissionError::RoleIsFixed(_) => {}
            other => panic!("Expected 'RoleIsFixed', got {other:?}"),
        });
        Ok(())
    }
}
