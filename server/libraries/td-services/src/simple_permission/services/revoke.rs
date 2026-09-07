//
// Copyright 2026 Tabsdata Inc.
//

use crate::permission::authorize_permission_target;
use crate::simple_permission::{assert_simple_permission_valid, resolve_grant};
use crate::user_role::assert_not_fixed;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::RbacSource;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user_role::{UserRoleDB, UserRoleDBWithNames};
use td_queries::Queries;

#[service_factory]
async fn revoke_simple_permission(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: DeleteRequest<SimplePermissionGrant>,
) -> Result<(), TdError> {
    let (ctx, grant) = request.into_parts();
    let token = authz.none();
    let mut trx = db.begin().await?;
    let conn = trx.get(token);

    // validate (reject org-wide wildcards; project required/forbidden)
    assert_simple_permission_valid(&grant)?;
    // resolve the target role membership (and project for authz)
    let resolved = resolve_grant(conn, &grant).await?;
    // authz by target (shared with grant and the raw permission API).
    authorize_permission_target(&authz, conn, &ctx, &resolved).await?;

    // revoking a permission the user does not hold reports a clear
    // "not found" error instead of the opaque user_role row-not-found
    // SQL error from the fetch.
    conn.assert_exists::<UserRoleDB>(and!(&resolved.user_id, &resolved.role_id))
        .await?;
    // membership remove, reusing the user_role delete fixed-row guard.
    let row: UserRoleDBWithNames = conn
        .fetch_one(and!(&resolved.user_id, &resolved.role_id))
        .await?;
    assert_not_fixed(&row)?;
    conn.delete::<UserRoleDB>(and!(&resolved.user_id, &resolved.role_id))
        .await?;
    trx.commit().await?;

    // Role membership changed; with the transaction committed, invalidate the
    // cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(token)).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, ProjectIdName, ProjectName, RoleId, RoleName,
        SimplePermissionType, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    async fn user_role_count(db: &TdPool, user: &UserId, role: &RoleId) -> usize {
        db.fetch_all::<UserRoleDB>(())
            .await
            .unwrap()
            .into_iter()
            .filter(|ur| &ur.user_id == user && &ur.role_id == role)
            .count()
    }

    /// Create a project through the real create service so the per-project
    /// roles are minted.
    async fn create_project(db: &TdPool, name: &str) -> Result<(), TdError> {
        use td_objects::dxo::project::ProjectCreate;
        let create = ProjectCreate {
            name: ProjectName::try_from(name)?,
            description: td_objects::types::basic::Description::try_from("d")?,
            base_path: BasePath::default(),
        };
        Services::with_defaults(db.clone())
            .create_project(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create),
            )
            .await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_revoke_develop_project_removes_dev_membership(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        let dev_role = db
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_dev")?)
            .await?;
        db.seed().user_role(&user.id, &dev_role.id).await?;
        assert_eq!(user_role_count(&db, &user.id, &dev_role.id).await, 1);

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::DevelopProject,
            project: Some(ProjectIdName::from_str("p0")?),
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = Services::with_defaults(db.clone());
        service.revoke_simple_permission(request).await?;

        assert_eq!(user_role_count(&db, &user.id, &dev_role.id).await, 0);
        Ok(())
    }

    /// Revoking a permission the user does not hold reports a clear "not
    /// found" error rather than the opaque `user_role` row-not-found SQL error.
    #[td_test::test(db)]
    async fn test_revoke_not_granted_reports_not_found(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::CreateProjects,
            project: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = Services::with_defaults(db.clone());
        assert_service_error(
            service.revoke_simple_permission(request).await,
            |err| match err {
                SqlError::CouldNotFindEntity(_, _) => {}
                other => panic!("Expected 'CouldNotFindEntity', got {other:?}"),
            },
        );
        Ok(())
    }

    /// The `admin` user's `sys_admin` membership is fixed and cannot be revoked
    /// (reuses the user_role delete guard).
    #[td_test::test(db)]
    async fn test_revoke_fixed_membership_rejected(db: TdPool) -> Result<(), TdError> {
        let grant = SimplePermissionGrant {
            user: UserName::admin(),
            permission: SimplePermissionType::ManageSystem,
            project: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = Services::with_defaults(db.clone());
        assert_service_error(
            service.revoke_simple_permission(request).await,
            |err| match err {
                UserRoleError::FixedUserRole(_, _) => {}
                other => panic!("Expected FixedUserRole, got {other:?}"),
            },
        );
        Ok(())
    }
}
