//
// Copyright 2026 Tabs Data Inc.
//

//! Revoke a flat Simple permission from a user (`tdk permission revoke`).
//!
//! The inverse of [`grant`](super::grant): resolves the same target role and
//! removes the `user_role` membership, reusing the user_role delete behavior
//! including the fixed-row guard (e.g. the `admin` user cannot be removed from
//! `sys_admin`). Same authz as grant.

use crate::permission::authorize_permission_target;
use crate::simple_permission::layers::{
    ResolvedGrant, assert_simple_permission_valid, resolve_grant, resolved_role_id,
    resolved_user_id,
};
use crate::user_role::layers::assert_not_fixed;
use ta_services::factory::service_factory;
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::And;
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user_role::{UserRoleDB, UserRoleDBWithNames};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{RoleId, UserId};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = RevokeSimplePermissionService,
    request = DeleteRequest<SimplePermissionGrant>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<SimplePermissionGrant>>::extract::<RequestContext>),
        from_fn(
            With::<DeleteRequest<SimplePermissionGrant>>::extract_name::<SimplePermissionGrant>
        ),
        // validate (reject org-wide wildcards; project required/forbidden)
        from_fn(assert_simple_permission_valid),
        // resolve the target role membership (and project for authz)
        from_fn(resolve_grant),
        // authz by target (shared with grant and the raw permission API).
        from_fn(authorize_permission_target::<ResolvedGrant>),
        // membership remove, reusing the user_role delete fixed-row guard.
        from_fn(resolved_role_id),
        from_fn(resolved_user_id),
        // revoking a permission the user does not hold reports a clear
        // "not found" error instead of the opaque user_role row-not-found
        // SQL error from the fetch.
        from_fn(By::<And![RoleId, UserId]>::assert_exists::<UserRoleDB>),
        from_fn(By::<And![RoleId, UserId]>::fetch_one::<UserRoleDBWithNames>),
        from_fn(assert_not_fixed),
        from_fn(By::<And![RoleId, UserId]>::delete::<UserRoleDB>),
        // refresh the authz cache (role membership changed)
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::user_role::UserRoleError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, ProjectIdName, ProjectName, RoleId, RoleName, SimplePermissionType,
        UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    async fn user_role_count(db: &TdPool, user: &UserId, role: &RoleId) -> usize {
        db.live()
            .fetch_all::<UserRoleDB>(())
            .await
            .unwrap()
            .into_iter()
            .filter(|ur| &ur.user_id == user && &ur.role_id == role)
            .count()
    }

    /// Create a project through the real create service so the per-project
    /// roles are minted.
    async fn create_project(db: &TdPool, name: &str) -> Result<(), TdError> {
        use crate::project::service::create::CreateProjectService;
        use td_objects::dxo::project::ProjectCreate;
        let create = ProjectCreate::builder()
            .name(&ProjectName::try_from(name)?)
            .description(&td_objects::types::basic::Description::try_from("d")?)
            .build()?;
        CreateProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
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
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;
        let dev_role = db
            .live()
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_dev")?)
            .await?;
        db.seed().user_role(&user.id, &dev_role.id).await?;
        assert_eq!(user_role_count(&db, &user.id, &dev_role.id).await, 1);

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::DevelopProject)
            .project(Some(ProjectIdName::from_str("p0")?))
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = RevokeSimplePermissionService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        assert_eq!(user_role_count(&db, &user.id, &dev_role.id).await, 0);
        Ok(())
    }

    /// Revoking a permission the user does not hold reports a clear "not
    /// found" error rather than the opaque `user_role` row-not-found SQL error.
    #[td_test::test(db)]
    async fn test_revoke_not_granted_reports_not_found(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::CreateProjects)
            .project(None)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = RevokeSimplePermissionService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            SqlError::CouldNotFindEntity(_, _) => {}
            other => panic!("Expected 'CouldNotFindEntity', got {other:?}"),
        })
        .await;
        Ok(())
    }

    /// The `admin` user's `sys_admin` membership is fixed and cannot be revoked
    /// (reuses the user_role delete guard).
    #[td_test::test(db)]
    async fn test_revoke_fixed_membership_rejected(db: TdPool) -> Result<(), TdError> {
        let grant = SimplePermissionGrant::builder()
            .user(UserName::admin())
            .permission(SimplePermissionType::ManageSystem)
            .project(None)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(grant);

        let service = RevokeSimplePermissionService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            UserRoleError::FixedUserRole(_, _) => {}
            other => panic!("Expected FixedUserRole, got {other:?}"),
        })
        .await;
        Ok(())
    }
}
