//
// Copyright 2026 Tabsdata Inc.
//

use crate::permission::authorize_permission_target;
use crate::simple_permission::{assert_simple_permission_valid, resolve_grant};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::RbacSource;
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::Fixed;
use td_queries::LiveEntry;

#[service_factory]
async fn grant_simple_permission(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<SimplePermissionGrant>,
) -> Result<(), TdError> {
    let (ctx, _, grant) = request.into_parts();
    let token = authz.none();
    let mut trx = db.begin().await?;
    let conn = trx.get(token);

    // validate (reject org-wide wildcards; project required/forbidden)
    assert_simple_permission_valid(&grant)?;
    // resolve the target role membership (and project for authz)
    let resolved = resolve_grant(conn, &grant).await?;
    // authz by target: project perm -> manage_access | manage_project on P;
    // system perm -> manage_access on the system (shared with the raw API).
    authorize_permission_target(&authz, conn, &ctx, &resolved).await?;

    // a duplicate grant reports the membership already exists instead of
    // surfacing the raw user_role unique-constraint SQL error.
    conn.live()
        .assert_not_exists::<UserRoleDB>(and!(&resolved.role_id, &resolved.user_id))
        .await?;
    // membership add (same as user_role create).
    let user_role = UserRoleDB::new(resolved.user_id, resolved.role_id, &ctx, Fixed::FALSE);
    conn.live().insert(&user_role).await?;
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
    use crate::simple_permission::SimplePermissionError;
    use std::str::FromStr;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, EntityId, EntityName, PermissionType, ProjectIdName, ProjectName,
        RoleId, RoleName, SimplePermissionType, UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    fn grant_request(
        user: UserId,
        _role: RoleId,
        grant: SimplePermissionGrant,
    ) -> CreateRequest<SimplePermissionGrant> {
        RequestContext::with(AccessTokenId::default(), user).create((), grant)
    }

    /// Create a project through the real create service so the per-project
    /// roles (`<name>_admin`/`_dev`/`_view`) are minted — `db.seed().project`
    /// alone does not mint them.
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

    async fn user_role_count(db: &TdPool, user: &UserId, role: &RoleId) -> usize {
        db.live()
            .fetch_all::<UserRoleDB>(())
            .await
            .unwrap()
            .into_iter()
            .filter(|ur| &ur.user_id == user && &ur.role_id == role)
            .count()
    }

    #[td_test::test(db)]
    async fn test_grant_develop_project_adds_to_dev_role(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::DevelopProject,
            project: Some(ProjectIdName::from_str("p0")?),
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.grant_simple_permission(request).await?;

        // resolved to `p0_dev`
        let dev_role = db
            .live()
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_dev")?)
            .await?;
        assert_eq!(user_role_count(&db, &user.id, &dev_role.id).await, 1);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_create_projects_adds_to_project_creator(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::CreateProjects,
            project: None,
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.grant_simple_permission(request).await?;

        assert_eq!(
            user_role_count(&db, &user.id, &RoleId::project_creator()).await,
            1
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_manage_system_adds_to_sys_admin(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::ManageSystem,
            project: None,
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.grant_simple_permission(request).await?;

        assert_eq!(
            user_role_count(&db, &user.id, &RoleId::sys_admin()).await,
            1
        );
        Ok(())
    }

    /// Granting a permission the user already holds reports a clear
    /// "already exists" error rather than the opaque `user_role`
    /// unique-constraint SQL error from the insert.
    #[td_test::test(db)]
    async fn test_grant_already_granted_reports_already_exists(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = || -> Result<SimplePermissionGrant, TdError> {
            Ok(SimplePermissionGrant {
                user: UserName::try_from("joaquin")?,
                permission: SimplePermissionType::CreateProjects,
                project: None,
            })
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant()?);
        service.grant_simple_permission(request).await?;

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant()?);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                SqlError::EntityAlreadyExists(_, _) => {}
                other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
            },
        );
        Ok(())
    }

    /// The system user is excluded from grant resolution: targeting it by name
    /// resolves to no user, so the grant fails with a not-found error.
    #[td_test::test(db)]
    async fn test_grant_to_system_user_rejected(db: TdPool) -> Result<(), TdError> {
        let grant = SimplePermissionGrant {
            user: UserName::system(),
            permission: SimplePermissionType::ManageSystem,
            project: None,
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                SqlError::RowNotFound(_, _) => {}
                other => panic!("Expected 'RowNotFound', got {other:?}"),
            },
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_view_all_projects_rejected(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::ViewAllProjects,
            project: None,
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                SimplePermissionError::RbacOnlyWildcard(_) => {}
                other => panic!("Expected 'RbacOnlyWildcard', got {other:?}"),
            },
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_project_permission_requires_project(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::DevelopProject,
            project: None,
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                SimplePermissionError::ProjectRequired(_) => {}
                other => panic!("Expected 'ProjectRequired', got {other:?}"),
            },
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_system_permission_forbids_project(db: TdPool) -> Result<(), TdError> {
        db.seed().project(ProjectName::try_from("p0")?).await?;
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("joaquin")?,
            permission: SimplePermissionType::CreateProjects,
            project: Some(ProjectIdName::from_str("p0")?),
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                SimplePermissionError::ProjectForbidden(_) => {}
                other => panic!("Expected 'ProjectForbidden', got {other:?}"),
            },
        );
        Ok(())
    }

    /// A requester holding only `develop_project` on the project (not
    /// `manage_project`/`manage_access`) cannot grant — forbidden.
    #[td_test::test(db)]
    async fn test_grant_project_permission_forbidden_for_non_manager(
        db: TdPool,
    ) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let p0 = db
            .live()
            .fetch_one::<td_objects::dxo::project::ActiveProjectDB>(&ProjectName::try_from("p0")?)
            .await?;
        // requester only develops p0 (via a custom role), so cannot manage access.
        let dev_only = db
            .seed()
            .role(
                RoleName::try_from("dev_only")?,
                td_objects::types::basic::Description::try_from("d")?,
            )
            .await?;
        let entity_id: EntityId = p0.id.try_into()?;
        db.seed()
            .permission(
                PermissionType::develop_project(),
                Some(EntityName::try_from("p0")?),
                Some(entity_id),
                &dev_only,
            )
            .await?;
        let requester = db
            .seed()
            .user(&UserName::try_from("requester")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&requester.id, &dev_only.id).await?;

        let target = db
            .seed()
            .user(&UserName::try_from("target")?, &UserEnabled::TRUE)
            .await?;

        let grant = SimplePermissionGrant {
            user: UserName::try_from("target")?,
            permission: SimplePermissionType::ViewProject,
            project: Some(ProjectIdName::from_str("p0")?),
        };

        let service = Services::with_defaults(db.clone());
        let request = grant_request(requester.id, dev_only.id, grant);
        assert_service_error(
            service.grant_simple_permission(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
            },
        );
        let _ = target;
        Ok(())
    }
}
