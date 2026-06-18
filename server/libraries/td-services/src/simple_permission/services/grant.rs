//
// Copyright 2026 Tabs Data Inc.
//

//! Grant a flat Simple permission to a user (`td permission grant`).
//!
//! Translation:
//!
//! | permission                         | membership add                 | authz scope |
//! |------------------------------------|--------------------------------|-------------|
//! | `view/develop/manage_project` on P | `<P>_view`/`_dev`/`_admin`     | project P   |
//! | `create_projects`                  | `project_creator`              | system      |
//! | `manage_system`                    | `sys_admin`                    | system      |
//!
//! Authz: a project permission requires `manage_access` (system) OR
//! `manage_project` on P; a system permission requires `manage_access`.

use crate::permission::authorize_permission_target;
use crate::simple_permission::layers::{
    ResolvedGrant, assert_simple_permission_valid, resolve_grant, resolved_role_id,
    resolved_user_id,
};
use ta_services::factory::service_factory;
use td_authz::{AuthzContext, refresh_authz_context};
use td_objects::dxo::crudl::{CreateRequest, RequestContext};
use td_objects::dxo::simple_permission::SimplePermissionGrant;
use td_objects::dxo::user_role::{UserRoleDB, UserRoleDBBuilder};
use td_objects::tower_service::with::{With, builder};
use td_objects::types::basic::{RoleId, UserId};
use td_queries::insert;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = GrantSimplePermissionService,
    request = CreateRequest<(), SimplePermissionGrant>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), SimplePermissionGrant>>::extract::<RequestContext>),
        from_fn(
            With::<CreateRequest<(), SimplePermissionGrant>>::extract_data::<SimplePermissionGrant>
        ),
        // validate (reject org-wide wildcards; project required/forbidden)
        from_fn(assert_simple_permission_valid),
        // resolve the target role membership (and project for authz)
        from_fn(resolve_grant),
        // authz by target: project perm -> manage_access | manage_project on P;
        // system perm -> manage_access on the system (shared with the raw API).
        from_fn(authorize_permission_target::<ResolvedGrant>),
        // membership add (same as user_role create; a duplicate surfaces the
        // user_role create unique-constraint error).
        from_fn(builder::<UserRoleDB>),
        from_fn(resolved_role_id),
        from_fn(With::<RoleId>::set::<UserRoleDBBuilder>),
        from_fn(resolved_user_id),
        from_fn(With::<UserId>::set::<UserRoleDBBuilder>),
        from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
        from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB>),
        from_fn(insert::<UserRoleDB>),
        // refresh the authz cache (role membership changed)
        from_fn(refresh_authz_context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simple_permission::SimplePermissionError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::Buildable;
    use td_objects::dxo::user_role::UserRoleDB;
    use td_objects::errors::SqlError;
    use td_objects::types::basic::{
        AccessTokenId, EntityId, EntityName, PermissionType, ProjectIdName, ProjectName, RoleId,
        RoleName, SimplePermissionType, UserEnabled, UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    fn grant_request(
        user: UserId,
        _role: RoleId,
        grant: SimplePermissionGrant,
    ) -> CreateRequest<(), SimplePermissionGrant> {
        RequestContext::with(AccessTokenId::default(), user).create((), grant)
    }

    /// Create a project through the real create service so the per-project
    /// roles (`<name>_admin`/`_dev`/`_view`) are minted — `db.seed().project`
    /// alone does not mint them.
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
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::DevelopProject)
            .project(Some(ProjectIdName::from_str("p0")?))
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.raw_oneshot(request).await?;

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
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::CreateProjects)
            .project(None)
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.raw_oneshot(request).await?;

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
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::ManageSystem)
            .project(None)
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        service.raw_oneshot(request).await?;

        assert_eq!(
            user_role_count(&db, &user.id, &RoleId::sys_admin()).await,
            1
        );
        Ok(())
    }

    /// The system user is excluded from grant resolution: targeting it by name
    /// resolves to no user, so the grant fails with a not-found error.
    #[td_test::test(db)]
    async fn test_grant_to_system_user_rejected(db: TdPool) -> Result<(), TdError> {
        let grant = SimplePermissionGrant::builder()
            .user(UserName::system())
            .permission(SimplePermissionType::ManageSystem)
            .project(None)
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(service, request, |err| match err {
            SqlError::RowNotFound(_, _) => {}
            other => panic!("Expected 'RowNotFound', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_view_all_projects_rejected(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::ViewAllProjects)
            .project(None)
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(service, request, |err| match err {
            SimplePermissionError::RbacOnlyWildcard(_) => {}
            other => panic!("Expected 'RbacOnlyWildcard', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_project_permission_requires_project(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::DevelopProject)
            .project(None)
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(service, request, |err| match err {
            SimplePermissionError::ProjectRequired(_) => {}
            other => panic!("Expected 'ProjectRequired', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_grant_system_permission_forbids_project(db: TdPool) -> Result<(), TdError> {
        db.seed().project(ProjectName::try_from("p0")?).await?;
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("joaquin")?)
            .permission(SimplePermissionType::CreateProjects)
            .project(Some(ProjectIdName::from_str("p0")?))
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(UserId::admin(), RoleId::sys_admin(), grant);
        assert_service_error(service, request, |err| match err {
            SimplePermissionError::ProjectForbidden(_) => {}
            other => panic!("Expected 'ProjectForbidden', got {other:?}"),
        })
        .await;
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
            .fetch_one::<td_objects::dxo::project::ProjectDB>(&ProjectName::try_from("p0")?)
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
            .user(&UserName::try_from("requester")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&requester.id, &dev_only.id).await?;

        let target = db
            .seed()
            .user(&UserName::try_from("target")?, &UserEnabled::from(true))
            .await?;

        let grant = SimplePermissionGrant::builder()
            .user(UserName::try_from("target")?)
            .permission(SimplePermissionType::ViewProject)
            .project(Some(ProjectIdName::from_str("p0")?))
            .build()?;

        let service = GrantSimplePermissionService::with_defaults(db.clone()).service();
        let request = grant_request(requester.id, dev_only.id, grant);
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
        })
        .await;
        let _ = target;
        Ok(())
    }
}
