//
// Copyright 2026 Tabs Data Inc.
//

//! List a user's effective flat permissions (TEDB `td permission list
//! --user X`).
//!
//! Joins the user's `user_role` rows with `role_permission` and maps each row
//! to a Simple permission (TEDC "How a permission grant flows through"):
//!
//! - `(project:P, view/develop/manage_project)` -> that permission on `<Pname>`;
//! - `(project:__all__, manage_project)`        -> `admin_all_projects`;
//! - `(system, manage_access)`                  -> `manage_access`;
//! - `(system, create_projects)`                -> `create_projects`;
//! - `(system, manage_roles)`                       -> SKIP (internal, not user-facing);
//! - other `project:__all__` rows (org-wide view/develop wildcards) -> SKIP
//!   (no Simple-surface name; RBAC Mode View only).
//!
//! Results are de-duplicated (the same permission can come from several roles).

use crate::simple_permission::layers::flatten_user_permissions;
use ta_services::factory::service_factory;
use td_authz::rbac::{ManageAccess, ManageProject, OnAnyProject, Rbac, Scope, System};
use td_authz::AuthzContext;
use td_objects::dxo::crudl::{ListResponse, RequestContext};
use td_objects::dxo::simple_permission::{ListByUserRequest, SimplePermission};
use td_objects::tower_service::with::With;
use td_objects::types::basic::UserIdName;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListSimplePermissionByUserService,
    request = ListByUserRequest,
    response = ListResponse<SimplePermission>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListByUserRequest>::extract::<RequestContext>),
        // listing access is gated like the RBAC user-role listing surface:
        // manage_access on the system, or manage_project on any project.
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, (ManageAccess, OnAnyProject<ManageProject>)>::check),
        from_fn(With::<ListByUserRequest>::extract::<UserIdName>),
        from_fn(flatten_user_permissions),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::rest_urls::PermissionGrantUserQuery;
    use td_objects::types::basic::{
        AccessTokenId, ProjectName, RoleId, RoleName, SimplePermissionType, UserEnabled, UserId,
        UserName,
    };
    use td_objects::Buildable;
    use td_queries::test_utils::SeedEntry;
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    fn req(target: UserName) -> ListByUserRequest {
        ListByUserRequest::new(
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::sys_admin()),
            PermissionGrantUserQuery::builder()
                .user(UserIdName::Name(target))
                .build()
                .unwrap(),
        )
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
                RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::sys_admin())
                    .create((), create),
            )
            .await?;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_by_user_flattens_project_and_system(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;
        // develop p0 + create_projects
        let dev_role = db
            .live()
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_dev")?)
            .await?;
        db.seed().user_role(&user.id, &dev_role.id).await?;
        db.seed()
            .user_role(&user.id, &RoleId::project_creator())
            .await?;

        let service = ListSimplePermissionByUserService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(req(UserName::try_from("joaquin")?)).await?;

        let perms: Vec<_> = response.data.iter().map(|p| p.permission.clone()).collect();
        assert!(perms.contains(&SimplePermissionType::DevelopProject));
        assert!(perms.contains(&SimplePermissionType::CreateProjects));
        // develop on p0 carries the project name
        let dev = response
            .data
            .iter()
            .find(|p| p.permission == SimplePermissionType::DevelopProject)
            .unwrap();
        assert_eq!(dev.project.as_ref().unwrap().to_string(), "p0");
        // create_projects is system-scoped (no project)
        let create = response
            .data
            .iter()
            .find(|p| p.permission == SimplePermissionType::CreateProjects)
            .unwrap();
        assert!(create.project.is_none());
        Ok(())
    }

    /// A `sys_admin` member flattens to the system bundle: `manage_access`,
    /// `create_projects` and `admin_all_projects` (the `(project:__all__,
    /// manage_project)` row). `manage_roles` is skipped.
    #[td_test::test(db)]
    async fn test_list_by_user_sys_admin_bundle(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&user.id, &RoleId::sys_admin()).await?;

        let service = ListSimplePermissionByUserService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(req(UserName::try_from("joaquin")?)).await?;

        let perms: Vec<_> = response.data.iter().map(|p| p.permission.clone()).collect();
        assert!(perms.contains(&SimplePermissionType::ManageAccess));
        assert!(perms.contains(&SimplePermissionType::CreateProjects));
        assert!(perms.contains(&SimplePermissionType::AdminAllProjects));
        // manage_roles has no Simple-surface name and must not leak.
        assert!(perms.iter().all(|p| !matches!(
            p,
            SimplePermissionType::ViewAllProjects | SimplePermissionType::DevelopAllProjects
        )));
        Ok(())
    }
}
