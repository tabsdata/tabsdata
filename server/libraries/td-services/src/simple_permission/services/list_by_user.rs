//
// Copyright 2026 Tabsdata Inc.
//

use crate::simple_permission::flatten_user_permissions;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, SelfUser, User};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::ListResponse;
use td_objects::dxo::simple_permission::{ListByUserRequest, SimplePermission};
use td_objects::dxo::user::UserDB;
use td_queries::LiveEntry;

#[service_factory]
async fn list_simple_permission_by_user(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListByUserRequest,
) -> Result<ListResponse<SimplePermission>, TdError> {
    let conn = gated_conn.get(authz.none());
    // resolve the target user (errors NotFound for an unknown user) so the
    // listing can be scoped to it.
    let user: UserDB = conn.live().fetch_one(&request.user).await?;
    // accessible to a manage_access holder, or the target user themselves.
    let _grant = authz
        .require::<User, (SelfUser, ManageAccess)>(conn, &request.context, &user.id)
        .await?;
    flatten_user_permissions(conn, &user.id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::UserParam;
    use td_objects::types::basic::UserIdName;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, ProjectName, RoleId, RoleName, SimplePermissionType, UserEnabled,
        UserId, UserName,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    fn req_as(requester: UserId, target: UserName) -> ListByUserRequest {
        ListByUserRequest::new(
            RequestContext::with(AccessTokenId::default(), requester),
            UserParam {
                user: UserIdName::Name(target),
            },
        )
    }

    /// Requester is `admin` (holds `manage_access`).
    fn req(target: UserName) -> ListByUserRequest {
        req_as(UserId::admin(), target)
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
            .await?
            .data;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_list_by_user_flattens_project_and_system(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
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

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_simple_permission_by_user(req(UserName::try_from("joaquin")?))
            .await?
            .data;

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

    /// A `sys_admin` member flattens to the system bundle: `manage_system`
    /// (from the `manage_access` row) and `create_projects`. `manage_roles` and
    /// the org-wide `manage_project` row have no standalone Simple name.
    #[td_test::test(db)]
    async fn test_list_by_user_sys_admin_bundle(db: TdPool) -> Result<(), TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&user.id, &RoleId::sys_admin()).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_simple_permission_by_user(req(UserName::try_from("joaquin")?))
            .await?
            .data;

        let perms: Vec<_> = response.data.iter().map(|p| p.permission.clone()).collect();
        assert!(perms.contains(&SimplePermissionType::ManageSystem));
        assert!(perms.contains(&SimplePermissionType::CreateProjects));
        // manage_roles has no Simple-surface name and must not leak.
        assert!(perms.iter().all(|p| !matches!(
            p,
            SimplePermissionType::ViewAllProjects | SimplePermissionType::DevelopAllProjects
        )));
        Ok(())
    }

    /// The target user themselves can list their own permissions even without
    /// `manage_access`.
    #[td_test::test(db)]
    async fn test_list_by_user_self_access(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;
        let user = db
            .seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        let dev_role = db
            .live()
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_dev")?)
            .await?;
        db.seed().user_role(&user.id, &dev_role.id).await?;

        // requester == target (joaquin), who holds no manage_access.
        let service = Services::with_defaults(db.clone());
        let response = service
            .list_simple_permission_by_user(req_as(user.id, UserName::try_from("joaquin")?))
            .await?
            .data;

        let perms: Vec<_> = response.data.iter().map(|p| p.permission.clone()).collect();
        assert!(perms.contains(&SimplePermissionType::DevelopProject));
        Ok(())
    }

    /// A requester who is neither a `manage_access` holder nor the target user
    /// is forbidden from listing the target's permissions.
    #[td_test::test(db)]
    async fn test_list_by_user_forbidden_for_other_non_admin(db: TdPool) -> Result<(), TdError> {
        db.seed()
            .user(&UserName::try_from("joaquin")?, &UserEnabled::TRUE)
            .await?;
        let other = db
            .seed()
            .user(&UserName::try_from("other")?, &UserEnabled::TRUE)
            .await?;

        let service = Services::with_defaults(db.clone());
        let request = req_as(other.id, UserName::try_from("joaquin")?);
        assert_service_error(
            service.list_simple_permission_by_user(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
            },
        );
        Ok(())
    }
}
