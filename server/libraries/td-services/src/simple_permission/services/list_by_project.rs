//
// Copyright 2026 Tabsdata Inc.
//

use crate::simple_permission::list_project_access;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, ManageProject, Project};
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::ListResponse;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::simple_permission::{ListByProjectRequest, SimpleProjectAccess};
use td_queries::Queries;

#[service_factory]
async fn list_simple_permission_by_project(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO>,
    request: ListByProjectRequest,
) -> Result<ListResponse<SimpleProjectAccess>, TdError> {
    // resolve the project and authorize against it: manage_access (system)
    // OR manage_project on the project (a project admin can see the roster).
    let conn = gated_conn.get(authz.none());
    let project: ActiveProjectDB = conn.fetch_one(&request.project).await?;
    let _grant = authz
        .require::<Project, (ManageAccess, ManageProject)>(conn, &request.context, &project.id)
        .await?;
    list_project_access(conn, &request.project).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::project::ProjectParam;
    use td_objects::types::basic::ProjectIdName;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, Description, EntityId, EntityName, PermissionType, ProjectName,
        RoleId, RoleName, SimplePermissionType, UserEnabled, UserId, UserName,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    fn req(requester: UserId, _role: RoleId, project: ProjectName) -> ListByProjectRequest {
        ListByProjectRequest::new(
            RequestContext::with(AccessTokenId::default(), requester),
            ProjectParam {
                project: ProjectIdName::Name(project),
            },
        )
    }

    /// Create a project through the real create service so the per-project
    /// roles are minted.
    async fn create_project(db: &TdPool, name: &str) -> Result<(), TdError> {
        use td_objects::dxo::project::ProjectCreate;
        let create = ProjectCreate {
            name: ProjectName::try_from(name)?,
            description: Description::try_from("d")?,
            base_path: BasePath::default(),
        };
        Services::with_defaults(db.clone())
            .create_project(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).create((), create),
            )
            .await?;
        Ok(())
    }

    /// Both sources appear with their labels: a `<P>_admin` member as
    /// `project (p0_admin)` / `manage_project`, and a `sys_admin` member as
    /// `manage_system` / `system (manage_system)`.
    #[td_test::test(db)]
    async fn test_list_by_project_two_sources(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;

        // project-source: alice in p0_admin
        let alice = db
            .seed()
            .user(&UserName::try_from("alice")?, &UserEnabled::TRUE)
            .await?;
        let admin_role = db
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_admin")?)
            .await?;
        db.seed().user_role(&alice.id, &admin_role.id).await?;

        // system-source: carol in sys_admin (holds (project:__all__, manage_project))
        let carol = db
            .seed()
            .user(&UserName::try_from("carol")?, &UserEnabled::TRUE)
            .await?;
        db.seed().user_role(&carol.id, &RoleId::sys_admin()).await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_simple_permission_by_project(req(
                UserId::admin(),
                RoleId::sys_admin(),
                ProjectName::try_from("p0")?,
            ))
            .await?
            .data;

        let alice_row = response
            .data
            .iter()
            .find(|r| r.user.to_string() == "alice")
            .expect("alice present");
        assert_eq!(alice_row.permission, SimplePermissionType::ManageProject);
        assert_eq!(alice_row.source, "project (p0_admin)");

        let carol_row = response
            .data
            .iter()
            .find(|r| r.user.to_string() == "carol")
            .expect("carol present");
        assert_eq!(carol_row.permission, SimplePermissionType::ManageSystem);
        assert_eq!(carol_row.source, "system (manage_system)");
        Ok(())
    }

    /// The system user is excluded from the roster even when it holds roles that
    /// would otherwise surface it — here both `p0_admin` (project source) and
    /// `sys_admin` (system / `manage_system` source).
    #[td_test::test(db)]
    async fn test_list_by_project_excludes_system_user(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;

        let admin_role = db
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_admin")?)
            .await?;
        db.seed()
            .user_role(&UserId::system(), &admin_role.id)
            .await?;
        db.seed()
            .user_role(&UserId::system(), &RoleId::sys_admin())
            .await?;

        let service = Services::with_defaults(db.clone());
        let response = service
            .list_simple_permission_by_project(req(
                UserId::admin(),
                RoleId::sys_admin(),
                ProjectName::try_from("p0")?,
            ))
            .await?
            .data;

        assert!(
            response.data.iter().all(|r| r.user != UserName::system()),
            "system user should be excluded from the project roster"
        );
        Ok(())
    }

    /// A requester holding only `develop_project` on the project cannot list
    /// its roster — forbidden (needs manage_project / manage_access).
    #[td_test::test(db)]
    async fn test_list_by_project_forbidden_for_non_manager(db: TdPool) -> Result<(), TdError> {
        let p0 = db.seed().project(ProjectName::try_from("p0")?).await?;
        let dev_only = db
            .seed()
            .role(RoleName::try_from("dev_only")?, Description::try_from("d")?)
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

        let service = Services::with_defaults(db.clone());
        let request = req(requester.id, dev_only.id, ProjectName::try_from("p0")?);
        assert_service_error(
            service.list_simple_permission_by_project(request).await,
            |err| match err {
                RbacError::Forbidden(_, _) => {}
                other => panic!("expected Forbidden, got {other:?}"),
            },
        );
        Ok(())
    }
}
