//
// Copyright 2026 Tabs Data Inc.
//

//! List everyone with authority on a project (TEDB `td permission list
//! --project P`), with a source label (TEDC "Listing access on a project: two
//! sources"):
//!
//! - `source = project (<rolename>)` — members of `<P>_admin|_dev|_view`;
//! - `source = system (admin_all_projects)` — members of any role holding
//!   `(project:__all__, manage_project)`.

use crate::simple_permission::layers::list_project_access;
use ta_services::factory::service_factory;
use td_authz::rbac::{ManageAccess, ManageProject, Project, Rbac, Scope};
use td_authz::AuthzContext;
use td_objects::dxo::crudl::{ListResponse, RequestContext};
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::simple_permission::{ListByProjectRequest, SimpleProjectAccess};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListSimplePermissionByProjectService,
    request = ListByProjectRequest,
    response = ListResponse<SimpleProjectAccess>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListByProjectRequest>::extract::<RequestContext>),
        // resolve the project and authorize against it: manage_access (system)
        // OR manage_project on the project (a project admin can see the roster).
        from_fn(With::<ListByProjectRequest>::extract::<ProjectIdName>),
        from_fn(By::<ProjectIdName>::fetch_one::<ProjectDB>),
        from_fn(With::<ProjectDB>::extract::<ProjectId>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, (ManageAccess, ManageProject)>::check),
        from_fn(list_project_access),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_authz::rbac::RbacError;
    use td_database::pool::TdPool;
    use td_error::{assert_service_error, TdError};
    use td_objects::rest_urls::PermissionGrantProjectQuery;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, EntityName, PermissionType, ProjectName, RoleId,
        RoleName, SimplePermissionType, UserEnabled, UserId, UserName,
    };
    use td_objects::Buildable;
    use td_queries::test_utils::SeedEntry;
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    fn req(
        requester: UserId,
        role: RoleId,
        project: ProjectName,
    ) -> ListByProjectRequest {
        ListByProjectRequest::new(
            RequestContext::with(AccessTokenId::default(), requester),
            PermissionGrantProjectQuery::builder()
                .project(ProjectIdName::Name(project))
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
            .description(&Description::try_from("d")?)
            .build()?;
        CreateProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create((), create),
            )
            .await?;
        Ok(())
    }

    /// Both sources appear with their labels: a `<P>_admin` member as
    /// `project (p0_admin)` / `manage_project`, and an `admin_all_projects`
    /// holder (a `sys_admin` member) as `system (admin_all_projects)`.
    #[td_test::test(db)]
    async fn test_list_by_project_two_sources(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "p0").await?;

        // project-source: alice in p0_admin
        let alice = db
            .seed()
            .user(&UserName::try_from("alice")?, &UserEnabled::from(true))
            .await?;
        let admin_role = db
            .live()
            .fetch_one::<td_objects::dxo::role::RoleDB>(&RoleName::try_from("p0_admin")?)
            .await?;
        db.seed().user_role(&alice.id, &admin_role.id).await?;

        // system-source: carol in sys_admin (holds (project:__all__, manage_project))
        let carol = db
            .seed()
            .user(&UserName::try_from("carol")?, &UserEnabled::from(true))
            .await?;
        db.seed().user_role(&carol.id, &RoleId::sys_admin()).await?;

        let service = ListSimplePermissionByProjectService::with_defaults(db.clone()).service();
        let response = service
            .raw_oneshot(req(UserId::admin(), RoleId::sys_admin(), ProjectName::try_from("p0")?))
            .await?;

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
        assert_eq!(carol_row.permission, SimplePermissionType::AdminAllProjects);
        assert_eq!(carol_row.source, "system (admin_all_projects)");
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
        let entity_id: EntityId = p0.id.clone().try_into()?;
        db.seed()
            .permission(
                PermissionType::DevelopProject,
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

        let service = ListSimplePermissionByProjectService::with_defaults(db.clone()).service();
        let request = req(requester.id, dev_only.id, ProjectName::try_from("p0")?);
        assert_service_error(service, request, |err| match err {
            RbacError::Forbidden(_, _) => {}
            other => panic!("Expected 'Forbidden', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
