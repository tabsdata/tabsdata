//
// Copyright 2026 Tabs Data Inc.
//

//! Smoke tests for the projects CRUDL — one end-to-end pass through every
//! service (create -> read -> list -> update -> delete) so the scaffold is
//! exercised on real Postgres (and proves the bootstrap-seeded sandbox project
//! shows up in list).

use crate::project::service::create::CreateProjectService;
use crate::project::service::delete::DeleteProjectService;
use crate::project::service::list::ListProjectsService;
use crate::project::service::read::ReadProjectService;
use crate::project::service::update::UpdateProjectService;
use ta_services::service::TdService;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::permission::PermissionDB;
use td_objects::errors::SqlError;
use td_objects::dxo::project::{ProjectCreate, ProjectUpdate};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::rest_urls::ProjectParam;
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, EntityId, PermissionEntityType, PermissionType,
    ProjectId, ProjectIdName, ProjectName, RoleKind, RoleName, UserEnabled, UserId, UserName,
};
use td_queries::LiveEntry;
use td_queries::test_utils::SeedEntry;
use td_tower::ctx_service::RawOneshot;

fn admin_ctx() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn project_param(name: &str) -> ProjectParam {
    ProjectParam::builder()
        .project(ProjectIdName::Name(ProjectName::try_from(name).unwrap()))
        .build()
        .unwrap()
}

#[td_test::test(db)]
async fn test_create_read_update_delete_project(db: TdPool) -> Result<(), TdError> {
    let initial_name = ProjectName::try_from("payments")?;
    let updated_name = ProjectName::try_from("payments_v2")?;
    let initial_desc = Description::try_from("Payments project")?;
    let updated_desc = Description::try_from("Payments project (renamed)")?;

    // Create with an explicit base_path (the default would be `/`).
    let base_path = BasePath::try_from("/payments")?;
    let create = ProjectCreate::builder()
        .name(&initial_name)
        .description(&initial_desc)
        .base_path(&base_path)
        .build()?;
    let created = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create))
        .await?;
    assert_eq!(created.name, initial_name);
    assert_eq!(created.description, initial_desc);
    assert_eq!(created.base_path, base_path);

    // Read by name.
    let read = ReadProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().read(project_param("payments")))
        .await?;
    assert_eq!(read.id, created.id);
    assert_eq!(
        read.base_path, base_path,
        "base_path round-trips through read"
    );

    // Update.
    let upd = ProjectUpdate::builder()
        .name(Some(updated_name.clone()))
        .description(Some(updated_desc.clone()))
        .build()?;
    let updated = UpdateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().update(project_param("payments"), upd))
        .await?;
    assert_eq!(updated.name, updated_name);
    assert_eq!(updated.description, updated_desc);

    // Delete (soft).
    DeleteProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().delete(project_param("payments_v2")))
        .await?;

    // After delete the row is gone from the active view: list shows only
    // sandbox (the bootstrap seed).
    let list = ListProjectsService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().list((), ListParams::default()))
        .await?;
    assert_eq!(list.len, 1, "only sandbox remains after delete");
    assert_eq!(list.data[0].name, ProjectName::try_from("sandbox")?);

    Ok(())
}

/// `project list` is filtered to the projects the requester may view: a
/// non-admin sees only projects it holds a grant on; a sys_admin (manage_project
/// on `__all__`) sees every project.
#[td_test::test(db)]
async fn test_list_filters_to_visible_projects(db: TdPool) -> Result<(), TdError> {
    // Two extra projects beside the bootstrap `sandbox` (seeded directly: the
    // create service is Postgres-only, but listing is backend-agnostic).
    let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
    let _beta = db.seed().project(ProjectName::try_from("beta")?).await?;

    // A non-admin user: member of `default` (its `create_projects` grant gates
    // the list endpoint) plus a role granting `view_project` on `alpha` only.
    let bob = db
        .seed()
        .user(&UserName::try_from("bob")?, &UserEnabled::from(true))
        .await?;
    let default_role = db
        .live()
        .fetch_one::<RoleDB>(&RoleName::try_from("default")?)
        .await?;
    db.seed().user_role(&bob.id, &default_role.id).await?;
    let alpha_viewers = db
        .seed()
        .role(
            RoleName::try_from("alpha_viewers")?,
            Description::try_from("alpha viewers")?,
        )
        .await?;
    db.seed()
        .permission(
            PermissionType::view_project(),
            None,
            Some(EntityId::try_from(alpha.id)?),
            &alpha_viewers,
        )
        .await?;
    db.seed().user_role(&bob.id, &alpha_viewers.id).await?;

    // Bob sees only `alpha`.
    let bob_ctx = RequestContext::with(AccessTokenId::default(), bob.id);
    let bob_list = ListProjectsService::with_defaults(db.clone())
        .service()
        .raw_oneshot(bob_ctx.list((), ListParams::default()))
        .await?;
    let bob_names: Vec<_> = bob_list.data.iter().map(|p| p.name.to_string()).collect();
    assert_eq!(
        bob_names,
        vec!["alpha".to_string()],
        "bob should see only alpha"
    );

    // The admin (sys_admin) sees all three (sandbox, alpha, beta).
    let admin_list = ListProjectsService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().list((), ListParams::default()))
        .await?;
    assert_eq!(admin_list.len, 3, "admin should see every project");

    Ok(())
}

/// Project names are unique within the registry (DB-level constraint): a
/// second create with the same name fails. After a soft delete the name is
/// freed because the row is renamed to `deleted_<id>`.
#[td_test::test(db)]
async fn test_project_name_must_be_unique(db: TdPool) -> Result<(), TdError> {
    let name = ProjectName::try_from("billing")?;
    let create = || {
        ProjectCreate::builder()
            .name(&name)
            .description(Description::try_from("Billing").unwrap())
            .build()
            .unwrap()
    };

    // First create succeeds.
    CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create()))
        .await?;

    // Second create with the same name fails up front with a clean
    // `EntityAlreadyExists` (via the `By::<ProjectName>::assert_not_exists`
    // pre-check) rather than the raw unique-constraint violation.
    let second = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create()))
        .await;
    let err = second.err().expect("duplicate project name must be rejected");
    assert!(
        matches!(err.domain_err::<SqlError>(), SqlError::EntityAlreadyExists(_, _)),
        "expected EntityAlreadyExists, got {err:?}"
    );

    // Soft-delete frees the name: the row is renamed to `deleted_<id>` so the
    // unique constraint on `name` no longer blocks reuse.
    DeleteProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().delete(project_param("billing")))
        .await?;
    CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create()))
        .await?;
    Ok(())
}

/// Renaming a project to a name already taken by another project is rejected
/// up front with a clean `EntityAlreadyExists` (via the
/// `assert_project_rename_available` pre-check) rather than the raw
/// unique-constraint violation from the update write.
#[td_test::test(db)]
async fn test_rename_to_existing_name_rejected(db: TdPool) -> Result<(), TdError> {
    // A second project alongside the seeded `sandbox`.
    CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(
            admin_ctx().create(
                (),
                ProjectCreate::builder()
                    .name(&ProjectName::try_from("billing")?)
                    .description(&Description::try_from("Billing")?)
                    .build()?,
            ),
        )
        .await?;

    // Renaming `sandbox` to the taken name `billing` collides.
    let upd = ProjectUpdate::builder()
        .name(Some(ProjectName::try_from("billing")?))
        .description(None)
        .build()?;
    let res = UpdateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().update(project_param("sandbox"), upd))
        .await;
    let err = res.err().expect("rename to an existing name must be rejected");
    assert!(
        matches!(err.domain_err::<SqlError>(), SqlError::EntityAlreadyExists(_, _)),
        "expected EntityAlreadyExists, got {err:?}"
    );
    Ok(())
}

/// Sandbox can be renamed via update (any backend) — deleting sandbox is
/// blocked on every backend, and creating additional projects is blocked on
/// single-project backends.
#[td_test::test(db)]
async fn test_sandbox_can_be_renamed(db: TdPool) -> Result<(), TdError> {
    let new_name = ProjectName::try_from("primary")?;
    let upd = ProjectUpdate::builder()
        .name(Some(new_name.clone()))
        .description(None)
        .build()?;
    let updated = UpdateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().update(project_param("sandbox"), upd))
        .await?;
    assert_eq!(updated.name, new_name);
    Ok(())
}

/// Helper: fetch the per-project roles minted for `project_id`, indexed by
/// kind. Returns the three `RoleDB` rows whose `project_id` is the project.
async fn project_roles(db: &TdPool, project_id: ProjectId) -> Result<Vec<RoleDB>, TdError> {
    let by_project: Option<ProjectId> = Some(project_id);
    Ok(db.live().fetch_all::<RoleDB>(by_project).await?)
}

/// Creating a project mints exactly three system-managed per-project roles
/// (`<name>_admin/_dev/_view`), each with the right kind, `project_id`,
/// `fixed = true`, and exactly one `fixed` permission on the project; and adds
/// the creating user to `<name>_admin` (membership NOT fixed).
#[td_test::test(db)]
async fn test_create_mints_per_project_roles(db: TdPool) -> Result<(), TdError> {
    let name = ProjectName::try_from("rocket")?;
    let create = ProjectCreate::builder()
        .name(&name)
        .description(Description::try_from("Rocket project")?)
        .build()?;
    let created = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create))
        .await?;

    let roles = project_roles(&db, created.id).await?;
    assert_eq!(roles.len(), 3, "exactly three per-project roles are minted");

    let entity_id = EntityId::try_from(created.id)?;
    let expected = [
        (
            RoleKind::ProjectAdmin,
            "rocket_admin",
            PermissionType::manage_project(),
        ),
        (
            RoleKind::ProjectDev,
            "rocket_dev",
            PermissionType::develop_project(),
        ),
        (
            RoleKind::ProjectView,
            "rocket_view",
            PermissionType::view_project(),
        ),
    ];

    for (kind, role_name, permission_type) in expected {
        let role = roles
            .iter()
            .find(|r| r.name == RoleName::try_from(role_name).unwrap())
            .unwrap_or_else(|| panic!("missing role {role_name}"));
        assert_eq!(role.kind, kind, "{role_name} kind");
        assert_eq!(role.project_id, Some(created.id), "{role_name} project_id");
        assert!(*role.fixed, "{role_name} must be system-managed (fixed)");

        // Exactly one permission row, fixed, on the project entity.
        let perms = db.live().fetch_all::<PermissionDB>(role.id).await?;
        assert_eq!(perms.len(), 1, "{role_name} has exactly one permission");
        let perm = &perms[0];
        assert_eq!(
            perm.permission_type, permission_type,
            "{role_name} permission"
        );
        assert_eq!(perm.entity_type, PermissionEntityType::project());
        assert_eq!(
            perm.entity_id, entity_id,
            "{role_name} entity is the project"
        );
        assert!(*perm.fixed, "{role_name} permission must be fixed");
    }

    // Creating user (admin) is a member of `<name>_admin`, NOT fixed.
    let admin_role = roles
        .iter()
        .find(|r| r.kind == RoleKind::ProjectAdmin)
        .expect("admin role");
    let memberships = db.live().fetch_all::<UserRoleDB>(admin_role.id).await?;
    assert_eq!(memberships.len(), 1, "one creator membership on admin role");
    assert_eq!(memberships[0].user_id, UserId::admin());
    assert!(
        !*memberships[0].fixed,
        "per-project membership is user-managed (not fixed)"
    );

    // `_dev` / `_view` start empty.
    for kind in [RoleKind::ProjectDev, RoleKind::ProjectView] {
        let role = roles.iter().find(|r| r.kind == kind).unwrap();
        let memberships = db.live().fetch_all::<UserRoleDB>(role.id).await?;
        assert!(memberships.is_empty(), "{kind:?} role starts empty");
    }

    Ok(())
}

/// Deleting a project cascades: its three per-project roles and all their
/// permission + membership rows are hard-removed (so the names are freed),
/// even though the role/permission rows are `fixed = true`.
#[td_test::test(db)]
async fn test_delete_cascades_per_project_roles(db: TdPool) -> Result<(), TdError> {
    let name = ProjectName::try_from("comet")?;
    let create = ProjectCreate::builder()
        .name(&name)
        .description(Description::try_from("Comet project")?)
        .build()?;
    let created = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create))
        .await?;

    // Collect the role ids before delete.
    let roles = project_roles(&db, created.id).await?;
    assert_eq!(roles.len(), 3);
    let role_ids: Vec<_> = roles.iter().map(|r| r.id).collect();

    DeleteProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().delete(project_param("comet")))
        .await?;

    // Role rows for the project are gone.
    let roles_after = project_roles(&db, created.id).await?;
    assert!(
        roles_after.is_empty(),
        "per-project roles dropped on delete"
    );

    // Their permission and membership rows are gone too.
    for role_id in role_ids {
        let perms = db.live().fetch_all::<PermissionDB>(role_id).await?;
        assert!(perms.is_empty(), "permissions cascaded");
        let memberships = db.live().fetch_all::<UserRoleDB>(role_id).await?;
        assert!(memberships.is_empty(), "memberships cascaded");
    }

    // The freed names can be reused: re-creating `comet` mints them again.
    let create = ProjectCreate::builder()
        .name(ProjectName::try_from("comet")?)
        .description(Description::try_from("Comet again")?)
        .build()?;
    let recreated = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create))
        .await?;
    let roles = project_roles(&db, recreated.id).await?;
    assert_eq!(roles.len(), 3, "re-create mints the roles again");

    Ok(())
}

#[cfg(sqlite)]
mod sqlite_single_project {
    use super::*;
    use crate::project::ProjectError;
    use td_error::TdDomainError;

    /// SQLite is single-project: creating new projects is rejected.
    #[tokio::test]
    async fn test_create_rejected_on_sqlite() {
        let db = td_database::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        let create = ProjectCreate::builder()
            .name(ProjectName::try_from("anything").unwrap())
            .description(Description::try_from("nope").unwrap())
            .build()
            .unwrap();
        let err = CreateProjectService::with_defaults(db.serving.clone())
            .service()
            .raw_oneshot(admin_ctx().create((), create))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            ProjectError::SingleProjectBackend.code(),
            "got {}",
            err.code()
        );
    }

    /// SQLite: sandbox is the only project and cannot be deleted.
    #[tokio::test]
    async fn test_delete_sandbox_rejected_on_sqlite() {
        let db = td_database::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        let err = DeleteProjectService::with_defaults(db.serving.clone())
            .service()
            .raw_oneshot(admin_ctx().delete(project_param("sandbox")))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            ProjectError::CannotDeleteSandboxOnSingleProjectBackend.code(),
            "got {}",
            err.code()
        );
    }

    /// SQLite: sandbox can still be renamed via update.
    #[tokio::test]
    async fn test_update_sandbox_allowed_on_sqlite() {
        let db = td_database::test_utils::sqlite::pool().await;
        db.upgrade().await.unwrap();
        let upd = ProjectUpdate::builder()
            .name(Some(ProjectName::try_from("primary").unwrap()))
            .description(None)
            .build()
            .unwrap();
        UpdateProjectService::with_defaults(db.serving.clone())
            .service()
            .raw_oneshot(admin_ctx().update(project_param("sandbox"), upd))
            .await
            .unwrap();
    }
}

/// PostgreSQL-only delete guards: sandbox is protected on every backend, and a
/// project that still owns collections cannot be deleted (additional projects
/// only exist on Postgres, so these can only be exercised there).
#[cfg(postgres)]
mod postgres_delete_guards {
    use super::*;
    use crate::Context;
    use crate::collection::service::CollectionServices;
    use crate::project::ProjectError;
    use ta_services::factory::ServiceFactory;
    use td_database::pool::scoped::InProject;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_error::TdDomainError;
    use td_objects::dxo::collection::CollectionCreate;
    use td_objects::types::basic::CollectionName;

    /// On Postgres sandbox is an ordinary project: deletable once empty (a
    /// fresh DB seeds it with no collections). Nothing in the schema requires
    /// it to keep existing.
    #[td_test::test(db)]
    async fn test_delete_sandbox_allowed_on_postgres(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        DeleteProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(admin_ctx().delete(project_param("sandbox")))
            .await?;
        // It's gone from the active registry.
        let list = ListProjectsService::with_defaults(db)
            .service()
            .raw_oneshot(admin_ctx().list((), ListParams::default()))
            .await?;
        assert!(
            !list
                .data
                .iter()
                .any(|p| p.name == ProjectName::try_from("sandbox").unwrap()),
            "sandbox should be gone after delete on postgres"
        );
        Ok(())
    }

    /// A project with a collection cannot be deleted (would strand the data);
    /// once emptied it can.
    #[td_test::test(db)]
    async fn test_delete_non_empty_project_rejected(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }

        // Create a project and a collection inside it (scoping the collection
        // create to the new project, the way the apiserver's resolver would).
        let create = ProjectCreate::builder()
            .name(&ProjectName::try_from("warehouse")?)
            .description(&Description::try_from("WH")?)
            .build()?;
        let project = CreateProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(admin_ctx().create((), create))
            .await?;

        let coll = CollectionCreate::builder()
            .name(&CollectionName::try_from("c0")?)
            .description(&Description::try_from("c0")?)
            .build()?;
        let collections = CollectionServices::build(&Context::with_defaults(db.clone()));
        collections
            .create
            .service()
            .raw_oneshot(admin_ctx().create((), coll).in_project(project.id))
            .await?;

        // Delete is refused while the project still owns a collection.
        let err = DeleteProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(admin_ctx().delete(project_param("warehouse")))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            ProjectError::ProjectNotEmpty(String::new()).code(),
            "got {}",
            err.code()
        );
        Ok(())
    }
}
