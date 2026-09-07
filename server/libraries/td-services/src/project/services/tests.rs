//
// Copyright 2026 Tabsdata Inc.
//

//! Smoke tests for the projects CRUDL — one end-to-end pass through every
//! service (create -> read -> list -> update -> delete) so the scaffold is
//! exercised on real Postgres.

use crate::Services;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::project::{ProjectCreate, ProjectUpdate};
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::errors::SqlError;
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, EntityId, GroupName, PermissionEntityType,
    PermissionType, ProjectId, ProjectIdName, ProjectName, RoleKind, RoleName, UserEnabled, UserId,
    UserName,
};
use td_queries::LiveEntry;
use td_queries::test_utils::SeedEntry;

fn admin_ctx() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

fn project_param(name: &str) -> ProjectParam {
    ProjectParam {
        project: ProjectIdName::Name(ProjectName::try_from(name).unwrap()),
    }
}

#[td_test::test(db)]
async fn test_create_read_update_delete_project(db: TdPool) -> Result<(), TdError> {
    let initial_name = ProjectName::try_from("payments")?;
    let updated_name = ProjectName::try_from("payments_v2")?;
    let initial_desc = Description::try_from("Payments project")?;
    let updated_desc = Description::try_from("Payments project (renamed)")?;

    // Create with an explicit base_path (the default would be `/`).
    let base_path = BasePath::try_from("/payments")?;
    let create = ProjectCreate {
        name: initial_name.clone(),
        description: initial_desc.clone(),
        base_path: base_path.clone(),
    };
    let created = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create))
        .await?
        .data;
    assert_eq!(created.name, initial_name);
    assert_eq!(created.description, initial_desc);
    assert_eq!(created.base_path, base_path);

    let read = Services::with_defaults(db.clone())
        .read_project(admin_ctx().read(project_param("payments")))
        .await?
        .data;
    assert_eq!(read.id, created.id);
    assert_eq!(
        read.base_path, base_path,
        "base_path round-trips through read"
    );

    let upd = ProjectUpdate {
        name: Some(updated_name.clone()),
        description: Some(updated_desc.clone()),
    };
    let updated = Services::with_defaults(db.clone())
        .update_project(admin_ctx().update(project_param("payments"), upd))
        .await?
        .data;
    assert_eq!(updated.name, updated_name);
    assert_eq!(updated.description, updated_desc);

    Services::with_defaults(db.clone())
        .delete_project(admin_ctx().delete(project_param("payments_v2")))
        .await?
        .data;

    // After delete the row is gone from the active view.
    let list = Services::with_defaults(db.clone())
        .list_projects(admin_ctx().list((), ListParams::default()))
        .await?
        .data;
    assert_eq!(list.len, 0, "no project remains after delete");

    Ok(())
}

/// `project list` is filtered to the projects the requester may view: a
/// non-admin sees only projects it holds a grant on; a sys_admin (manage_project
/// on `__all__`) sees every project.
#[td_test::test(db)]
async fn test_list_filters_to_visible_projects(db: TdPool) -> Result<(), TdError> {
    // Two projects (seeded directly: the create service is Postgres-only, but
    // listing is backend-agnostic).
    let alpha = db.seed().project(ProjectName::try_from("alpha")?).await?;
    let _beta = db.seed().project(ProjectName::try_from("beta")?).await?;

    // A non-admin user: member of `default` (its `create_projects` grant gates
    // the list endpoint) plus a role granting `view_project` on `alpha` only.
    let bob = db
        .seed()
        .user(&UserName::try_from("bob")?, &UserEnabled::TRUE)
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
    let bob_list = Services::with_defaults(db.clone())
        .list_projects(bob_ctx.list((), ListParams::default()))
        .await?
        .data;
    let bob_names: Vec<_> = bob_list.data.iter().map(|p| p.name.to_string()).collect();
    assert_eq!(
        bob_names,
        vec!["alpha".to_string()],
        "bob should see only alpha"
    );

    // The admin (sys_admin) sees both.
    let admin_list = Services::with_defaults(db.clone())
        .list_projects(admin_ctx().list((), ListParams::default()))
        .await?
        .data;
    assert_eq!(admin_list.len, 2, "admin should see every project");

    Ok(())
}

/// Project names are unique within the registry (DB-level constraint): a
/// second create with the same name fails. After a soft delete the name is
/// freed because the row is renamed to `deleted_<id>`.
#[td_test::test(db)]
async fn test_project_name_must_be_unique(db: TdPool) -> Result<(), TdError> {
    let name = ProjectName::try_from("billing")?;
    let create = || ProjectCreate {
        name: name.clone(),
        description: Description::try_from("Billing").unwrap(),
        base_path: BasePath::default(),
    };

    Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create()))
        .await?
        .data;

    // Second create with the same name fails up front with a clean
    // `EntityAlreadyExists` (via the `By::<ProjectName>::assert_not_exists`
    // pre-check) rather than the raw unique-constraint violation.
    let second = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create()))
        .await;
    let err = second.expect_err("duplicate project name must be rejected");
    assert!(
        matches!(
            err.domain_err::<SqlError>(),
            SqlError::EntityAlreadyExists(_, _)
        ),
        "expected EntityAlreadyExists, got {err:?}"
    );

    // Soft-delete frees the name: the row is renamed to `deleted_<id>` so the
    // unique constraint on `name` no longer blocks reuse.
    Services::with_defaults(db.clone())
        .delete_project(admin_ctx().delete(project_param("billing")))
        .await?
        .data;
    Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create()))
        .await?
        .data;
    Ok(())
}

/// Renaming a project to a name already taken by another project is rejected
/// up front with a clean `EntityAlreadyExists` (via the
/// `assert_project_rename_available` pre-check) rather than the raw
/// unique-constraint violation from the update write.
#[td_test::test(db)]
async fn test_rename_to_existing_name_rejected(db: TdPool) -> Result<(), TdError> {
    db.seed().test_project().await?;
    Services::with_defaults(db.clone())
        .create_project(admin_ctx().create(
            (),
            ProjectCreate {
                name: ProjectName::try_from("billing")?,
                description: Description::try_from("Billing")?,
                base_path: BasePath::default(),
            },
        ))
        .await?
        .data;

    // Renaming `test` to the taken name `billing` collides.
    let upd = ProjectUpdate {
        name: Some(ProjectName::try_from("billing")?),
        description: None,
    };
    let res = Services::with_defaults(db.clone())
        .update_project(admin_ctx().update(project_param("test"), upd))
        .await;
    let err = res.expect_err("rename to an existing name must be rejected");
    assert!(
        matches!(
            err.domain_err::<SqlError>(),
            SqlError::EntityAlreadyExists(_, _)
        ),
        "expected EntityAlreadyExists, got {err:?}"
    );
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
    let create = ProjectCreate {
        name,
        description: Description::try_from("Rocket project")?,
        base_path: BasePath::default(),
    };
    let created = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create))
        .await?
        .data;

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
    let create = ProjectCreate {
        name,
        description: Description::try_from("Comet project")?,
        base_path: BasePath::default(),
    };
    let created = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create))
        .await?
        .data;

    // Collect the role ids before delete.
    let roles = project_roles(&db, created.id).await?;
    assert_eq!(roles.len(), 3);
    let role_ids: Vec<_> = roles.iter().map(|r| r.id).collect();

    Services::with_defaults(db.clone())
        .delete_project(admin_ctx().delete(project_param("comet")))
        .await?
        .data;

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
    let create = ProjectCreate {
        name: ProjectName::try_from("comet")?,
        description: Description::try_from("Comet again")?,
        base_path: BasePath::default(),
    };
    let recreated = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create))
        .await?
        .data;
    let roles = project_roles(&db, recreated.id).await?;
    assert_eq!(roles.len(), 3, "re-create mints the roles again");

    Ok(())
}

/// Renaming a project rewrites its per-project roles in place, freeing the old
/// names and keeping ids/kind/project_id/fixed.
#[td_test::test(db)]
async fn test_rename_renames_per_project_roles(db: TdPool) -> Result<(), TdError> {
    let create = ProjectCreate {
        name: ProjectName::try_from("orbit")?,
        description: Description::try_from("Orbit project")?,
        base_path: BasePath::default(),
    };
    let created = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create))
        .await?
        .data;

    let before = project_roles(&db, created.id).await?;
    let admin_before_id = before
        .iter()
        .find(|r| r.kind == RoleKind::ProjectAdmin)
        .expect("admin role")
        .id;

    let upd = ProjectUpdate {
        name: Some(ProjectName::try_from("galaxy")?),
        description: None,
    };
    Services::with_defaults(db.clone())
        .update_project(admin_ctx().update(project_param("orbit"), upd))
        .await?
        .data;

    let after = project_roles(&db, created.id).await?;
    assert_eq!(after.len(), 3, "still exactly three roles");

    let expected = [
        (
            RoleKind::ProjectAdmin,
            "galaxy_admin",
            "Project galaxy administrators",
        ),
        (
            RoleKind::ProjectDev,
            "galaxy_dev",
            "Project galaxy developers",
        ),
        (
            RoleKind::ProjectView,
            "galaxy_view",
            "Project galaxy viewers",
        ),
    ];
    for (kind, role_name, description) in expected {
        let role = after
            .iter()
            .find(|r| r.kind == kind)
            .unwrap_or_else(|| panic!("missing {kind:?} role"));
        assert_eq!(role.name, RoleName::try_from(role_name)?, "renamed name");
        assert_eq!(
            role.description,
            Description::try_from(description)?,
            "refreshed description"
        );
        assert_eq!(role.project_id, Some(created.id), "project_id unchanged");
        assert!(*role.fixed, "{role_name} still fixed");
    }

    let admin_after_id = after
        .iter()
        .find(|r| r.kind == RoleKind::ProjectAdmin)
        .unwrap()
        .id;
    assert_eq!(admin_after_id, admin_before_id, "role id preserved");

    for old in ["orbit_admin", "orbit_dev", "orbit_view"] {
        assert!(
            db.live()
                .fetch_optional::<RoleDB>(&RoleName::try_from(old)?)
                .await?
                .is_none(),
            "{old} freed"
        );
    }
    Ok(())
}

/// Regression: after a rename, creating a new project with the old name
/// succeeds (previously collided on the stranded `<name>_*` role names).
#[td_test::test(db)]
async fn test_rename_then_recreate_old_name(db: TdPool) -> Result<(), TdError> {
    let create_foo = || ProjectCreate {
        name: ProjectName::try_from("foo").unwrap(),
        description: Description::try_from("Foo").unwrap(),
        base_path: BasePath::default(),
    };

    Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create_foo()))
        .await?
        .data;

    let upd = ProjectUpdate {
        name: Some(ProjectName::try_from("bar")?),
        description: None,
    };
    Services::with_defaults(db.clone())
        .update_project(admin_ctx().update(project_param("foo"), upd))
        .await?
        .data;

    let new_foo = Services::with_defaults(db.clone())
        .create_project(admin_ctx().create((), create_foo()))
        .await?
        .data;
    let roles = project_roles(&db, new_foo.id).await?;
    assert_eq!(roles.len(), 3, "new foo mints its own three roles");
    Ok(())
}

/// Delete guards: a seeded project is deletable once empty, and a project that
/// still owns collections cannot be deleted.
mod delete_guards {
    use super::*;
    use crate::Services;
    use crate::project::ProjectError;
    use td_database::pool::scoped::ScopedExt;
    use td_error::TdDomainError;
    use td_objects::dxo::collection::CollectionCreate;
    use td_objects::types::basic::CollectionName;

    /// On Postgres a seeded project is ordinary: deletable once empty. Nothing
    /// in the schema requires it to keep existing.
    #[td_test::test(db)]
    async fn test_delete_seeded_project_allowed_on_postgres(db: TdPool) -> Result<(), TdError> {
        db.seed().test_project().await?;
        Services::with_defaults(db.clone())
            .delete_project(admin_ctx().delete(project_param("test")))
            .await?
            .data;
        // It's gone from the active registry.
        let list = Services::with_defaults(db)
            .list_projects(admin_ctx().list((), ListParams::default()))
            .await?
            .data;
        assert!(
            !list
                .data
                .iter()
                .any(|p| p.name == ProjectName::try_from("test").unwrap()),
            "the test project should be gone after delete on postgres"
        );
        Ok(())
    }

    /// A project with a collection cannot be deleted (would strand the data);
    /// once emptied it can.
    #[td_test::test(db)]
    async fn test_delete_non_empty_project_rejected(db: TdPool) -> Result<(), TdError> {
        // Create a project and a collection inside it (scoping the collection
        // create to the new project, the way the apiserver's resolver would).
        let create = ProjectCreate {
            name: ProjectName::try_from("warehouse")?,
            description: Description::try_from("WH")?,
            base_path: BasePath::default(),
        };
        let project = Services::with_defaults(db.clone())
            .create_project(admin_ctx().create((), create))
            .await?
            .data;

        let coll = CollectionCreate {
            name: CollectionName::try_from("c0")?,
            description: Description::try_from("c0")?,
            group: GroupName::default_group(),
            base_path: None,
            connection: None,
            secrets: None,
            vars: None,
        };
        let collections = Services::with_defaults(db.clone());
        collections
            .create_collection(admin_ctx().create((), coll).in_project(project.id))
            .await?
            .data;

        // Delete is refused while the project still owns a collection.
        let err = Services::with_defaults(db.clone())
            .delete_project(admin_ctx().delete(project_param("warehouse")))
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
