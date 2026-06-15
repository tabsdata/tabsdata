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
use td_objects::dxo::project::{ProjectCreate, ProjectUpdate};
use td_objects::rest_urls::ProjectParam;
use td_objects::types::basic::{
    AccessTokenId, Description, ProjectIdName, ProjectName, RoleId, UserId,
};
use td_tower::ctx_service::RawOneshot;

fn admin_ctx() -> RequestContext {
    RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    )
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

    // Create.
    let create = ProjectCreate::builder()
        .name(&initial_name)
        .description(&initial_desc)
        .build()?;
    let created = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create))
        .await?;
    assert_eq!(created.name, initial_name);
    assert_eq!(created.description, initial_desc);

    // Read by name.
    let read = ReadProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().read(project_param("payments")))
        .await?;
    assert_eq!(read.id, created.id);

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

    // Second create with the same name fails (unique constraint).
    let second = CreateProjectService::with_defaults(db.clone())
        .service()
        .raw_oneshot(admin_ctx().create((), create()))
        .await;
    assert!(second.is_err(), "duplicate project name must be rejected");

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

#[cfg(sqlite)]
mod sqlite_single_project {
    use super::*;
    use crate::project::ProjectError;
    use td_database::schema::{ApiserverSchemaManager, SchemaManager};
    use td_error::TdDomainError;

    /// SQLite is single-project: creating new projects is rejected.
    #[tokio::test]
    async fn test_create_rejected_on_sqlite() {
        let db = td_database::test_utils::sqlite::pool().await;
        ApiserverSchemaManager.upgrade(&db).await.unwrap();
        let create = ProjectCreate::builder()
            .name(ProjectName::try_from("anything").unwrap())
            .description(Description::try_from("nope").unwrap())
            .build()
            .unwrap();
        let err = CreateProjectService::with_defaults(db)
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
        ApiserverSchemaManager.upgrade(&db).await.unwrap();
        let err = DeleteProjectService::with_defaults(db)
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
        ApiserverSchemaManager.upgrade(&db).await.unwrap();
        let upd = ProjectUpdate::builder()
            .name(Some(ProjectName::try_from("primary").unwrap()))
            .description(None)
            .build()
            .unwrap();
        UpdateProjectService::with_defaults(db)
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
    use td_database::scoped::InProject;
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
