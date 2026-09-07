//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::scope_to_project::scope_to_created_project;
use crate::project::seed_groups::seed_default_groups;
use crate::project::seed_project_roles::seed_project_roles;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{CreateProjects, RbacSource, System, SystemEntity};
use td_database::access::{DbPool, Global};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::CreateRequest;
use td_objects::dxo::project::{
    ActiveProjectDB, ActiveProjectDBWithNames, ProjectCreate, ProjectRead,
};
use td_queries::LiveEntry;

#[service_factory]
async fn create_project(
    authz: Ctx<AuthzContext>,
    db: DbPool<Global>,
    request: CreateRequest<ProjectCreate>,
) -> Result<ProjectRead, TdError> {
    let (ctx, _, create) = request.into_parts();
    let mut trx = db.begin().await?;
    let grant = authz
        .require::<System, CreateProjects>(&mut trx, &ctx, &SystemEntity)
        .await?;
    let conn = trx.get(grant);

    let project = create.to_dao(&ctx);
    // A duplicate project name reports a clean "already exists" error
    // instead of the raw unique-constraint violation from the insert.
    conn.live()
        .assert_not_exists::<ActiveProjectDB>(&project.name)
        .await?;
    conn.live().insert(&project).await?;

    // Scope the connection to the new project so the group seeds default
    // `project_id` from the GUC instead of stamping it by hand.
    scope_to_created_project(conn, &project.id).await?;
    // A collection resolves its group within its own project, so seed the
    // system groups (default/source/destination) into the new project.
    seed_default_groups(conn, &ctx).await?;
    // Mint the three per-project roles (`<name>_admin/_dev/_view`) and add
    // the creating user to `<name>_admin`.
    seed_project_roles(conn, &ctx, &project.id, &project).await?;

    let with_names: ActiveProjectDBWithNames = conn.live().fetch_one(&project.id).await?;
    let read = ProjectRead::from_dao(with_names)?;
    trx.commit().await?;

    // `seed_project_roles` inserted the new per-project roles, their
    // permissions, and the creator's `<name>_admin` membership; with the
    // transaction committed, invalidate the cached authz grants so the
    // creator's `manage_project` is visible on the next request.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(read)
}
