//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::delete_project_roles::delete_project_roles;
use crate::project::guards::reject_delete_non_empty_project;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, RbacSource};
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::DeleteRequest;
use td_objects::dxo::project::{ActiveProjectDB, ProjectDeleteDB, ProjectParam};
use td_queries::Queries;

#[service_factory]
async fn delete_project(
    authz: Ctx<AuthzContext>,
    db: DbPool<AcrossProjects>,
    request: DeleteRequest<ProjectParam>,
) -> Result<(), TdError> {
    let (ctx, param) = request.into_parts();
    let mut trx = db.begin().await?;
    let id_name = param.project;
    // FOR UPDATE: the emptiness guard must not race collection create.
    let conn = trx.get(authz.none());
    let current: ActiveProjectDB = conn.for_update().fetch_one(&id_name).await?;
    // Deleting a project requires `manage_project` on that project.
    let grant = authz
        .require::<Project, ManageProject>(conn, &ctx, &current.id)
        .await?;

    // Guard before the soft-delete: never strand a non-empty project's data.
    let collections: Vec<ActiveCollectionDB> = conn.fetch_all(current.id).await?;
    reject_delete_non_empty_project(&current.id, &collections)?;

    let delete_db = current.to_delete_dao(&ctx);
    conn.update::<ProjectDeleteDB>(&delete_db, current.id)
        .await?;
    // System cascade: drop the project's three per-project roles together
    // with their permission + membership rows (bypasses the `fixed` guard;
    // see `delete_project_roles`).
    delete_project_roles(conn, &current.id).await?;
    trx.commit().await?;

    // The cascade dropped the project's roles, permissions, and memberships;
    // with the transaction committed, invalidate the cached authz grants.
    let mut conn = db.read().await?;
    authz.refresh(conn.get(grant)).await?;

    Ok(())
}
