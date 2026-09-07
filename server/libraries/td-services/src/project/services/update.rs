//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::rename_project_roles::{
    assert_project_roles_rename_available, rename_project_roles,
};
use crate::project::update::{assert_project_rename_available, update_project_validate};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{GatedConnection, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::project::{
    ActiveProjectDB, ActiveProjectDBWithNames, ProjectRead, ProjectUpdate, ProjectUpdateDB,
};
use td_queries::LiveEntry;

#[service_factory]
async fn update_project(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW>,
    request: UpdateRequest<ProjectUpdate, ProjectParam>,
) -> Result<ProjectRead, TdError> {
    let (ctx, param, update) = request.into_parts();
    let id_name = param.project;
    let conn = gated_trx.get(authz.none());
    let current: ActiveProjectDB = conn.live().fetch_one(&id_name).await?;
    // Renaming a project requires `manage_project` on that project.
    let _grant = authz
        .require::<Project, ManageProject>(conn, &ctx, &current.id)
        .await?;

    update_project_validate(&update)?;
    // A rename to a name already taken by another project reports a clean
    // "already exists" error instead of the raw unique-constraint
    // violation from the update write.
    assert_project_rename_available(conn, &update, &current).await?;
    // Keep the derived per-project roles in step with the rename.
    assert_project_roles_rename_available(conn, &update, &current).await?;
    rename_project_roles(conn, &ctx, &current.id, &update, &current).await?;

    let current_id = current.id;
    let mut update_db = ProjectUpdateDB::from(current);
    update_db.modified_on = ctx.time;
    update_db.modified_by_id = ctx.user_id;
    if let Some(name) = &update.name {
        update_db.name = name.clone();
    }
    if let Some(description) = &update.description {
        update_db.description = description.clone();
    }
    conn.live()
        .update::<ProjectUpdateDB, ActiveProjectDB>(&update_db, current_id)
        .await?;

    let with_names: ActiveProjectDBWithNames = conn.live().fetch_one(&current_id).await?;
    let read = ProjectRead::from_dao(with_names)?;
    Ok(read)
}
