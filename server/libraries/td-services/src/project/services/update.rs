//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::ProjectError;
use crate::project::rename_project_roles::{
    assert_project_roles_rename_available, rename_project_roles,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::DbScope;
use td_database::access::{AccessMode, GatedConnection, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Dao;
use td_objects::FromDao;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::project::{
    ActiveProjectDB, ActiveProjectDBWithNames, ProjectRead, ProjectUpdate, ProjectUpdateDB,
};
use td_objects::errors::SqlError;
use td_queries::Queries;

#[service_factory]
async fn update_project(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW>,
    request: UpdateRequest<ProjectUpdate, ProjectParam>,
) -> Result<ProjectRead, TdError> {
    let (ctx, param, update) = request.into_parts();
    let id_name = param.project;
    let conn = gated_trx.get(authz.none());
    let current: ActiveProjectDB = conn.fetch_one(&id_name).await?;
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
    update_db.modified_on = ctx.time.into();
    update_db.modified_by_id = ctx.user_id.into();
    if let Some(name) = &update.name {
        update_db.name = name.clone();
    }
    if let Some(description) = &update.description {
        update_db.description = description.clone();
    }
    conn.update::<ProjectUpdateDB>(&update_db, current_id)
        .await?;

    let with_names: ActiveProjectDBWithNames = conn.fetch_one(&current_id).await?;
    let read = ProjectRead::from_dao(with_names)?;
    Ok(read)
}

/// Validate that an update request actually changes at least one field —
/// otherwise the call is a no-op and we surface it loudly.
pub fn update_project_validate(update: &ProjectUpdate) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() {
        Err(ProjectError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

/// On a rename (the update carries a new, different name), reject a collision
/// with another project up front with a clean "already exists" error instead
/// of the raw unique-constraint violation from the update write. Renaming a
/// project to its own current name is a no-op and is not a collision.
pub async fn assert_project_rename_available<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    update: &ProjectUpdate,
    current: &ActiveProjectDB,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.name
        && conn
            .fetch_optional::<ActiveProjectDB>(new_name)
            .await?
            .is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            ActiveProjectDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}
