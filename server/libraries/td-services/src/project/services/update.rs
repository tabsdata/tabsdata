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
use td_bridge::BridgeProviderHandle;
use td_database::access::DbScope;
use td_database::access::{AccessMode, DbPool, Global, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Dao;
use td_objects::FromDao;
use td_objects::dxo::crudl::UpdateRequest;
use td_objects::dxo::project::ProjectParam;
use td_objects::dxo::project::{ProjectDB, ProjectDBWithNames, ProjectRead, ProjectUpdate};
use td_objects::errors::SqlError;
use td_queries::Queries;

#[service_factory]
async fn update_project(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<Global>,
    request: UpdateRequest<ProjectUpdate, ProjectParam>,
) -> Result<ProjectRead, TdError> {
    let (ctx, param, update) = request.into_parts();
    let current = {
        let mut connection = db.read().await?;
        let connection = connection.get(authz.none());
        let current: ProjectDB = connection.scd2().fetch_one(&param.project).await?;
        let _grant = authz
            .require::<Project, ManageProject>(connection, &ctx, &current.project_id)
            .await?;
        update_project_validate(&update)?;
        assert_project_rename_available(connection, &update, &current).await?;
        assert_project_roles_rename_available(connection, &update, &current).await?;
        current
    };
    let project = update.name.as_ref().unwrap_or(&current.name);
    let secrets = bridge.project_secrets();
    match &update.secrets {
        Some(desired) => {
            secrets
                .upsert(&current.project_id, project, desired)
                .await?;
        }
        None if project != &current.name => {
            secrets.rename(&current.project_id, project).await?;
        }
        None => {}
    }
    retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let conn = gated_trx.get(authz.none());
        let current: ProjectDB = conn.scd2().fetch_one(&param.project).await?;
        let _grant = authz
            .require::<Project, ManageProject>(conn, &ctx, &current.project_id)
            .await?;
        update_project_validate(&update)?;

        // A rename to a name already taken by another project reports a clean
        // "already exists" error, here or from the writes when a concurrent
        // create or rename takes it after the checks.
        assert_project_rename_available(conn, &update, &current).await?;
        assert_project_roles_rename_available(conn, &update, &current).await?;

        let current_id = current.project_id;
        let mut gated_trx = gated_trx.enter_project(&current_id).await?;
        let at = gated_trx.locked_at();
        let conn = gated_trx.get(authz.none());
        conn.scd2()
            .open(&current.updated(&update, &ctx), at)
            .await
            .map_err(SqlError::already_exists)?;
        // Keep the derived per-project roles in step with the rename. They are
        // written after the project row, as a delete closes it before dropping
        // them: a rename racing a delete serializes on the project row instead
        // of deadlocking on its roles.
        rename_project_roles(conn, &ctx, &current_id, &update, &current).await?;

        let with_names: ProjectDBWithNames = conn.scd2().fetch_one(&current_id).await?;
        let read = ProjectRead::from_dao(with_names)?;
        gated_trx.commit().await?;
        Ok(read)
    })
    .await
}

/// Validate that an update request actually changes at least one field —
/// otherwise the call is a no-op and we surface it loudly.
pub fn update_project_validate(update: &ProjectUpdate) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() && update.secrets.is_none() {
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
    current: &ProjectDB,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.name
        && conn
            .scd2()
            .fetch_optional::<ProjectDB>(new_name)
            .await?
            .is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            ProjectDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}
