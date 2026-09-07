//
// Copyright 2026 Tabsdata Inc.
//

//! Rewrite a project's per-project role names/descriptions on rename, mirroring
//! `delete_project_roles`: `roles.name` is globally unique and derived from the
//! project name, so a rename must keep them in step or the old names strand.

use crate::project::per_project_roles::{PER_PROJECT_ROLES, per_project_role_name};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::DaoCore;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::project::{ActiveProjectDB, ProjectUpdate};
use td_objects::dxo::role::{RoleDB, RoleDBUpdate, RoleUpdate};
use td_objects::errors::SqlError;
use td_objects::types::basic::{Description, ProjectId, ProjectName, RoleName};
use td_queries::LiveEntry;

fn new_role_names(new_name: &ProjectName) -> Result<Vec<RoleName>, TdError> {
    PER_PROJECT_ROLES
        .iter()
        .map(|(_, suffix, _, _)| per_project_role_name(new_name, suffix))
        .collect()
}

/// Surface a clean "already exists" error if a target `<new>_*` name is taken,
/// instead of the raw unique-constraint violation from the rewrite.
pub async fn assert_project_roles_rename_available(
    conn: &mut DomainConnection<Apiserver>,
    update: &ProjectUpdate,
    current: &ActiveProjectDB,
) -> Result<(), TdError> {
    let Some(new_name) = &update.name else {
        return Ok(());
    };
    if new_name == &current.name {
        return Ok(());
    }
    for role_name in new_role_names(new_name)? {
        if conn
            .live()
            .fetch_optional::<RoleDB>(&role_name)
            .await?
            .is_some()
        {
            Err(SqlError::EntityAlreadyExists(
                RoleDB::display_name().to_string(),
                format!(" for 'name' = '{role_name}'"),
            ))?;
        }
    }
    Ok(())
}

pub async fn rename_project_roles(
    conn: &mut DomainConnection<Apiserver>,
    context: &RequestContext,
    project_id: &ProjectId,
    update: &ProjectUpdate,
    current: &ActiveProjectDB,
) -> Result<(), TdError> {
    let Some(new_name) = &update.name else {
        return Ok(());
    };
    if new_name == &current.name {
        return Ok(());
    }

    let by_project: Option<ProjectId> = Some(*project_id);
    let roles: Vec<RoleDB> = conn.live().fetch_all::<RoleDB>(by_project).await?;

    for role in &roles {
        let Some((_, suffix, _, description_tmpl)) = PER_PROJECT_ROLES
            .iter()
            .find(|(kind, ..)| *kind == role.kind)
        else {
            continue;
        };

        let new_role_name = per_project_role_name(new_name, suffix)?;
        let new_description =
            Description::try_from(description_tmpl.replace("{name}", &new_name.to_string()))?;

        let role_update = RoleUpdate {
            name: Some(new_role_name),
            description: Some(new_description),
        };
        let role_update_db = RoleDBUpdate {
            name: role_update.name,
            description: role_update.description,
            modified_on: context.time.into(),
            modified_by_id: context.user_id.into(),
        };
        conn.live()
            .update::<RoleDBUpdate, RoleDB>(&role_update_db, role.id)
            .await?;
    }

    Ok(())
}
