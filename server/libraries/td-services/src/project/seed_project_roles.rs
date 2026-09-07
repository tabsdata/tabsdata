//
// Copyright 2026 Tabsdata Inc.
//

//! Mint the three per-project roles (`<name>_admin`, `<name>_dev`,
//! `<name>_view`) when a project is created, and add the creating user to
//! `<name>_admin`.
//!
//! Per-project, system-managed roles: each role is a
//! system-managed role (`fixed = true`) carrying exactly one fixed permission
//! on the new project — `manage_project` / `develop_project` / `view_project`
//! respectively. Membership, by contrast, is user-managed (governed by
//! `manage_project` on the project), so the creator's `<name>_admin` membership
//! row is **not** fixed.
//!
//! Runs after `seed_default_groups`, inside the project-create transaction, so
//! the `projects` row already exists (the `roles.project_id` FK is satisfied)
//! and `ProjectId` is in context.

use crate::project::per_project_roles::{PER_PROJECT_ROLES, per_project_role_name};
use td_database::access::DbScope;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::project::ProjectInsertDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{
    Description, EntityId, Fixed, PermissionEntityType, PermissionId, PermissionType, ProjectId,
    RoleId, RoleKind,
};
use td_queries::Queries;

pub async fn seed_project_roles<S: DbScope>(
    conn: &mut DomainConnection<Apiserver, S>,
    context: &RequestContext,
    project_id: &ProjectId,
    project: &ProjectInsertDB,
) -> Result<(), TdError> {
    let project_id = *project_id;
    let project_name = &project.name;
    let entity_id = EntityId::try_from(project_id)?;

    let mut admin_role_id = None;
    for (kind, suffix, permission_type, description_tmpl) in PER_PROJECT_ROLES {
        let permission_type = PermissionType::try_from(permission_type)?;
        let is_admin = matches!(kind, RoleKind::ProjectAdmin);
        let role_name = per_project_role_name(project_name, suffix)?;
        let description =
            Description::try_from(description_tmpl.replace("{name}", &project_name.to_string()))?;

        let role = RoleDB {
            id: RoleId::default(),
            name: role_name,
            description,
            created_on: context.time.into(),
            created_by_id: context.user_id.into(),
            modified_on: context.time.into(),
            modified_by_id: context.user_id.into(),
            kind,
            project_id: Some(project_id),
            fixed: Fixed::TRUE,
        };

        let permission = PermissionDB {
            id: PermissionId::default(),
            role_id: role.id,
            permission_type,
            entity_type: PermissionEntityType::project(),
            entity_id,
            granted_by_id: context.user_id,
            granted_on: context.time,
            fixed: Fixed::TRUE,
        };

        conn.insert::<RoleDB>(&role).await?;
        conn.insert::<PermissionDB>(&permission).await?;

        if is_admin {
            admin_role_id = Some(role.id);
        }
    }

    // Add the creating user to `<name>_admin`. Per-project membership is
    // user-managed (governed by `manage_project` on the project), so this
    // membership row is NOT fixed — it can be added/removed later by any
    // `manage_project` holder.
    let admin_role_id = admin_role_id.expect("ProjectAdmin role minted above");
    let user_role = UserRoleDB::new(context.user_id, admin_role_id, context, Fixed::FALSE);
    conn.insert::<UserRoleDB>(&user_role).await?;

    Ok(())
}
