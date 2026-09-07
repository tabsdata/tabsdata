//
// Copyright 2025 Tabsdata Inc.
//

use td_database::access::DbScope;
use td_error::td_error;

pub mod services;

// The target-based permission authorization construct is shared with the
// Simple-permission facade (grant/revoke).

#[td_error]
pub enum PermissionError {
    #[error("The permission is a system permission that cannot be deleted")]
    PermissionIsFixed = 0,
    #[error("The given role does not have the given permission")]
    RolePermissionMismatch = 1,
    #[error("Role '{0}' is system-managed; its permissions cannot be modified")]
    RoleIsFixed(td_objects::types::basic::RoleName) = 2,
    #[error(
        "Permission '{0}' is not registered in the authorization registry (no built-in or extension entity kind declares it)"
    )]
    UnknownPermission(td_objects::types::basic::PermissionType) = 3,
}

use td_authz::AuthzContext;
use td_authz::rbac::{
    ManageAccess, ManageProject, On, Project, RbacError, RbacSource, System, SystemEntity, enforce,
    identity_grants,
};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::permission::{PermissionCreate, PermissionDB, PermissionDBWithNames};
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::types::basic::{
    EntityId, PermissionEntityType, ProjectId, ProjectName, RoleIdName,
};
use td_objects::types::basic::{Fixed, PermissionId};
use td_queries::Queries;

/// Finish a permission row from its create request: derive the entity type
/// from the authorization registry — a kind owns its permissions — so this
/// works for built-in *and* extension permissions without a hard-coded
/// mapping; resolve a named target project to its id.
pub async fn build_permission_db<S: DbScope>(
    source: &AuthzContext,
    conn: &mut DomainConnection<Apiserver, S>,
    context: &RequestContext,
    role: &RoleDB,
    permission_create: &PermissionCreate,
) -> Result<PermissionDB, TdError> {
    let permission_type = &permission_create.permission_type;
    let entity_name = &permission_create.entity_name;
    let entity_type = source
        .registry()
        .entity_type_of(permission_type.as_str())
        .ok_or_else(|| PermissionError::UnknownPermission(permission_type.clone()))?;
    let entity_type = PermissionEntityType::try_from(entity_type)?;

    let entity_id = if let Some(entity_name) = entity_name {
        let project_name = ProjectName::try_from(entity_name.to_string())?;
        let project = conn.fetch_one::<ActiveProjectDB>(&project_name).await?;

        EntityId::try_from(project.id)?
    } else {
        EntityId::all_entities()
    };

    Ok(PermissionDB {
        id: PermissionId::default(),
        role_id: role.id,
        permission_type: permission_create.permission_type.clone(),
        entity_type,
        entity_id,
        granted_by_id: context.user_id,
        granted_on: context.time,
        fixed: Fixed::FALSE,
    })
}

pub fn assert_permission_is_not_fixed(permission: &PermissionDBWithNames) -> Result<(), TdError> {
    if *permission.fixed {
        Err(PermissionError::PermissionIsFixed)?
    } else {
        Ok(())
    }
}

/// Reject adding a permission to a system-managed role from the user surface.
///
/// System-managed roles: the `sys_admin` role, the `project_creator`
/// role, and the three per-project roles (`project_admin`/`project_dev`/`project_view`)
/// have permission sets fixed by the system — only their *membership* is
/// editable. The role/permission delete
/// services already block removal via the `fixed` flag; this mirrors that on
/// the create path so a `manage_access`/`manage_project` holder cannot grow a
/// system-managed role's permission set.
///
/// Gating is on `kind`, not the row's `fixed` flag: the built-in `default` role
/// is seeded `fixed = true` yet the model mandates its composition be editable (via
/// `manage_roles`); custom roles (`kind = Custom`) are user-defined and freely
/// editable. So `Default` (the built-in default role) and `Custom` (API-created
/// roles) stay editable, while the four system-managed kinds are blocked.
pub fn assert_role_is_not_fixed(role: &RoleDB) -> Result<(), TdError> {
    use td_objects::types::basic::RoleKind;
    match role.kind {
        RoleKind::SysAdmin
        | RoleKind::ProjectCreator
        | RoleKind::ProjectAdmin
        | RoleKind::ProjectDev
        | RoleKind::ProjectView => Err(PermissionError::RoleIsFixed(role.name.clone()))?,
        RoleKind::Default | RoleKind::Custom => Ok(()),
    }
}

/// The authorization scope a permission row implies, derived from its target.
pub enum PermissionTargetScope {
    /// The permission targets one specific project — granting/revoking it
    /// requires `manage_access` on the system OR `manage_project` on that
    /// project.
    Project(ProjectId),
    /// The permission targets `project:__all__` or a system entity — it
    /// requires `manage_access` on the system (a per-project admin must not be
    /// able to grant org-wide or system-level permissions).
    Global,
}

/// A permission row that can be classified into a [`PermissionTargetScope`].
/// Implemented for both the built `PermissionDB` (create path) and the fetched
/// `PermissionDBWithNames` (delete path).
pub trait PermissionTarget {
    fn target_scope(&self) -> Result<PermissionTargetScope, TdError>;
}

impl PermissionTarget for PermissionDB {
    fn target_scope(&self) -> Result<PermissionTargetScope, TdError> {
        if self.entity_type == PermissionEntityType::project() && !self.entity_id.is_all_entities()
        {
            Ok(PermissionTargetScope::Project(ProjectId::try_from(
                self.entity_id,
            )?))
        } else {
            Ok(PermissionTargetScope::Global)
        }
    }
}

impl PermissionTarget for PermissionDBWithNames {
    fn target_scope(&self) -> Result<PermissionTargetScope, TdError> {
        if self.entity_type == PermissionEntityType::project()
            && let Some(entity_id) = self.entity_id
        {
            Ok(PermissionTargetScope::Project(ProjectId::try_from(
                entity_id,
            )?))
        } else {
            Ok(PermissionTargetScope::Global)
        }
    }
}

/// Authorize creating/deleting a permission row by its target:
///
/// - a permission on a specific project: `manage_access` OR `manage_project` on
///   that project;
/// - a permission on `__all__` / a system entity: `manage_access` on the system.
///
/// The decision is the pure [`enforce`] over the requester's effective grants,
/// so no [`Scope`](td_authz::rbac::Scope) is involved.
pub async fn authorize_permission_target<P, S: DbScope>(
    source: &AuthzContext,
    conn: &mut DomainConnection<Apiserver, S>,
    request: &RequestContext,
    permission: &P,
) -> Result<(), TdError>
where
    P: PermissionTarget,
{
    let user_grants = source.user_grants(conn, &request.user_id).await?;
    let request_grants = identity_grants(request);
    let (allowed, requirement, scope) = match permission.target_scope()? {
        PermissionTargetScope::Project(project_id) => (
            enforce::<Project, (ManageAccess, ManageProject)>(
                &user_grants,
                &request_grants,
                &On::Entity(project_id),
            ),
            "(ManageAccess, ManageProject)",
            format!("project:{project_id}"),
        ),
        PermissionTargetScope::Global => (
            enforce::<System, ManageAccess>(
                &user_grants,
                &request_grants,
                &On::Entity(SystemEntity),
            ),
            "ManageAccess",
            "system:__system__".to_string(),
        ),
    };
    if allowed {
        Ok(())
    } else {
        Err(RbacError::Forbidden(requirement.to_string(), scope))?
    }
}

pub fn assert_role_in_permission(
    role_id_name: &RoleIdName,
    permission: &PermissionDBWithNames,
) -> Result<(), TdError> {
    match role_id_name {
        RoleIdName::Id(id) => {
            if id != &permission.role_id {
                Err(PermissionError::RolePermissionMismatch)?
            }
        }
        RoleIdName::Name(name) => {
            if name != &permission.role {
                Err(PermissionError::RolePermissionMismatch)?
            }
        }
    }
    Ok(())
}
