//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::PermissionError;
use std::ops::Deref;
use td_authz::AuthzContext;
use td_authz::rbac::{
    ManageAccess, ManageProject, On, Project, RbacError, RbacSource, System, SystemEntity, enforce,
    identity_grants,
};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::permission::{
    PermissionCreate, PermissionDB, PermissionDBBuilder, PermissionDBWithNames,
};
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{EntityId, PermissionEntityType, ProjectId, ProjectName, RoleIdName};
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};

pub trait PermissionBuildService {
    fn build_permission_db(
        conn: MutRef<DynamicConnection<'static>>,
        input: Ref<PermissionDBBuilder>,
        permission_create: Ref<PermissionCreate>,
    ) -> impl Future<Output = Result<PermissionDB, TdError>>;
}

impl PermissionBuildService for With<PermissionDBBuilder> {
    async fn build_permission_db(
        mut conn: MutRef<DynamicConnection<'static>>,
        input: Ref<PermissionDBBuilder>,
        permission_create: Ref<PermissionCreate>,
    ) -> Result<PermissionDB, TdError> {
        let permission_type = &permission_create.permission_type;
        let entity_name = &permission_create.entity_name;
        let entity_type = permission_type.on_entity_type();

        let entity_id = if let Some(entity_name) = entity_name {
            let project_name = ProjectName::try_from(entity_name.to_string())?;
            let project = conn.live().fetch_one::<ProjectDB>(&project_name).await?;

            EntityId::try_from(project.id)?
        } else {
            EntityId::all_entities()
        };

        let mut input = input.deref().clone();
        let permission_db = input
            .entity_type(entity_type)
            .entity_id(entity_id)
            .build()?;
        Ok(permission_db)
    }
}

pub async fn assert_permission_is_not_fixed(
    permission: Ref<PermissionDBWithNames>,
) -> Result<(), TdError> {
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
pub async fn assert_role_is_not_fixed(role: Ref<RoleDB>) -> Result<(), TdError> {
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
        if self.permission_type.on_entity_type() == PermissionEntityType::Project
            && !self.entity_id.is_all_entities()
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
        if self.permission_type.on_entity_type() == PermissionEntityType::Project
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

/// Authorize creating/deleting a permission row by its target — replacing the
/// per-service `is_permission_on_a_single_project` conditional (and the coarse
/// `OnAnyProject` pre-gate) with one check:
///
/// - a permission on a specific project: `manage_access` OR `manage_project` on
///   that project;
/// - a permission on `__all__` / a system entity: `manage_access` on the system.
///
/// The decision is the pure [`enforce`] over the requester's effective grants,
/// so no [`Scope`](td_authz::rbac::Scope) needs to be set in context.
pub async fn authorize_permission_target<P>(
    source: Ref<AuthzContext>,
    mut conn: MutRef<DynamicConnection<'static>>,
    request: Ref<RequestContext>,
    permission: Ref<P>,
) -> Result<(), TdError>
where
    P: PermissionTarget + Send + Sync + 'static,
{
    let user_grants = source.user_grants(&mut conn, &request.user_id).await?;
    let request_grants = identity_grants(&request);
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

pub async fn assert_role_in_permission(
    role_id_name: Ref<RoleIdName>,
    permission: Ref<PermissionDBWithNames>,
) -> Result<(), TdError> {
    match role_id_name.deref() {
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
