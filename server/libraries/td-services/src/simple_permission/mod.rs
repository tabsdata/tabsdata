//
// Copyright 2026 Tabsdata Inc.
//

//! Simple Security Mode permission facade.
//!
//! Exposes flat per-user permission operations (`grant` / `revoke` / `list`)
//! that translate to the underlying RBAC role-membership / `role_permission`
//! model. The RBAC model is unchanged: there is no `simple_permission` table.
//! Every operation here is "add/remove user to/from a role" or "join
//! `user_role` with `role_permission` and present the result as a flat list."
//!
//! See [`services`] for the per-endpoint translation + authz, and
//! the shared validation/resolution helpers below.

use td_error::td_error;

pub mod services;

#[td_error]
pub enum SimplePermissionError {
    /// `view_all_projects` / `develop_all_projects` have no Simple-surface
    /// mapping — org-wide authority is offered only via `manage_system`
    /// (full system admin).
    #[error(
        "'{0}' is not available in Simple Security Mode; grant 'view_project'/'develop_project' \
         per project, or switch to RBAC Mode View to grant an org-wide wildcard"
    )]
    RbacOnlyWildcard(SimplePermissionType) = 0,
    #[error("Permission '{0}' is project-scoped and requires a project")]
    ProjectRequired(SimplePermissionType) = 1,
    #[error("Permission '{0}' is system-scoped and must not carry a project")]
    ProjectForbidden(SimplePermissionType) = 2,
    /// The named project exists but its per-project role
    /// (`<project>_admin`/`_dev`/`_view`) could not be found — should not
    /// happen for a live project (the roles are minted on create).
    #[error("Per-project role '{0}' not found for project; the project may be malformed")]
    ProjectRoleNotFound(String) = 3,
    #[error("Project '{0}' not found")]
    ProjectNotFound(ProjectName) = 4,
}

// Shared validation + RBAC-translation helpers for the Simple-permission
// facade. These turn a flat `(user, permission, project?)` into the concrete
// `(user_id, role_id, project_id?)` of the RBAC role-membership the grant /
// revoke maps to.

use crate::permission::{PermissionTarget, PermissionTargetScope};
use std::collections::{HashMap, HashSet};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::crudl::{ListParams, ListResponse};
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::simple_permission::{
    SimplePermission, SimplePermissionGrant, SimpleProjectAccess,
};
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{
    PermissionEntityType, PermissionType, ProjectId, ProjectIdName, ProjectName, RoleId, RoleName,
    SimplePermissionType, UserId, UserName, UserSystem,
};
use td_queries::LiveEntry;
use td_security::{PROJECT_CREATOR_ROLE, SYS_ADMIN_ROLE};

/// The concrete RBAC target a Simple grant/revoke resolves to: the user, the
/// role whose membership *is* the grant, and (for project permissions) the
/// project the operation is authorized against.
#[derive(Debug, Clone)]
pub struct ResolvedGrant {
    pub user_id: UserId,
    pub role_id: RoleId,
    /// `Some` for the three project permissions (authz scope = this project),
    /// `None` for the three system permissions (authz scope = system).
    pub project_id: Option<ProjectId>,
}

/// The grant/revoke authorization scope: a project permission authorizes
/// against its project, a system permission against the system — the same
/// rule the raw permission API uses (see
/// [`authorize_permission_target`](crate::permission::authorize_permission_target)).
impl PermissionTarget for ResolvedGrant {
    fn target_scope(&self) -> Result<PermissionTargetScope, TdError> {
        Ok(match &self.project_id {
            Some(project_id) => PermissionTargetScope::Project(*project_id),
            None => PermissionTargetScope::Global,
        })
    }
}

/// Reject the org-wide read/develop wildcards (no Simple-surface mapping) and
/// enforce the project-presence rule: required for project permissions,
/// forbidden for system permissions.
pub fn assert_simple_permission_valid(grant: &SimplePermissionGrant) -> Result<(), TdError> {
    let permission = &grant.permission;
    if permission.is_rbac_only_wildcard() {
        Err(SimplePermissionError::RbacOnlyWildcard(permission.clone()))?;
    }
    match (permission.is_project_scoped(), grant.project.is_some()) {
        (true, false) => Err(SimplePermissionError::ProjectRequired(permission.clone()))?,
        (false, true) => Err(SimplePermissionError::ProjectForbidden(permission.clone()))?,
        _ => Ok(()),
    }
}

/// Resolve `(user, permission, project?)` to its RBAC target role.
///
/// - project permission: the `<project>_admin|_dev|_view` per-project role;
/// - `create_projects`: the `project_creator` role;
/// - `manage_system`: the `sys_admin` role, which holds the whole system bundle
///   (user/role administration plus authority over all projects).
pub async fn resolve_grant(
    conn: &mut DomainConnection<Apiserver>,
    grant: &SimplePermissionGrant,
) -> Result<ResolvedGrant, TdError> {
    // Resolve the user by name (the grant surface carries a UserName).
    // System users (the built-in `system` user and other service users) are not
    // grantable: filter them out so the fetch fails for them.
    let user = conn
        .live()
        .fetch_one::<UserDB>(and!(&grant.user, UserSystem::FALSE))
        .await?;

    match grant.permission {
        SimplePermissionType::ViewProject
        | SimplePermissionType::DevelopProject
        | SimplePermissionType::ManageProject => {
            let suffix = grant
                .permission
                .project_role_suffix()
                .expect("project-scoped permission has a role suffix");
            let project_id_name = grant
                .project
                .clone()
                .expect("project-scoped permission carries a project (validated)");
            let project = conn
                .live()
                .fetch_one::<ActiveProjectDB>(&project_id_name)
                .await?;
            let role_name = per_project_role_name(&project.name, suffix)?;
            let role = conn
                .live()
                .fetch_optional::<RoleDB>(&role_name)
                .await?
                .ok_or_else(|| SimplePermissionError::ProjectRoleNotFound(role_name.to_string()))?;
            Ok(ResolvedGrant {
                user_id: user.id,
                role_id: role.id,
                project_id: Some(project.id),
            })
        }
        SimplePermissionType::CreateProjects => {
            let role = conn
                .live()
                .fetch_one::<RoleDB>(&RoleName::try_from(PROJECT_CREATOR_ROLE.to_string())?)
                .await?;
            Ok(ResolvedGrant {
                user_id: user.id,
                role_id: role.id,
                project_id: None,
            })
        }
        SimplePermissionType::ManageSystem => {
            let role = conn
                .live()
                .fetch_one::<RoleDB>(&RoleName::try_from(SYS_ADMIN_ROLE.to_string())?)
                .await?;
            Ok(ResolvedGrant {
                user_id: user.id,
                role_id: role.id,
                project_id: None,
            })
        }
        // wildcards are rejected by `assert_simple_permission_valid` first.
        SimplePermissionType::ViewAllProjects | SimplePermissionType::DevelopAllProjects => Err(
            SimplePermissionError::RbacOnlyWildcard(grant.permission.clone()),
        )?,
    }
}

/// Derive a `<project>_<suffix>` per-project role name, mirroring the minting
/// side (`project::seed_project_roles`).
pub fn per_project_role_name(project: &ProjectName, suffix: &str) -> Result<RoleName, TdError> {
    RoleName::try_from(format!("{project}_{suffix}"))
}

/// Map a project's `RoleDB` (its kind) to the Simple project permission it
/// confers, for the list-by-project source labelling. `None` for non
/// per-project roles.
pub fn project_role_permission(role: &RoleDB) -> Option<SimplePermissionType> {
    use td_objects::types::basic::RoleKind;
    match role.kind {
        RoleKind::ProjectAdmin => Some(SimplePermissionType::ManageProject),
        RoleKind::ProjectDev => Some(SimplePermissionType::DevelopProject),
        RoleKind::ProjectView => Some(SimplePermissionType::ViewProject),
        _ => None,
    }
}

// Listing: join user_role with role_permission and flatten to the Simple
// surface.

/// `project_id -> project_name`, for resolving project-scoped permission rows
/// to a human name without an extra query per row.
async fn project_names(
    conn: &mut DomainConnection<Apiserver>,
) -> Result<HashMap<ProjectId, ProjectName>, TdError> {
    let projects = conn.live().fetch_all::<ActiveProjectDB>(()).await?;
    Ok(projects.into_iter().map(|p| (p.id, p.name)).collect())
}

/// Build a small (no pagination) [`ListResponse`] from a flat data vector.
fn list_response<T: Clone>(data: Vec<T>) -> Result<ListResponse<T>, TdError> {
    Ok(ListResponse {
        list_params: ListParams::default(),
        len: data.len(),
        data,
        previous: None,
        previous_pagination_id: None,
        next: None,
        next_pagination_id: None,
    })
}

/// List a user's effective flat permissions, joining their `user_role`
/// memberships with `role_permission` and mapping each row to a Simple
/// permission (de-duplicated). `manage_roles` and the org-wide view/develop
/// wildcards are skipped (no Simple-surface name).
///
/// The target user is taken as an already-resolved `UserId` (the service
/// resolves it up front for the scope/authz check).
pub async fn flatten_user_permissions(
    conn: &mut DomainConnection<Apiserver>,
    user_id: &UserId,
) -> Result<ListResponse<SimplePermission>, TdError> {
    let user_roles = conn.live().fetch_all::<UserRoleDB>(()).await?;
    let role_ids: HashSet<RoleId> = user_roles
        .into_iter()
        .filter(|ur| ur.user_id == *user_id)
        .map(|ur| ur.role_id)
        .collect();

    let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
    let projects = project_names(conn).await?;

    let mut seen: HashSet<SimplePermission> = HashSet::new();
    let mut out = Vec::new();
    for perm in permissions
        .into_iter()
        .filter(|p| role_ids.contains(&p.role_id))
    {
        if let Some(simple) = simple_permission_of(&perm, &projects)?
            && seen.insert(simple.clone())
        {
            out.push(simple);
        }
    }
    list_response(out)
}

/// Map one `role_permission` row to its Simple-surface permission, or `None`
/// if it has no Simple name (`manage_roles`, org-wide view/develop wildcards).
fn simple_permission_of(
    perm: &PermissionDB,
    projects: &HashMap<ProjectId, ProjectName>,
) -> Result<Option<SimplePermission>, TdError> {
    let simple = match perm.entity_type.as_str() {
        "system" => match perm.permission_type.as_str() {
            // `manage_access` is the marker of the `sys_admin` bundle, surfaced as
            // the consolidated `manage_system` Simple permission.
            "manage_access" => Some((SimplePermissionType::ManageSystem, None)),
            "create_projects" => Some((SimplePermissionType::CreateProjects, None)),
            // manage_roles is internal — not user-facing on the Simple surface;
            // any other (e.g. extension) system permission likewise has no
            // Simple name.
            _ => None,
        },
        "project" => {
            if perm.entity_id.is_all_entities() {
                // Org-wide project rows have no standalone Simple name. For a
                // `sys_admin`, `(project:__all__, manage_project)` is part of the
                // `manage_system` bundle (surfaced via its `manage_access` row);
                // the view/develop wildcards are RBAC Mode View only.
                None
            } else {
                let project_id = ProjectId::from(*perm.entity_id);
                let project_name = projects.get(&project_id).cloned();
                match perm.permission_type.as_str() {
                    "view_project" => Some((SimplePermissionType::ViewProject, project_name)),
                    "develop_project" => Some((SimplePermissionType::DevelopProject, project_name)),
                    "manage_project" => Some((SimplePermissionType::ManageProject, project_name)),
                    _ => None,
                }
            }
        }
        // an unknown/extension entity type has no Simple-surface representation.
        _ => None,
    };
    Ok(simple.map(|(permission, project)| SimplePermission {
        permission,
        project,
    }))
}

/// List everyone with authority on a project, with a source label
///:
///
/// - `source = project (<rolename>)` — members of `<P>_admin|_dev|_view`,
///   mapped to `manage|develop|view_project`;
/// - `source = system (manage_system)` — members of any role holding
///   `(project:__all__, manage_project)` (i.e. system admins), mapped to
///   `manage_system`.
pub async fn list_project_access(
    conn: &mut DomainConnection<Apiserver>,
    project_id_name: &ProjectIdName,
) -> Result<ListResponse<SimpleProjectAccess>, TdError> {
    let project = conn
        .live()
        .fetch_one::<ActiveProjectDB>(project_id_name)
        .await?;

    let roles = conn.live().fetch_all::<RoleDB>(()).await?;
    let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
    let user_roles = conn.live().fetch_all::<UserRoleDB>(()).await?;
    // Exclude system users (built-in `system` and other service users) from the
    // project roster.
    let users = conn.live().fetch_all::<UserDB>(UserSystem::FALSE).await?;
    let user_names: HashMap<UserId, UserName> = users.into_iter().map(|u| (u.id, u.name)).collect();

    // Project-source roles: the three per-project roles bound to this project,
    // each mapped to its Simple permission and labelled `project (<rolename>)`.
    let mut project_source: HashMap<RoleId, (SimplePermissionType, String)> = HashMap::new();
    // System-source roles: any role holding (project:__all__, manage_project).
    let mut system_source_roles: HashSet<RoleId> = HashSet::new();

    for role in &roles {
        if role.project_id.as_ref() == Some(&project.id)
            && let Some(permission) = project_role_permission(role)
        {
            project_source.insert(role.id, (permission, format!("project ({})", role.name)));
        }
    }
    for perm in &permissions {
        if perm.entity_type == PermissionEntityType::project()
            && perm.entity_id.is_all_entities()
            && perm.permission_type == PermissionType::manage_project()
        {
            system_source_roles.insert(perm.role_id);
        }
    }

    let mut seen: HashSet<SimpleProjectAccess> = HashSet::new();
    let mut out = Vec::new();
    for user_role in &user_roles {
        let Some(user_name) = user_names.get(&user_role.user_id) else {
            continue;
        };
        // project-source membership
        if let Some((permission, source)) = project_source.get(&user_role.role_id) {
            push_access(
                &mut seen,
                &mut out,
                user_name,
                permission.clone(),
                source.clone(),
            )?;
        }
        // system-source membership (manage_system)
        if system_source_roles.contains(&user_role.role_id) {
            push_access(
                &mut seen,
                &mut out,
                user_name,
                SimplePermissionType::ManageSystem,
                "system (manage_system)".to_string(),
            )?;
        }
    }
    list_response(out)
}

fn push_access(
    seen: &mut HashSet<SimpleProjectAccess>,
    out: &mut Vec<SimpleProjectAccess>,
    user: &UserName,
    permission: SimplePermissionType,
    source: String,
) -> Result<(), TdError> {
    let access = SimpleProjectAccess {
        user: user.clone(),
        permission,
        source,
    };
    if seen.insert(access.clone()) {
        out.push(access);
    }
    Ok(())
}
