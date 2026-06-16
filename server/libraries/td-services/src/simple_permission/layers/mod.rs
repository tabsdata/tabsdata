//
// Copyright 2026 Tabs Data Inc.
//

//! Shared validation + RBAC-translation helpers for the Simple-permission
//! facade. These turn a flat `(user, permission, project?)` into the concrete
//! `(user_id, role_id, project_id?)` of the RBAC role-membership the grant /
//! revoke maps to (TEDC "The mapping").

use crate::simple_permission::SimplePermissionError;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::{ListParams, ListResponse, ListResponseBuilder};
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::simple_permission::{
    SimplePermission, SimplePermissionGrant, SimpleProjectAccess,
};
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::{
    PermissionEntityType, PermissionType, ProjectId, ProjectIdName, ProjectName, RoleId, RoleName,
    SimplePermissionType, UserId, UserIdName, UserName,
};
use td_objects::Buildable;
use td_queries::LiveEntry;
use td_security::{PROJECT_CREATOR_ROLE, SYS_ADMIN_ROLE};
use td_tower::default_services::Condition;
use td_tower::extractors::{MutRef, Ref};

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

/// Reject the org-wide read/develop wildcards (no Simple-surface mapping) and
/// enforce the project-presence rule: required for project permissions,
/// forbidden for system permissions (TEDB).
pub async fn assert_simple_permission_valid(
    grant: Ref<SimplePermissionGrant>,
) -> Result<(), TdError> {
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

/// Branch predicate for the grant/revoke authz: project-scoped permissions
/// authorize against the project, system-scoped ones against the system.
pub async fn is_grant_project_scoped(
    grant: Ref<SimplePermissionGrant>,
) -> Result<Condition, TdError> {
    Ok(Condition(grant.permission.is_project_scoped()))
}

/// Resolve `(user, permission, project?)` to its RBAC target role.
///
/// - project permission: the `<project>_admin|_dev|_view` per-project role
///   (TEDC "Project permissions → project roles");
/// - `create_projects`: the `project_creator` role (TEDB defaults);
/// - `manage_access` / `admin_all_projects`: the `sys_admin` role, which holds
///   the whole system bundle (TEDC "System permissions").
pub async fn resolve_grant(
    mut conn: MutRef<DynamicConnection<'static>>,
    grant: Ref<SimplePermissionGrant>,
) -> Result<ResolvedGrant, TdError> {
    // Resolve the user by name (the grant surface carries a UserName).
    let user = conn.live().fetch_one::<UserDB>(&grant.user).await?;

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
            let project = conn.live().fetch_one::<ProjectDB>(&project_id_name).await?;
            let role_name = per_project_role_name(&project.name, suffix)?;
            let role = conn
                .live()
                .fetch_optional::<RoleDB>(&role_name)
                .await?
                .ok_or_else(|| {
                    SimplePermissionError::ProjectRoleNotFound(role_name.to_string())
                })?;
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
        SimplePermissionType::ManageAccess | SimplePermissionType::AdminAllProjects => {
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
        SimplePermissionType::ViewAllProjects | SimplePermissionType::DevelopAllProjects => {
            Err(SimplePermissionError::RbacOnlyWildcard(grant.permission.clone()))?
        }
    }
}

/// Extract the `UserId` from the resolved target (membership user).
pub async fn resolved_user_id(resolved: Ref<ResolvedGrant>) -> Result<UserId, TdError> {
    Ok(resolved.user_id)
}

/// Extract the `RoleId` from the resolved target (the role whose membership is
/// the grant).
pub async fn resolved_role_id(resolved: Ref<ResolvedGrant>) -> Result<RoleId, TdError> {
    Ok(resolved.role_id)
}

/// Extract the project the operation authorizes against. Only the project
/// branch of the grant/revoke conditional calls this, and `resolve_grant`
/// always sets `project_id` for project permissions, so the `None` case is
/// unreachable in practice.
pub async fn resolved_project_id(resolved: Ref<ResolvedGrant>) -> Result<ProjectId, TdError> {
    resolved
        .project_id
        .clone()
        .ok_or_else(|| TdError::from(SimplePermissionError::ProjectRoleNotFound(String::new())))
}

/// Derive a `<project>_<suffix>` per-project role name, mirroring the minting
/// side (`project::service::layer::seed_project_roles`).
pub fn per_project_role_name(project: &ProjectName, suffix: &str) -> Result<RoleName, TdError> {
    RoleName::try_from(format!("{project}_{suffix}"))
}

/// Map a project's `RoleDB` (its kind) to the Simple project permission it
/// confers, for the list-by-project source labelling. `None` for non
/// per-project roles.
pub fn project_role_permission(role: &RoleDB) -> Option<SimplePermissionType> {
    use td_objects::types::basic::RoleKind;
    match role.kind {
        RoleKind::PrjAdmin => Some(SimplePermissionType::ManageProject),
        RoleKind::PrjDev => Some(SimplePermissionType::DevelopProject),
        RoleKind::PrjView => Some(SimplePermissionType::ViewProject),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Listing: join user_role with role_permission and flatten to the Simple
// surface (TEDC "How a permission grant flows through" / "Listing access on a
// project: two sources").
// ---------------------------------------------------------------------------

/// `project_id -> project_name`, for resolving project-scoped permission rows
/// to a human name without an extra query per row.
async fn project_names(
    conn: &mut DynamicConnection<'static>,
) -> Result<HashMap<ProjectId, ProjectName>, TdError> {
    let projects = conn.live().fetch_all::<ProjectDB>(()).await?;
    Ok(projects.into_iter().map(|p| (p.id, p.name)).collect())
}

/// Build a small (no pagination) [`ListResponse`] from a flat data vector.
fn list_response<T: Clone>(data: Vec<T>) -> Result<ListResponse<T>, TdError> {
    Ok(ListResponseBuilder::default()
        .list_params(ListParams::default())
        .data(data)
        .previous_page(None, None)
        .next_page(None, None)
        .build()?)
}

/// List a user's effective flat permissions, joining their `user_role`
/// memberships with `role_permission` and mapping each row to a Simple
/// permission (de-duplicated). `role_mgnt` and the org-wide view/develop
/// wildcards are skipped (no Simple-surface name).
pub async fn flatten_user_permissions(
    mut conn: MutRef<DynamicConnection<'static>>,
    user_id_name: Ref<UserIdName>,
) -> Result<ListResponse<SimplePermission>, TdError> {
    let user = conn.live().fetch_one::<UserDB>(user_id_name.deref()).await?;

    let user_roles = conn.live().fetch_all::<UserRoleDB>(()).await?;
    let role_ids: HashSet<RoleId> = user_roles
        .into_iter()
        .filter(|ur| ur.user_id == user.id)
        .map(|ur| ur.role_id)
        .collect();

    let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
    let projects = project_names(&mut conn).await?;

    let mut seen: HashSet<SimplePermission> = HashSet::new();
    let mut out = Vec::new();
    for perm in permissions.into_iter().filter(|p| role_ids.contains(&p.role_id)) {
        if let Some(simple) = simple_permission_of(&perm, &projects)? {
            if seen.insert(simple.clone()) {
                out.push(simple);
            }
        }
    }
    list_response(out)
}

/// Map one `role_permission` row to its Simple-surface permission, or `None`
/// if it has no Simple name (`role_mgnt`, org-wide view/develop wildcards).
fn simple_permission_of(
    perm: &PermissionDB,
    projects: &HashMap<ProjectId, ProjectName>,
) -> Result<Option<SimplePermission>, TdError> {
    let simple = match perm.entity_type {
        PermissionEntityType::System => match perm.permission_type {
            PermissionType::ManageAccess => Some((SimplePermissionType::ManageAccess, None)),
            PermissionType::CreateProjects => Some((SimplePermissionType::CreateProjects, None)),
            // role_mgnt is internal — not user-facing on the Simple surface.
            PermissionType::RoleMgnt => None,
            // a project permission on the system entity is malformed; skip.
            _ => None,
        },
        PermissionEntityType::Project => {
            if perm.entity_id.is_all_entities() {
                match perm.permission_type {
                    // (project:__all__, manage_project) is `admin_all_projects`.
                    PermissionType::ManageProject => {
                        Some((SimplePermissionType::AdminAllProjects, None))
                    }
                    // org-wide view/develop wildcards: RBAC-only, no Simple name.
                    _ => None,
                }
            } else {
                let project_id = ProjectId::from(*perm.entity_id);
                let project_name = projects.get(&project_id).cloned();
                match perm.permission_type {
                    PermissionType::ViewProject => {
                        Some((SimplePermissionType::ViewProject, project_name))
                    }
                    PermissionType::DevelopProject => {
                        Some((SimplePermissionType::DevelopProject, project_name))
                    }
                    PermissionType::ManageProject => {
                        Some((SimplePermissionType::ManageProject, project_name))
                    }
                    _ => None,
                }
            }
        }
    };
    simple
        .map(|(permission, project)| {
            SimplePermission::builder()
                .permission(permission)
                .project(project)
                .build()
        })
        .transpose()
        .map_err(Into::into)
}

/// List everyone with authority on a project, with a source label
/// (TEDC "Listing access on a project: two sources"):
///
/// - `source = project (<rolename>)` — members of `<P>_admin|_dev|_view`,
///   mapped to `manage|develop|view_project`;
/// - `source = system (admin_all_projects)` — members of any role holding
///   `(project:__all__, manage_project)`, mapped to `admin_all_projects`.
pub async fn list_project_access(
    mut conn: MutRef<DynamicConnection<'static>>,
    project_id_name: Ref<ProjectIdName>,
) -> Result<ListResponse<SimpleProjectAccess>, TdError> {
    let project = conn
        .live()
        .fetch_one::<ProjectDB>(project_id_name.deref())
        .await?;

    let roles = conn.live().fetch_all::<RoleDB>(()).await?;
    let permissions = conn.live().fetch_all::<PermissionDB>(()).await?;
    let user_roles = conn.live().fetch_all::<UserRoleDB>(()).await?;
    let users = conn.live().fetch_all::<UserDB>(()).await?;
    let user_names: HashMap<UserId, UserName> =
        users.into_iter().map(|u| (u.id, u.name)).collect();

    // Project-source roles: the three per-project roles bound to this project,
    // each mapped to its Simple permission and labelled `project (<rolename>)`.
    let mut project_source: HashMap<RoleId, (SimplePermissionType, String)> = HashMap::new();
    // System-source roles: any role holding (project:__all__, manage_project).
    let mut system_source_roles: HashSet<RoleId> = HashSet::new();

    for role in &roles {
        if role.project_id.as_ref() == Some(&project.id) {
            if let Some(permission) = project_role_permission(role) {
                project_source.insert(
                    role.id,
                    (permission, format!("project ({})", role.name)),
                );
            }
        }
    }
    for perm in &permissions {
        if perm.entity_type == PermissionEntityType::Project
            && perm.entity_id.is_all_entities()
            && perm.permission_type == PermissionType::ManageProject
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
            push_access(&mut seen, &mut out, user_name, permission.clone(), source.clone())?;
        }
        // system-source membership (admin_all_projects)
        if system_source_roles.contains(&user_role.role_id) {
            push_access(
                &mut seen,
                &mut out,
                user_name,
                SimplePermissionType::AdminAllProjects,
                "system (admin_all_projects)".to_string(),
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
    let access = SimpleProjectAccess::builder()
        .user(user.clone())
        .permission(permission)
        .source(source)
        .build()?;
    if seen.insert(access.clone()) {
        out.push(access);
    }
    Ok(())
}
