//
// Copyright 2026 Tabs Data Inc.
//

//! RBAC -> Simple representability check.
//!
//! Simple Security Mode can faithfully present a configuration if and only if
//! all five conditions below hold. The check reads `roles` + `role_permission`
//! and returns a list of human-readable violation reasons; an empty list means
//! the configuration is representable and the switch back to Simple is safe.
//!
//! 1. Only built-in role kinds exist — no role with `kind = custom`.
//! 2. The `sys_admin` role has its default (fixed) composition.
//! 3. The `default` role's composition is Simple-describable: only system rows
//!    from `{(system, create_projects)}` and/or rows on `project:__all__`; NO
//!    specific-project rows.
//! 4. No `role_permission` row references a *specific* project on a SYSTEM role
//!    (`sys_admin` / `default` / `project_creator`); `project:__all__` rows are
//!    fine.
//! 5. Each per-project role (`<P>_admin`/`_dev`/`_view`) holds exactly its one
//!    expected project permission and no system rows.
//!
//! Membership is unconstrained: representability depends only on the role
//! *definitions*, not on who is in which role.

use std::collections::HashMap;
use td_error::TdError;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::role::RoleDB;
use td_objects::types::basic::{PermissionEntityType, PermissionType, RoleId, RoleKind};
use td_queries::{LiveEntry, QueryExecutor};

/// The expected fixed composition of the `sys_admin` role (condition 2),
/// as `(entity_type, on `__all__`?, permission)` triples. System rows are
/// stored with `entity_id = __all__` but disambiguated by `entity_type`.
// (entity_type, on_all, permission) of the fixed `sys_admin` composition.
// String literals because the permission types are now open strings (no longer
// const-constructible enum variants); rows are compared by their stored value.
const SYS_ADMIN_COMPOSITION: &[(&str, bool, &str)] = &[
    ("system", true, "manage_access"),
    ("system", true, "create_projects"),
    ("system", true, "manage_roles"),
    // (project:__all__, manage_project) — the org-wide admin wildcard.
    ("project", true, "manage_project"),
];

/// Classify one permission row for human-readable reasons.
fn describe_row(perm: &PermissionDB) -> String {
    let target = match perm.entity_type.as_str() {
        "system" => "system".to_string(),
        "project" => {
            if perm.entity_id.is_all_entities() {
                "project:__all__".to_string()
            } else {
                format!("project:{}", *perm.entity_id)
            }
        }
        other => other.to_string(),
    };
    format!("({target}, {})", perm.permission_type)
}

/// Whether this row targets a *specific* project (not the `__all__` wildcard).
fn is_specific_project_row(perm: &PermissionDB) -> bool {
    perm.entity_type == PermissionEntityType::project() && !perm.entity_id.is_all_entities()
}

/// Whether this row targets a system entity.
fn is_system_row(perm: &PermissionDB) -> bool {
    perm.entity_type == PermissionEntityType::system()
}

/// Whether this row is one `(entity_type, on __all__?, permission)` composition entry.
fn row_matches(perm: &PermissionDB, (et, on_all, pt): &(&str, bool, &str)) -> bool {
    perm.entity_type.as_str() == *et
        && perm.permission_type.as_str() == *pt
        && perm.entity_id.is_all_entities() == *on_all
}

/// Run the RBAC -> Simple representability check against the live `roles` and
/// `role_permission` tables. Returns the list of reasons the configuration
/// cannot be represented on the Simple surface (empty = representable).
///
/// `db` is any query executor (a pool reference or a connection), so this can be
/// called both at apiserver startup and from a service/test. `Copy` is required
/// because each `.live()` consumes the executor and the check issues two reads;
/// `&TdPool` (and the connection references) are `Copy`.
pub async fn check_representability(
    db: impl QueryExecutor + Copy,
    sys_admin_extras: &[(&str, &str)],
) -> Result<Vec<String>, TdError> {
    let roles = db.live().fetch_all::<RoleDB>(()).await?;
    let permissions = db.live().fetch_all::<PermissionDB>(()).await?;
    Ok(evaluate(&roles, &permissions, sys_admin_extras))
}

/// Pure evaluation of the five conditions over already-loaded `roles` and
/// `permissions`. Split out so it can be unit-tested without a DB.
pub fn evaluate(
    roles: &[RoleDB],
    permissions: &[PermissionDB],
    sys_admin_extras: &[(&str, &str)],
) -> Vec<String> {
    let mut reasons = Vec::new();

    // Index permissions by role for per-role composition checks.
    let mut by_role: HashMap<RoleId, Vec<&PermissionDB>> = HashMap::new();
    for perm in permissions {
        by_role.entry(perm.role_id).or_default().push(perm);
    }
    let empty: Vec<&PermissionDB> = Vec::new();

    for role in roles {
        let role_perms = by_role.get(&role.id).unwrap_or(&empty);
        match role.kind {
            // Condition 1: no custom roles.
            RoleKind::Custom => {
                reasons.push(format!(
                    "role '{}' is a custom role (kind=custom); Simple Security Mode has no \
                     concept of custom roles",
                    role.name
                ));
            }
            // Condition 2: sys_admin must hold its default composition.
            RoleKind::SysAdmin => {
                check_sys_admin(role, role_perms, &mut reasons, sys_admin_extras);
            }
            // Condition 3: default role's composition must be Simple-describable.
            RoleKind::Default => {
                check_default(role, role_perms, &mut reasons);
            }
            // Condition 4 (project_creator half) + system-role specific-project rows.
            RoleKind::ProjectCreator => {
                check_project_creator(role, role_perms, &mut reasons);
            }
            // Condition 5: per-project roles must hold exactly their one
            // expected project permission and no system rows.
            RoleKind::ProjectAdmin => {
                check_per_project(
                    role,
                    role_perms,
                    PermissionType::manage_project(),
                    &mut reasons,
                );
            }
            RoleKind::ProjectDev => {
                check_per_project(
                    role,
                    role_perms,
                    PermissionType::develop_project(),
                    &mut reasons,
                );
            }
            RoleKind::ProjectView => {
                check_per_project(
                    role,
                    role_perms,
                    PermissionType::view_project(),
                    &mut reasons,
                );
            }
        }
    }

    reasons
}

/// Condition 2: `sys_admin` holds exactly its fixed composition. Also
/// covers condition 4 for `sys_admin` (a specific-project row would not match
/// the expected wildcard composition and surfaces here).
fn check_sys_admin(
    role: &RoleDB,
    perms: &[&PermissionDB],
    reasons: &mut Vec<String>,
    sys_admin_extras: &[(&str, &str)],
) {
    // Expected composition = the OS built-ins plus any extension permissions
    // declared to ride on sys_admin (stored on `__all__`, like the system rows).
    let expected: Vec<(&str, bool, &str)> = SYS_ADMIN_COMPOSITION
        .iter()
        .copied()
        .chain(sys_admin_extras.iter().map(|(et, pt)| (*et, true, *pt)))
        .collect();

    // Any row not in the expected set is an unrepresentable modification.
    for p in perms {
        if !expected.iter().any(|e| row_matches(p, e)) {
            reasons.push(format!(
                "the 'sys_admin' role has been modified from its default composition: \
                 unexpected permission {}",
                describe_row(p)
            ));
        }
    }
    // Any expected row missing is also a modification.
    for e in &expected {
        let (et, on_all, pt) = *e;
        if !perms.iter().any(|p| row_matches(p, e)) {
            let target = match (et, on_all) {
                ("system", _) => "system".to_string(),
                ("project", true) => "project:__all__".to_string(),
                ("project", false) => "project".to_string(),
                (other, _) => other.to_string(),
            };
            reasons.push(format!(
                "the 'sys_admin' role is missing its default permission ({target}, {pt}); \
                 role '{}' no longer matches its fixed composition",
                role.name
            ));
        }
    }
}

/// Condition 3: the `default` role may only hold system rows drawn from
/// `{(system, create_projects)}` and/or `project:__all__` rows; NO
/// specific-project rows.
fn check_default(role: &RoleDB, perms: &[&PermissionDB], reasons: &mut Vec<String>) {
    for p in perms {
        if is_specific_project_row(p) {
            reasons.push(format!(
                "the 'default' role holds a specific-project permission {} on role '{}'; the \
                 Simple surface only allows system or project:__all__ rows on 'default'",
                describe_row(p),
                role.name
            ));
        } else if is_system_row(p) && p.permission_type != PermissionType::create_projects() {
            reasons.push(format!(
                "the 'default' role holds the system permission {} on role '{}', which the \
                 Simple surface cannot describe (only 'create_projects' is allowed on 'default')",
                describe_row(p),
                role.name
            ));
        }
        // project:__all__ rows are allowed (the "loosened defaults" idiom).
    }
}

/// Condition 4 (`project_creator` half): a SYSTEM role must not directly hold a
/// specific-project row. `project_creator`'s own composition is fixed by the
/// system, so only the specific-project guard is meaningful here.
fn check_project_creator(role: &RoleDB, perms: &[&PermissionDB], reasons: &mut Vec<String>) {
    for p in perms {
        if is_specific_project_row(p) {
            reasons.push(format!(
                "the system role 'project_creator' holds a specific-project permission {} on \
                 role '{}'; project access must flow through per-project roles",
                describe_row(p),
                role.name
            ));
        }
    }
}

/// Condition 5: a per-project role holds exactly one project permission (its
/// expected one, on a specific project) and no system rows.
fn check_per_project(
    role: &RoleDB,
    perms: &[&PermissionDB],
    expected: PermissionType,
    reasons: &mut Vec<String>,
) {
    let mut expected_count = 0usize;
    for p in perms {
        if is_system_row(p) {
            reasons.push(format!(
                "the per-project role '{}' holds a system permission {}; per-project roles must \
                 hold only their single project permission",
                role.name,
                describe_row(p)
            ));
        } else if p.entity_id.is_all_entities() {
            reasons.push(format!(
                "the per-project role '{}' holds a wildcard permission {}; per-project roles must \
                 be scoped to their own project",
                role.name,
                describe_row(p)
            ));
        } else if p.permission_type == expected {
            expected_count += 1;
        } else {
            reasons.push(format!(
                "the per-project role '{}' holds an unexpected project permission {} (expected \
                 only '{expected}')",
                role.name,
                describe_row(p)
            ));
        }
    }
    if expected_count != 1 {
        reasons.push(format!(
            "the per-project role '{}' should hold exactly one '{expected}' permission on its \
             project, but holds {expected_count}",
            role.name
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::permission::PermissionDBBuilder;
    use td_objects::dxo::project::ProjectDB;
    use td_objects::dxo::role::{RoleCreate, RoleDBBuilder};
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, Fixed, ProjectName, RoleName, UserId,
    };
    use td_objects::{Buildable, TryUpdater};
    use td_queries::test_utils::SeedEntry;

    fn admin_context() -> RequestContext {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
    }

    async fn role_by_name(db: &TdPool, name: &str) -> Result<RoleDB, TdError> {
        Ok(db
            .live()
            .fetch_one::<RoleDB>(&RoleName::try_from(name)?)
            .await?)
    }

    /// Mint the three per-project roles (`<name>_admin`/`_dev`/`_view`) for an
    /// existing project, mirroring the service-layer minting: each is a
    /// system-managed (`fixed = true`) role of its per-project kind holding
    /// exactly its one fixed permission on the project. `db.seed().project()`
    /// only creates the project + system groups, so the per-project roles —
    /// the subject of condition 5 — must be seeded explicitly here.
    async fn seed_per_project_roles(db: &TdPool, project: &ProjectDB) -> Result<(), TdError> {
        let context = admin_context();
        let entity_id = EntityId::try_from(project.id)?;
        let specs = [
            (
                RoleKind::ProjectAdmin,
                "admin",
                PermissionType::manage_project(),
            ),
            (
                RoleKind::ProjectDev,
                "dev",
                PermissionType::develop_project(),
            ),
            (
                RoleKind::ProjectView,
                "view",
                PermissionType::view_project(),
            ),
        ];
        for (kind, suffix, permission_type) in specs {
            let role_name = RoleName::try_from(format!("{}_{suffix}", project.name))?;
            let role_create = RoleCreate::builder()
                .name(role_name)
                .description(Description::try_from("per-project role")?)
                .build()?;
            let role = RoleDBBuilder::try_from(&role_create)?
                .try_update_from(&context)?
                .kind(kind)
                .project_id(Some(project.id))
                .fixed(Fixed::from(true))
                .build()?;
            let permission = PermissionDBBuilder::default()
                .try_update_from(&context)?
                .try_update_from(&role)?
                .permission_type(permission_type)
                .entity_type(PermissionEntityType::project())
                .entity_id(entity_id)
                .fixed(Fixed::from(true))
                .build()?;
            db.live().insert::<RoleDB>(&role).await?;
            db.live().insert::<PermissionDB>(&permission).await?;
        }
        Ok(())
    }

    /// A fresh bootstrap (only the built-in roles) is representable.
    #[td_test::test(db)]
    async fn test_fresh_bootstrap_is_representable(db: TdPool) -> Result<(), TdError> {
        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons.is_empty(),
            "expected no violations, got: {reasons:?}"
        );
        Ok(())
    }

    /// A bootstrap with per-project roles minted is still representable.
    #[td_test::test(db)]
    async fn test_with_project_is_representable(db: TdPool) -> Result<(), TdError> {
        let project = db.seed().project(ProjectName::try_from("foo")?).await?;
        seed_per_project_roles(&db, &project).await?;
        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons.is_empty(),
            "expected no violations, got: {reasons:?}"
        );
        Ok(())
    }

    /// A custom role makes the configuration unrepresentable (condition 1).
    #[td_test::test(db)]
    async fn test_custom_role_violates(db: TdPool) -> Result<(), TdError> {
        let role_create = RoleCreate::builder()
            .name(RoleName::try_from("data_engineers")?)
            .description(Description::try_from("custom")?)
            .build()?;
        let role = RoleDBBuilder::try_from(&role_create)?
            .try_update_from(&admin_context())?
            .kind(RoleKind::Custom)
            .build()?;
        db.live().insert::<RoleDB>(&role).await?;

        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("data_engineers") && r.contains("custom")),
            "expected a custom-role violation naming 'data_engineers', got: {reasons:?}"
        );
        Ok(())
    }

    /// A specific-project row on the `default` role violates condition 3.
    #[td_test::test(db)]
    async fn test_default_specific_project_row_violates(db: TdPool) -> Result<(), TdError> {
        let project = db.seed().project(ProjectName::try_from("foo")?).await?;
        let default_role = role_by_name(&db, "default").await?;

        // Add (project:foo, view_project) directly onto the `default` role.
        db.seed()
            .permission(
                PermissionType::view_project(),
                None,
                Some(EntityId::try_from(project.id)?),
                &default_role,
            )
            .await?;

        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("default") && r.contains("specific-project")),
            "expected a default specific-project violation, got: {reasons:?}"
        );
        Ok(())
    }

    /// A specific-project row on a SYSTEM role (project_creator) violates
    /// condition 4.
    #[td_test::test(db)]
    async fn test_system_role_specific_project_row_violates(db: TdPool) -> Result<(), TdError> {
        let project = db.seed().project(ProjectName::try_from("foo")?).await?;
        let project_creator = role_by_name(&db, "project_creator").await?;

        db.seed()
            .permission(
                PermissionType::manage_project(),
                None,
                Some(EntityId::try_from(project.id)?),
                &project_creator,
            )
            .await?;

        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("project_creator") && r.contains("specific-project")),
            "expected a project_creator specific-project violation, got: {reasons:?}"
        );
        Ok(())
    }

    /// A system row on the `default` role other than `create_projects` violates
    /// condition 3.
    #[td_test::test(db)]
    async fn test_default_extra_system_row_violates(db: TdPool) -> Result<(), TdError> {
        let default_role = role_by_name(&db, "default").await?;

        // manage_access is a system permission; entity_id defaults to __all__.
        db.seed()
            .permission(PermissionType::manage_access(), None, None, &default_role)
            .await?;

        let reasons = check_representability(&db, &[]).await?;
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("default") && r.contains("manage_access")),
            "expected a default extra-system-row violation, got: {reasons:?}"
        );
        Ok(())
    }
}
