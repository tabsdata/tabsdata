//
// Copyright 2026 Tabs Data Inc.
//

//! RBAC -> Simple representability check (TEDC "What makes switching ...
//! one-way").
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

/// The expected fixed composition of the `sys_admin` role (TEDC condition 2),
/// as `(entity_type, on `__all__`?, permission)` triples. System rows are
/// stored with `entity_id = __all__` but disambiguated by `entity_type`.
const SYS_ADMIN_COMPOSITION: &[(PermissionEntityType, bool, PermissionType)] = &[
    (PermissionEntityType::System, true, PermissionType::ManageAccess),
    (PermissionEntityType::System, true, PermissionType::CreateProjects),
    (PermissionEntityType::System, true, PermissionType::ManageRoles),
    // (project:__all__, manage_project) — the org-wide admin wildcard.
    (PermissionEntityType::Project, true, PermissionType::ManageProject),
];

/// Classify one permission row for human-readable reasons.
fn describe_row(perm: &PermissionDB) -> String {
    let target = match perm.entity_type {
        PermissionEntityType::System => "system".to_string(),
        PermissionEntityType::Project => {
            if perm.entity_id.is_all_entities() {
                "project:__all__".to_string()
            } else {
                format!("project:{}", *perm.entity_id)
            }
        }
    };
    format!("({target}, {})", perm.permission_type)
}

/// Whether this row targets a *specific* project (not the `__all__` wildcard).
fn is_specific_project_row(perm: &PermissionDB) -> bool {
    perm.entity_type == PermissionEntityType::Project && !perm.entity_id.is_all_entities()
}

/// Whether this row targets a system entity.
fn is_system_row(perm: &PermissionDB) -> bool {
    perm.entity_type == PermissionEntityType::System
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
) -> Result<Vec<String>, TdError> {
    let roles = db.live().fetch_all::<RoleDB>(()).await?;
    let permissions = db.live().fetch_all::<PermissionDB>(()).await?;
    Ok(evaluate(&roles, &permissions))
}

/// Pure evaluation of the five conditions over already-loaded `roles` and
/// `permissions`. Split out so it can be unit-tested without a DB.
pub fn evaluate(roles: &[RoleDB], permissions: &[PermissionDB]) -> Vec<String> {
    let mut reasons = Vec::new();

    // Index permissions by role for per-role composition checks.
    let mut by_role: HashMap<RoleId, Vec<&PermissionDB>> = HashMap::new();
    for perm in permissions {
        by_role.entry(perm.role_id.clone()).or_default().push(perm);
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
                check_sys_admin(role, role_perms, &mut reasons);
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
            RoleKind::PrjAdmin => {
                check_per_project(role, role_perms, PermissionType::ManageProject, &mut reasons);
            }
            RoleKind::PrjDev => {
                check_per_project(role, role_perms, PermissionType::DevelopProject, &mut reasons);
            }
            RoleKind::PrjView => {
                check_per_project(role, role_perms, PermissionType::ViewProject, &mut reasons);
            }
        }
    }

    reasons
}

/// Condition 2: `sys_admin` holds exactly its fixed composition (TEDC). Also
/// covers condition 4 for `sys_admin` (a specific-project row would not match
/// the expected wildcard composition and surfaces here).
fn check_sys_admin(role: &RoleDB, perms: &[&PermissionDB], reasons: &mut Vec<String>) {
    let matches_expected = |p: &PermissionDB| {
        SYS_ADMIN_COMPOSITION.iter().any(|(et, on_all, pt)| {
            p.entity_type == *et
                && p.permission_type == *pt
                && p.entity_id.is_all_entities() == *on_all
        })
    };

    // Any row not in the expected set is an unrepresentable modification.
    for p in perms {
        if !matches_expected(p) {
            reasons.push(format!(
                "the 'sys_admin' role has been modified from its default composition: \
                 unexpected permission {}",
                describe_row(p)
            ));
        }
    }
    // Any expected row missing is also a modification.
    for (et, on_all, pt) in SYS_ADMIN_COMPOSITION {
        let present = perms.iter().any(|p| {
            p.entity_type == *et
                && p.permission_type == *pt
                && p.entity_id.is_all_entities() == *on_all
        });
        if !present {
            let target = match (et, on_all) {
                (PermissionEntityType::System, _) => "system".to_string(),
                (PermissionEntityType::Project, true) => "project:__all__".to_string(),
                (PermissionEntityType::Project, false) => "project".to_string(),
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
        } else if is_system_row(p) && p.permission_type != PermissionType::CreateProjects {
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
    use td_objects::dxo::project::ProjectDB;
    use td_objects::dxo::role::RoleCreate;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, ProjectName, RoleId, RoleName, UserId,
    };
    use td_objects::Buildable;
    use td_queries::test_utils::SeedEntry;
    use td_queries::LiveEntry;

    use crate::project::service::create::CreateProjectService;
    use crate::role::services::create::CreateRoleService;
    use ta_services::service::TdService;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::project::ProjectCreate;
    use td_tower::ctx_service::RawOneshot;

    fn admin_ctx() -> RequestContext {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
    }

    async fn create_project(db: &TdPool, name: &str) -> Result<ProjectDB, TdError> {
        let create = ProjectCreate::builder()
            .name(&ProjectName::try_from(name)?)
            .description(&Description::try_from("d")?)
            .build()?;
        CreateProjectService::with_defaults(db.clone())
            .service()
            .raw_oneshot(admin_ctx().create((), create))
            .await?;
        Ok(db.live().fetch_one::<ProjectDB>(&ProjectName::try_from(name)?).await?)
    }

    async fn role_by_name(db: &TdPool, name: &str) -> Result<RoleDB, TdError> {
        Ok(db.live().fetch_one::<RoleDB>(&RoleName::try_from(name)?).await?)
    }

    /// A fresh bootstrap (only the built-in roles) is representable.
    #[td_test::test(db)]
    async fn test_fresh_bootstrap_is_representable(db: TdPool) -> Result<(), TdError> {
        let reasons = check_representability(&db).await?;
        assert!(reasons.is_empty(), "expected no violations, got: {reasons:?}");
        Ok(())
    }

    /// A bootstrap with per-project roles minted is still representable.
    #[td_test::test(db)]
    async fn test_with_project_is_representable(db: TdPool) -> Result<(), TdError> {
        create_project(&db, "foo").await?;
        let reasons = check_representability(&db).await?;
        assert!(reasons.is_empty(), "expected no violations, got: {reasons:?}");
        Ok(())
    }

    /// A custom role makes the configuration unrepresentable (condition 1).
    #[td_test::test(db)]
    async fn test_custom_role_violates(db: TdPool) -> Result<(), TdError> {
        let create = RoleCreate::builder()
            .try_name("data_engineers")?
            .try_description("custom")?
            .build()?;
        CreateRoleService::with_defaults(db.clone())
            .service()
            .raw_oneshot(admin_ctx().create((), create))
            .await?;

        let reasons = check_representability(&db).await?;
        assert!(
            reasons.iter().any(|r| r.contains("data_engineers") && r.contains("custom")),
            "expected a custom-role violation naming 'data_engineers', got: {reasons:?}"
        );
        Ok(())
    }

    /// A specific-project row on the `default` role violates condition 3.
    #[td_test::test(db)]
    async fn test_default_specific_project_row_violates(db: TdPool) -> Result<(), TdError> {
        let project = create_project(&db, "foo").await?;
        let default_role = role_by_name(&db, "default").await?;

        // Add (project:foo, view_project) directly onto the `default` role.
        db.seed()
            .permission(
                PermissionType::ViewProject,
                None,
                Some(EntityId::try_from(project.id.clone())?),
                &default_role,
            )
            .await?;

        let reasons = check_representability(&db).await?;
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
        let project = create_project(&db, "foo").await?;
        let project_creator = role_by_name(&db, "project_creator").await?;

        db.seed()
            .permission(
                PermissionType::ManageProject,
                None,
                Some(EntityId::try_from(project.id.clone())?),
                &project_creator,
            )
            .await?;

        let reasons = check_representability(&db).await?;
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
            .permission(PermissionType::ManageAccess, None, None, &default_role)
            .await?;

        let reasons = check_representability(&db).await?;
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("default") && r.contains("manage_access")),
            "expected a default extra-system-row violation, got: {reasons:?}"
        );
        Ok(())
    }
}
