//
// Copyright 2026 Tabs Data Inc.
//

//! The three per-project roles (`<name>_admin/_dev/_view`), shared by the
//! create (`seed_project_roles`) and rename (`rename_project_roles`) paths so
//! their derived names/descriptions stay in lockstep.

use td_error::TdError;
use td_objects::types::basic::{ProjectName, RoleKind, RoleName};

/// `(kind, name suffix, permission, description template)`.
pub(crate) const PER_PROJECT_ROLES: [(RoleKind, &str, &str, &str); 3] = [
    (
        RoleKind::ProjectAdmin,
        "admin",
        "manage_project",
        "Project {name} administrators",
    ),
    (
        RoleKind::ProjectDev,
        "dev",
        "develop_project",
        "Project {name} developers",
    ),
    (
        RoleKind::ProjectView,
        "view",
        "view_project",
        "Project {name} viewers",
    ),
];

pub(crate) fn per_project_role_name(
    project: &ProjectName,
    suffix: &str,
) -> Result<RoleName, TdError> {
    RoleName::try_from(format!("{project}_{suffix}"))
}
