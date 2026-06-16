//
// Copyright 2026 Tabs Data Inc.
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
//! [`layers`] for the shared validation/resolution helpers.

use td_error::td_error;
use td_objects::types::basic::{ProjectName, SimplePermissionType};

pub mod layers;
pub mod services;

#[td_error]
pub enum SimplePermissionError {
    /// `view_all_projects` / `develop_all_projects` have no Simple-surface
    /// mapping — only `admin_all_projects` is offered instance-wide.
    #[error(
        "'{0}' is not available in Simple Security Mode; grant 'view_project'/'develop_project' \
         per project, or switch to RBAC Mode View to grant an org-wide wildcard"
    )]
    RbacOnlyWildcard(SimplePermissionType) = 0,
    /// A project-scoped permission was requested without a project.
    #[error("Permission '{0}' is project-scoped and requires a project")]
    ProjectRequired(SimplePermissionType) = 1,
    /// A system-scoped permission was requested with a project.
    #[error("Permission '{0}' is system-scoped and must not carry a project")]
    ProjectForbidden(SimplePermissionType) = 2,
    /// The named project exists but its per-project role
    /// (`<project>_admin`/`_dev`/`_view`) could not be found — should not
    /// happen for a live project (the roles are minted on create).
    #[error("Per-project role '{0}' not found for project; the project may be malformed")]
    ProjectRoleNotFound(String) = 3,
    /// The project named in the request does not exist.
    #[error("Project '{0}' not found")]
    ProjectNotFound(ProjectName) = 4,
}
