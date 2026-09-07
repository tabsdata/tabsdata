//
// Copyright 2026 Tabsdata Inc.
//

pub mod services;

use td_error::td_error;

#[td_error]
pub enum ProjectError {
    // 0-999: InputError — surfaces as 400.
    #[error("The project update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 0,

    // 1000-1999: NotFound — surfaces as 404.
    #[error("Project `{0}` not found")]
    ProjectNotFound(String) = 1000,
    #[error("`{{project}}` path segment is missing or empty")]
    MissingProjectSegment = 1001,

    // 2000-2999: NotAllowed — surfaces as 4xx (conflict-style).
    #[error(
        "Project `{0}` still has collections and cannot be deleted; delete its \
         collections first. (Deleting a non-empty project would strand its data: \
         the rows remain but become unreachable once the project is gone.)"
    )]
    ProjectNotEmpty(String) = 2001,
}

pub(crate) mod active;
pub(crate) mod delete_project_roles;
pub(crate) mod guards;
pub(crate) mod per_project_roles;
pub(crate) mod rename_project_roles;
pub(crate) mod seed_groups;
pub(crate) mod seed_project_roles;
pub(crate) mod update;
