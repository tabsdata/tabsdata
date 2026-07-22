//
// Copyright 2026 Tabsdata Inc.
//

//! Delete guard for the project registry: never orphan a non-empty project's
//! data.

use crate::project::ProjectError;
use td_error::TdError;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::types::basic::ProjectId;
use td_tower::extractors::Ref;

/// Reject deleting a project that still owns collections. Collections are the
/// top of the project-scoped data tree (functions, tables, runs, … all hang
/// off a collection), so "no collections" means "no project-scoped data".
/// Deleting a non-empty project would leave those rows in place but
/// unreachable — the resolver reads `projects_active`, so once the project is
/// soft-deleted no request can ever set the GUC back to it.
///
/// The delete-project service spans projects (bypass scope), so the
/// `By::<ProjectId>` fetch sees the target project's collections regardless of
/// the request's own scope.
pub async fn reject_delete_non_empty_project(
    project_id: Ref<ProjectId>,
    collections: Ref<Vec<ActiveCollectionDB>>,
) -> Result<(), TdError> {
    if !collections.is_empty() {
        Err(ProjectError::ProjectNotEmpty(project_id.to_string()))?;
    }
    Ok(())
}
