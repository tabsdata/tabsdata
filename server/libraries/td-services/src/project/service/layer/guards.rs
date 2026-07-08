//
// Copyright 2026 Tabs Data Inc.
//

//! Delete guards for the project registry.
//!
//! Two ways a project delete would leave the system in a bad state, each
//! guarded here:
//!   - removing `sandbox` on a single-project backend (sqlite/mysql), where it
//!     is the *only* project and there is no RLS to scope additional ones;
//!   - orphaning a non-empty project's data on any backend.
//!
//! On PostgreSQL `sandbox` is an ordinary project: it can be deleted once
//! empty, like any other. Nothing in the schema requires it to exist (the
//! column default reads the request GUC, with no sandbox fallback — see the
//! RLS phase in `td-schema`).

use crate::project::ProjectError;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::types::basic::ProjectId;
use td_tower::extractors::{MutRef, Ref};

/// Reject deleting `sandbox` on a single-project (non-PostgreSQL) backend,
/// where it is the only project and deleting it would leave the system with
/// none. On PostgreSQL sandbox is an ordinary, deletable project, so this is a
/// no-op there.
pub async fn reject_delete_sandbox_on_single_project_backend(
    conn: MutRef<DomainConnection<Apiserver>>,
    project_id: Ref<ProjectId>,
) -> Result<(), TdError> {
    if !conn.is_postgres() && project_id.is_sandbox() {
        Err(ProjectError::CannotDeleteSandboxOnSingleProjectBackend)?;
    }
    Ok(())
}

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
    collections: Ref<Vec<CollectionDB>>,
) -> Result<(), TdError> {
    if !collections.is_empty() {
        Err(ProjectError::ProjectNotEmpty(project_id.to_string()))?;
    }
    Ok(())
}
