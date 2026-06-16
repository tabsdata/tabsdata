//
// Copyright 2026 Tabs Data Inc.
//

//! Cascade-drop a deleted project's three per-project roles
//! (`<name>_admin/_dev/_view`) together with their `role_permission` and
//! `user_role` rows.
//!
//! System-managed roles: a per-project role's lifetime is bound to its
//! project — minted atomically on create, dropped atomically on delete. This is
//! an internal SYSTEM cascade (not the user-facing role/permission delete
//! services), so it deliberately bypasses the `fixed` guard: the role rows and
//! their permission rows are created `fixed = true`, yet must be removable here.
//!
//! Project delete is *soft* (the `projects` row is renamed to `deleted_<id>`
//! and kept), but the per-project role rows are **hard-deleted**: they are
//! meaningless without a live project, and their names (`<name>_admin`, …) must
//! be freed so the project name can be re-created later. The `roles.project_id`
//! FK to `projects` is `ON DELETE RESTRICT`, but soft delete keeps the project
//! row, so dropping the roles first is unconstrained; `permissions.role_id` and
//! `users_roles.role_id` are likewise `ON DELETE RESTRICT`, so those rows are
//! deleted before the role rows.

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::types::basic::ProjectId;
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};

pub async fn delete_project_roles(
    mut conn: MutRef<DynamicConnection<'static>>,
    project_id: Ref<ProjectId>,
) -> Result<(), TdError> {
    // The `roles.project_id` column is `Option<ProjectId>`; its SQL condition
    // is keyed on `Option<ProjectId>` (the column's registered type), so match
    // it with `Some(project_id)` rather than the bare id.
    let by_project: Option<ProjectId> = Some(*project_id.deref());
    let roles: Vec<RoleDB> = conn.live().fetch_all::<RoleDB>(by_project).await?;

    // Drop the dependent rows first (FK `ON DELETE RESTRICT`), per role.
    for role in &roles {
        conn.live().delete::<PermissionDB>(role.id).await?;
        conn.live().delete::<UserRoleDB>(role.id).await?;
    }

    // Then the role rows themselves.
    if !roles.is_empty() {
        conn.live().delete::<RoleDB>(by_project).await?;
    }

    Ok(())
}
