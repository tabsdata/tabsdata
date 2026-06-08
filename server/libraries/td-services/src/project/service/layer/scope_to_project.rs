//
// Copyright 2026 Tabs Data Inc.
//

//! Scope the connection to the just-created project.

use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::types::basic::ProjectId;
use td_tower::error::ConnectionError;
use td_tower::extractors::{MutRef, Ref};

/// Bind the connection to the freshly-created project so the system-group seeds
/// (and any later project-scoped write in this service) inherit `project_id`
/// from the column default instead of stamping it by hand.
///
/// `CreateProjectService` runs in `Global` scope (bypass) — correct for the
/// `projects` registry insert itself, but it leaves `app.project_id` unset.
/// Setting it here, once the project row exists, lets `seed_default_groups`
/// insert field-less `GroupDB` rows whose `project_id` defaults to the new
/// project. Bypass stays on, so RLS `WITH CHECK` still admits the writes; the
/// GUC only feeds the column default. No-op on non-PostgreSQL backends.
pub async fn scope_to_created_project(
    mut conn: MutRef<DynamicConnection<'static>>,
    project_id: Ref<ProjectId>,
) -> Result<(), TdError> {
    conn.set_project_scope(&project_id.to_string())
        .await
        .map_err(ConnectionError::CannotSetProjectScope)?;
    Ok(())
}
