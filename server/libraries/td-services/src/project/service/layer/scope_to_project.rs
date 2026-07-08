//
// Copyright 2026 Tabs Data Inc.
//

//! Scope the connection to the just-created project.

use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::types::basic::ProjectId;
use td_tower::error::ConnectionError;
use td_tower::extractors::{MutRef, Ref};

/// Re-scope the connection from global/bypass to the freshly-created project,
/// so the system-group seeds (and any later project-scoped write in this
/// service) are written exactly as a normal `Project`-scoped request would: RLS
/// enforced, `project_id` defaulted from the GUC instead of stamped by hand.
///
/// `CreateProjectService` runs in `Global` scope (bypass) — correct for the
/// `projects` registry insert itself, which has no project context yet. The
/// connection already runs as the non-bypass `td_app` role (set at pool
/// connect); once the project row exists, `set_project_scope` switches
/// `app.project_id` to the new project and clears the global bypass, so
/// `seed_default_groups`' field-less `GroupDB` rows default to the new project
/// and RLS `WITH CHECK` genuinely enforces it. No-op on non-PostgreSQL backends.
pub async fn scope_to_created_project(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    project_id: Ref<ProjectId>,
) -> Result<(), TdError> {
    conn.get()
        .await?
        .set_project_scope(&project_id.to_string())
        .await
        .map_err(ConnectionError::CannotSetProjectScope)?;
    Ok(())
}
