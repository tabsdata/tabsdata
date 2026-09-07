//
// Copyright 2026 Tabsdata Inc.
//

//! The live projects a per-project loop iterates.

use td_database::access::{DbPool, Global, Grant};
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::ProjectId;
use td_queries::LiveEntry;

pub(crate) async fn active_project_ids(
    db: &DbPool<Global>,
    grant: Grant,
) -> Result<Vec<ProjectId>, TdError> {
    let mut conn = db.read().await?;
    let projects: Vec<ActiveProjectDB> = conn.get(grant).live().fetch_all(&()).await?;
    Ok(projects.into_iter().map(|p| p.id).collect())
}
