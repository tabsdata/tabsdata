//
// Copyright 2026 Tabsdata Inc.
//

//! The live projects a per-project loop iterates.

use td_database::access::{DbPool, DbScope, Grant, RO};
use td_database::domain::{Apiserver, ScopedBy, Supports};
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::ProjectId;
use td_queries::Queries;

pub(crate) async fn active_project_ids<S: DbScope>(
    db: &DbPool<S, Apiserver>,
    grant: Grant,
) -> Result<Vec<ProjectId>, TdError>
where
    Apiserver: ScopedBy<S> + Supports<RO>,
{
    let mut conn = db.read().await?;
    let projects: Vec<ActiveProjectDB> = conn.get(grant).fetch_all(&()).await?;
    Ok(projects.into_iter().map(|p| p.id).collect())
}
