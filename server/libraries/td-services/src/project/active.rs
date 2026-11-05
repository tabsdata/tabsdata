//
// Copyright 2026 Tabsdata Inc.
//

//! The live projects a per-project loop iterates.

use td_database::access::{DbPool, DbScope, Grant};
use td_database::domain::{Apiserver, ScopedBy};
use td_database::pool::ServingPool;
use td_error::TdError;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::ProjectId;
use td_queries::Queries;

pub(crate) async fn active_project_ids<S: DbScope, P: ServingPool>(
    db: &DbPool<S, P>,
    grant: Grant,
) -> Result<Vec<ProjectId>, TdError>
where
    Apiserver: ScopedBy<S>,
{
    let mut conn = db.read().await?;
    let projects: Vec<ProjectDB> = conn.get(grant).scd2().fetch_all(&()).await?;
    Ok(projects.into_iter().map(|p| p.project_id).collect())
}
