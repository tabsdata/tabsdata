//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, InProject, ProjectLock, RW};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::types::basic::ProjectId;

pub async fn begin_locked(
    db: &TdPool,
    project: ProjectId,
) -> Result<GatedConnection<RW<ProjectLock>, InProject>, TdError> {
    in_project(db, project)
        .lock_project()
        .begin(Attempt::unretried())
        .await
}

fn in_project(db: &TdPool, project: ProjectId) -> DbPool<InProject> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
        .in_project(&project)
}
