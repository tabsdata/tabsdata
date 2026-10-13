//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::Attempt;
use td_database::access::{AcrossProjects, DbPool, GatedConnection, InProject, XW};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::types::basic::ProjectId;

pub async fn begin_exclusive(
    db: &TdPool,
    project: ProjectId,
) -> Result<GatedConnection<XW, InProject>, TdError> {
    DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects)
        .in_project(&project)
        .begin_exclusive(Attempt::unretried())
        .await
}
