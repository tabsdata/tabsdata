//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::topology::{ProjectTopologyDB, TopologyReadParam};
use td_objects::types::basic::ProjectId;
use td_queries::Queries;

#[td_error]
pub enum TopologyReadError {
    #[error("Read through either `topology_version` or `at`, not both")]
    VersionAndInstant = 0,
}

/// The version `param` names, the one current at its `at`, or the open one.
pub async fn read_version<M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, InProject, M>,
    project: &ProjectId,
    param: &TopologyReadParam,
) -> Result<ProjectTopologyDB, TdError> {
    Ok(match (&param.topology_version, &param.at) {
        (Some(_), Some(_)) => Err(TopologyReadError::VersionAndInstant)?,
        (Some(version), None) => {
            conn.timeline()
                .fetch_one::<ProjectTopologyDB>(version)
                .await?
        }
        (None, Some(at)) => {
            conn.scd2()
                .as_of(at)
                .fetch_one::<ProjectTopologyDB>(project)
                .await?
        }
        (None, None) => conn.scd2().fetch_one::<ProjectTopologyDB>(project).await?,
    })
}
