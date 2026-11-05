//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use ta_objects::errors::SqlError;
use td_database::access::{Holds, InProject, LockInstant, ProjectLock};
use td_database::domain::{Apiserver, DomainConnection};
use td_objects::dxo::topology::{ProjectTopologyInsertDB, TopologyInstant};
use td_objects::types::basic::{ProjectId, TopologyVersionId};

pub struct Topology<E> {
    pub(crate) executor: E,
}

impl<M: Holds<ProjectLock>> Topology<&mut DomainConnection<Apiserver, InProject, M>> {
    /// Opens `project`'s next topology version at `at`, the instant its
    /// topology rows open and close at. The project's lock keeps versions in
    /// commit order: each bump reads the version the one before it opened.
    pub async fn bump(
        self,
        project: &ProjectId,
        at: LockInstant,
    ) -> Result<TopologyInstant, SqlError> {
        let version = ProjectTopologyInsertDB {
            id: TopologyVersionId::default(),
            project_id: *project,
        };
        self.executor.scd2().open(&version, at).await?;
        Ok(TopologyInstant::bumped(at))
    }
}
