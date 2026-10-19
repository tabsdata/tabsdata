//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use ta_objects::errors::SqlError;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_objects::dxo::topology::{ProjectTopologyInsertDB, TopologyInstant};
use td_objects::types::basic::{ProjectId, TopologyVersionId};

pub struct Topology<E> {
    pub(crate) executor: E,
}

impl<M: WriteMode> Topology<&mut DomainConnection<Apiserver, InProject, M>> {
    /// Opens `project`'s next topology version at `at`, the instant its
    /// topology rows open and close at. Serializable writes keep versions in
    /// commit order: a stale bump fails closing the already-closed version.
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
