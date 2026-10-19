//
// Copyright 2026 Tabsdata Inc.
//

//! A project's topology versions.

use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{AtTime, ProjectId, Scd2ValidFrom, Scd2ValidTo, TopologyVersionId};
use ta_objects::FromDao;
use td_database::access::LockInstant;
use td_error::TdError;

/// A version of a project's topology. Project creation opens the first, the
/// empty topology; every topology write opens the next, and every topology
/// row it writes opens or closes at that version's instant.
#[ta_objects::Dao]
#[dao(table = "project_topologies", ops(insert), scd2, project_scoped)]
pub struct ProjectTopologyInsertDB {
    #[dao(pk)]
    pub id: TopologyVersionId,
    #[dao(partition_by)]
    pub project_id: ProjectId,
}

#[ta_objects::Dao]
#[dao(table = "project_topologies", ops(select), scd2, project_scoped)]
pub struct ProjectTopologyDB {
    #[dao(flatten)]
    pub base: ProjectTopologyInsertDB,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

/// The instant a unit of work's topology version opened at: the only instant a
/// topology row opens or closes at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopologyInstant(LockInstant);

impl TopologyInstant {
    /// Only the topology bump builds one; every call is an audit point.
    pub fn bumped(at: LockInstant) -> Self {
        Self(at)
    }
}

impl From<TopologyInstant> for LockInstant {
    fn from(at: TopologyInstant) -> Self {
        at.0
    }
}

/// A topology version as `/topology-versions` lists it: `valid_to` is null
/// while it is the project's open version.
#[ta_objects::Dto]
#[dto(list(on = ProjectTopologyDB))]
pub struct ProjectTopologyVersion {
    #[dto(list(pagination_by = "+", filter))]
    pub id: TopologyVersionId,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub valid_from: AtTime,
    pub valid_to: Scd2ValidTo,
}

impl FromDao for ProjectTopologyVersion {
    type Dao = ProjectTopologyDB;

    fn from_dao(dao: ProjectTopologyDB) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            valid_from: dao.interval.valid_from.into(),
            valid_to: dao.interval.valid_to,
        })
    }
}

/// `?topology_version=`: the topology version a read goes through, exact for
/// what an execution planned on. `?at=` reads through the version
/// current at an instant. Neither: the open version.
#[ta_objects::QueryParam]
pub struct TopologyReadParam {
    pub topology_version: Option<TopologyVersionId>,
    pub at: Option<AtTime>,
}
