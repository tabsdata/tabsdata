//
// Copyright 2026 Tabsdata Inc.
//

//! A project's topology versions.

use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{ProjectId, TopologyVersionId};
use td_database::access::LockInstant;

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
