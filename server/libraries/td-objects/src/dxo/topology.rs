//
// Copyright 2026 Tabsdata Inc.
//

//! A project's topology versions.

use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{
    AtTime, CollectionId, DependencyId, DependencyPos, DependencyVersionId, Enabled, FunctionId,
    FunctionName, FunctionStatus, FunctionVersionId, OfTable, Private, ProjectId, Scd2ValidFrom,
    Scd2ValidTo, System, TableFunctionParamPos, TableId, TableName, TableStatus, TableVersionId,
    TagEntityId, TagEntityType, TagId, TagKind, TagValue, TagVersionId, TopologyTagKey,
    TopologyVersionId, TriggerBy, TriggerId, TriggerStatus, TriggerVersionId,
};
use crate::types::composed::TableVersions;
use ta_objects::FromDao;
use td_database::access::LockInstant;
use td_error::TdError;

/// A version of a project's graph: its functions, tables, dependencies and
/// triggers. Project creation opens the first, the empty graph; every unit of
/// work that writes graph rows opens the next, and those rows open or close
/// at that version's instant. Project, collection and group writes version
/// their own timelines without one.
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

/// The columns of a `functions` row a topology version is planned on.
#[ta_objects::Dao]
#[dao(table = "functions", ops(select), scd2, project_scoped)]
pub struct TopologyFunction {
    #[dao(pk)]
    pub id: FunctionVersionId,
    #[dao(partition_by)]
    pub function_id: FunctionId,
    pub collection_id: CollectionId,
    pub name: FunctionName,
    pub status: FunctionStatus,
    pub enabled: Enabled,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

/// The columns of a `tables` row a topology version is planned on.
#[ta_objects::Dao]
#[dao(table = "tables", ops(select), scd2, project_scoped)]
pub struct TopologyTable {
    #[dao(pk)]
    pub id: TableVersionId,
    #[dao(partition_by)]
    pub table_id: TableId,
    pub collection_id: CollectionId,
    pub name: TableName,
    pub function_id: FunctionId,
    pub function_version_id: FunctionVersionId,
    pub function_param_pos: Option<TableFunctionParamPos>,
    pub private: Private,
    pub status: TableStatus,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl TopologyTable {
    /// Written by the system rather than at a function's output position.
    pub fn system(&self) -> System {
        System::from(self.function_param_pos.is_some_and(|pos| *pos < 0))
    }
}

/// The columns of a `triggers` row a topology version is planned on.
#[ta_objects::Dao]
#[dao(table = "triggers", ops(select), scd2, project_scoped)]
pub struct TopologyTrigger {
    #[dao(pk)]
    pub id: TriggerVersionId,
    #[dao(partition_by)]
    pub trigger_id: TriggerId,
    pub function_id: FunctionId,
    pub trigger_by_function_id: TriggerBy<FunctionId>,
    pub trigger_by_table_id: TableId,
    pub status: TriggerStatus,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

/// The columns of a `dependencies` row a topology version is planned on.
#[ta_objects::Dao]
#[dao(table = "dependencies", ops(select), scd2, project_scoped)]
pub struct TopologyDependency {
    #[dao(pk)]
    pub id: DependencyVersionId,
    #[dao(partition_by)]
    pub dependency_id: DependencyId,
    pub function_id: FunctionId,
    pub table_id: TableId,
    pub table_function_id: OfTable<FunctionId>,
    pub table_versions: TableVersions,
    pub dep_pos: DependencyPos,
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
            id: dao.id,
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

#[ta_objects::Dao]
#[dao(table = "tags", ops(select), scd2, project_scoped)]
pub struct TopologyTag {
    #[dao(pk)]
    pub id: TagVersionId,
    #[dao(partition_by)]
    pub tag_id: TagId,
    pub entity_type: TagEntityType,
    pub entity_id: TagEntityId,
    pub kind: TagKind,
    pub key: TopologyTagKey,
    pub value: TagValue,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}
