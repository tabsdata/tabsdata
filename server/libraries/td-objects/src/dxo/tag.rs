//
// Copyright 2026 Tabsdata Inc.
//

//! Topology tags open and close at topology instants; system and user tags at
//! the lock instant, and planning never reads them.

use crate::FromDao;
use crate::dxo::scd2_interval::Scd2Interval;
use crate::dxo::topology::TopologyInstant;
use crate::types::basic::{
    ProjectId, TagEntityId, TagEntityType, TagId, TagKey, TagKind, TagValue, TagVersionId,
    TopologyTagKey, UserId, UserTagKey,
};
use ta_objects::sql_condition::SqlCondition;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "tags", ops(insert), scd2(at = TopologyInstant), project_scoped)]
pub struct TopologyTagInsertDB {
    #[dao(pk)]
    pub id: TagVersionId,
    #[dao(partition_by)]
    pub tag_id: TagId,
    pub entity_type: TagEntityType,
    pub entity_id: TagEntityId,
    kind: TagKind,
    pub key: TopologyTagKey,
    pub value: TagValue,
    pub defined_by_id: UserId,
}

impl TopologyTagInsertDB {
    pub fn new(
        tag_id: TagId,
        entity_type: TagEntityType,
        entity_id: TagEntityId,
        key: TopologyTagKey,
        value: TagValue,
        defined_by_id: UserId,
    ) -> Result<Self, TdError> {
        key.check_applies_to(&entity_type)?;
        Ok(Self {
            id: TagVersionId::default(),
            tag_id,
            entity_type,
            entity_id,
            kind: TagKind::Topology,
            key,
            value,
            defined_by_id,
        })
    }
}

#[ta_objects::Dao]
#[dao(
    table = "tags",
    ops(select, close),
    scd2(at = TopologyInstant),
    project_scoped(unique_at(TagEntityType, TagEntityId, TopologyTagKey))
)]
pub struct TopologyTagDB {
    #[dao(flatten)]
    pub base: TopologyTagInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl TopologyTagDB {
    /// Only a topology key decodes here.
    pub fn topology() -> impl SqlCondition<Self> {
        TagKind::Topology
    }
}

#[ta_objects::Dao]
#[dao(table = "tags", ops(insert), scd2, project_scoped)]
pub struct UserTagInsertDB {
    #[dao(pk)]
    pub id: TagVersionId,
    #[dao(partition_by)]
    pub tag_id: TagId,
    pub entity_type: TagEntityType,
    pub entity_id: TagEntityId,
    kind: TagKind,
    pub key: UserTagKey,
    pub value: TagValue,
    pub defined_by_id: UserId,
}

impl UserTagInsertDB {
    pub fn new(
        tag_id: TagId,
        entity_type: TagEntityType,
        entity_id: TagEntityId,
        key: UserTagKey,
        value: TagValue,
        defined_by_id: UserId,
    ) -> Self {
        Self {
            id: TagVersionId::default(),
            tag_id,
            entity_type,
            entity_id,
            kind: TagKind::User,
            key,
            value,
            defined_by_id,
        }
    }
}

#[ta_objects::Dao]
#[dao(table = "tags", ops(close), scd2, project_scoped)]
pub struct UserTagDB {
    #[dao(flatten)]
    pub base: UserTagInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl UserTagDB {
    /// Only user tags close at the lock instant; topology tags close with
    /// their entity, at a topology instant.
    pub fn user() -> impl SqlCondition<Self> {
        TagKind::User
    }
}

#[ta_objects::Dao]
#[dao(
    table = "tags",
    ops(select),
    scd2,
    project_scoped(unique_at(TagEntityType, TagEntityId, TagKey))
)]
pub struct TagDB {
    #[dao(pk)]
    pub id: TagVersionId,
    #[dao(partition_by)]
    pub tag_id: TagId,
    pub project_id: ProjectId,
    pub entity_type: TagEntityType,
    pub entity_id: TagEntityId,
    pub kind: TagKind,
    pub key: TagKey,
    pub value: TagValue,
    pub defined_by_id: UserId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

/// An open tag as `/tags` lists them, across every entity of the project.
#[ta_objects::Dto]
#[dto(list(on = TagDB))]
#[derive(Eq, PartialEq)]
pub struct Tag {
    #[dto(list(pagination_by = "+"))]
    pub id: TagVersionId,
    #[dto(list(filter, order_by))]
    pub entity_type: TagEntityType,
    #[dto(list(filter))]
    pub entity_id: TagEntityId,
    #[dto(list(filter))]
    pub kind: TagKind,
    #[dto(list(filter, filter_like, order_by))]
    pub key: TagKey,
    #[dto(list(filter, filter_like, order_by))]
    pub value: TagValue,
}

impl FromDao for Tag {
    type Dao = TagDB;

    fn from_dao(dao: TagDB) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.id,
            entity_type: dao.entity_type,
            entity_id: dao.entity_id,
            kind: dao.kind,
            key: dao.key,
            value: dao.value,
        })
    }
}
