//
// Copyright 2026 Tabsdata Inc.
//

//! `groups` DAO/DXO.
//!
//! A group is an SCD2 timeline of versions, its kind fixed for its life.
//! `GroupDB` reads the versions (its open rows are the live groups),
//! `GroupDBWithNames` adds the `defined_by` name, and the
//! `Create` / `Update` DTOs are the API request surface.
//!
//! System rows (the seeded `default`, `sources`, `destinations` groups)
//! carry `system = true` and are rejected by `update` and `delete` at
//! the service layer; `kind` says which one a row is.

use crate::dxo::crudl::RequestContext;
use crate::dxo::scd2_interval::Scd2Interval;
use crate::types::basic::{
    AtTime, Description, GroupId, GroupIdName, GroupKind, GroupName, GroupVersionId, ProjectId,
    Scd2ValidFrom, System, UserId, UserLabel,
};
use ta_objects::FromDao;
use td_error::TdError;
use td_security::{DEFAULT_GROUP, DESTINATION_GROUP, SOURCE_GROUP};

/// The system groups every project is seeded with, as `(kind, name, description)`.
pub const SYSTEM_GROUPS: [(GroupKind, &str, &str); 3] = [
    (
        GroupKind::Default,
        DEFAULT_GROUP,
        "Default group — assigned to collections that do not specify a group.",
    ),
    (
        GroupKind::Sources,
        SOURCE_GROUP,
        "Sources group — assigned to ingestion collections.",
    ),
    (
        GroupKind::Destinations,
        DESTINATION_GROUP,
        "Destinations group — assigned to egestion collections.",
    ),
];

#[ta_objects::Dao]
#[dao(table = "groups", ops(insert), scd2, project_scoped)]
pub struct GroupInsertDB {
    #[dao(pk)]
    pub id: GroupVersionId,
    #[dao(partition_by)]
    pub group_id: GroupId,
    pub name: GroupName,
    pub description: Description,
    // Fixed for the group's life, from here down.
    pub system: System,
    pub kind: GroupKind,
    pub defined_by_id: UserId,
}

#[ta_objects::Dao]
#[dao(
    table = "groups",
    ops(select, close),
    scd2,
    project_scoped(unique_at(GroupName), unique_at(GroupIdName))
)]
pub struct GroupDB {
    #[dao(flatten)]
    pub base: GroupInsertDB,
    pub project_id: ProjectId,
    #[dao(flatten)]
    pub interval: Scd2Interval,
}

impl GroupDB {
    /// The version an update opens: this one's values with the update's
    /// changes on top.
    pub fn updated(&self, update: &GroupUpdate, ctx: &RequestContext) -> GroupInsertDB {
        GroupInsertDB {
            id: GroupVersionId::default(),
            name: update.name.clone().unwrap_or_else(|| self.name.clone()),
            description: update
                .description
                .clone()
                .unwrap_or_else(|| self.description.clone()),
            defined_by_id: ctx.user_id,
            ..self.base.clone()
        }
    }
}

#[ta_objects::Dao]
#[dao(
    view = "groups__with_names",
    scd2,
    project_scoped(unique_at(GroupName), unique_at(GroupIdName))
)]
pub struct GroupDBWithNames {
    #[dao(flatten)]
    pub base: GroupDB,
    pub defined_by: UserLabel,
}

#[ta_objects::Dto]
pub struct GroupCreate {
    pub name: GroupName,
    #[serde(default)]
    pub description: Description,
}

impl GroupCreate {
    pub fn to_dao(&self, ctx: &RequestContext) -> GroupInsertDB {
        GroupInsertDB {
            id: GroupVersionId::default(),
            group_id: GroupId::default(),
            name: self.name.clone(),
            description: self.description.clone(),
            system: System::FALSE,
            kind: GroupKind::Custom,
            defined_by_id: ctx.user_id,
        }
    }
}

#[ta_objects::Dto]
pub struct GroupUpdate {
    pub name: Option<GroupName>,
    pub description: Option<Description>,
}

#[ta_objects::Dto]
#[dto(list(on = GroupDBWithNames))]
pub struct Group {
    #[dto(list(pagination_by = "+"))]
    pub id: GroupVersionId,
    #[dto(list(filter))]
    pub group_id: GroupId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: GroupName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    pub system: System,
    pub kind: GroupKind,
    #[dto(list(filter, order_by, col = Scd2ValidFrom))]
    pub defined_on: AtTime,
    pub defined_by_id: UserId,
    #[dto(list(filter, filter_like, order_by))]
    pub defined_by: UserLabel,
}

impl FromDao for Group {
    type Dao = GroupDBWithNames;

    fn from_dao(dao: GroupDBWithNames) -> Result<Self, TdError> {
        let GroupDB { base, interval, .. } = dao.base;
        Ok(Self {
            id: base.id,
            group_id: base.group_id,
            name: base.name,
            description: base.description,
            system: base.system,
            kind: base.kind,
            defined_on: interval.valid_from.into(),
            defined_by_id: base.defined_by_id,
            defined_by: dao.defined_by,
        })
    }
}

#[ta_objects::UrlParam]
pub struct GroupParam {
    pub group: GroupIdName,
}
