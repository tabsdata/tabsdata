//
// Copyright 2026 Tabsdata Inc.
//

//! `groups` DAO/DXO.
//!
//! Mirrors the role DAO layout: `GroupDB` is the canonical table row,
//! `GroupDBWithNames` is the read-side view (joined to `users` for
//! `created_by` / `modified_by` name resolution), and the `Create` /
//! `Update` DTOs are the API request surface.
//!
//! System rows (the seeded `default`, `source`, `destination` groups)
//! carry `system = true` and are rejected by `update` and `delete` at
//! the service layer.

use crate::dxo::crudl::RequestContext;
use crate::types::basic::{
    AtTime, Created, Description, GroupId, GroupIdName, GroupName, Modified, ProjectId, System,
    UserId, UserName,
};
use ta_objects::FromDao;
use td_error::TdError;

#[ta_objects::Dao]
#[dao(table = "groups", ops(insert), project_scoped)]
pub struct GroupInsertDB {
    #[dao(pk)]
    pub id: GroupId,
    pub name: GroupName,
    pub description: Description,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub system: System,
}

/// The row as the database holds it: `project_id` is the `project_isolation` policy's, defaulted from the connection the insert ran on.
#[ta_objects::Dao]
#[dao(
    table = "groups",
    ops(select, delete),
    project_scoped(unique(GroupName))
)]
pub struct GroupDB {
    #[dao(flatten)]
    pub base: GroupInsertDB,
    pub project_id: ProjectId,
}

#[ta_objects::Dao]
#[dao(view = "groups__with_names", project_scoped(unique(GroupName)))]
pub struct GroupDBWithNames {
    #[dao(flatten)]
    pub base: GroupDB,
    pub created_by: Created<UserName>,
    pub modified_by: Modified<UserName>,
}

#[ta_objects::Dto]
pub struct GroupCreate {
    pub name: GroupName,
    #[serde(default)]
    pub description: Description,
}

impl GroupCreate {
    pub fn to_dao(self, ctx: &RequestContext) -> GroupInsertDB {
        GroupInsertDB {
            id: GroupId::default(),
            name: self.name,
            description: self.description,
            created_on: ctx.time.into(),
            created_by_id: ctx.user_id.into(),
            modified_on: ctx.time.into(),
            modified_by_id: ctx.user_id.into(),
            system: System::FALSE,
        }
    }
}

#[ta_objects::Dao]
#[dao(table = "groups", ops(update(GroupDB)), project_scoped)]
pub struct GroupUpdateDB {
    pub name: Option<GroupName>,
    pub description: Option<Description>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
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
    pub id: GroupId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: GroupName,
    #[dto(list(filter, filter_like, order_by))]
    pub description: Description,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub system: System,
    pub created_by: Created<UserName>,
    pub modified_by: Modified<UserName>,
}

impl FromDao for Group {
    type Dao = GroupDBWithNames;

    fn from_dao(dao: GroupDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.base.id,
            name: dao.base.base.name,
            description: dao.base.base.description,
            created_on: dao.base.base.created_on,
            created_by_id: dao.base.base.created_by_id,
            modified_on: dao.base.base.modified_on,
            modified_by_id: dao.base.base.modified_by_id,
            system: dao.base.base.system,
            created_by: dao.created_by,
            modified_by: dao.modified_by,
        })
    }
}

#[ta_objects::UrlParam]
pub struct GroupParam {
    pub group: GroupIdName,
}
