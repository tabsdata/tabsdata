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

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, Description, GroupId, GroupName, System, UserId, UserName};

    #[ta_objects::Dao]
    #[dao(sql_table = "groups")]
    #[dxo(
        builder(try_from = GroupCreate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct GroupDB {
        #[builder(default)]
        #[dxo(extractor)]
        pub id: GroupId,
        #[dxo(builder(include), extractor)]
        pub name: GroupName,
        #[dxo(builder(include))]
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[builder(default = "System::from(false)")]
        pub system: System,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "groups__with_names")]
    #[inherits(GroupDB)]
    pub struct GroupDBWithNames {
        #[dxo(extractor)]
        pub id: GroupId,

        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[ta_objects::Dto]
    pub struct GroupCreate {
        pub name: GroupName,
        #[serde(default)]
        pub description: Description,
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "groups")]
    #[dxo(
        builder(try_from = GroupUpdate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct GroupDBUpdate {
        #[dxo(builder(include))]
        pub name: Option<GroupName>,
        #[dxo(builder(include))]
        pub description: Option<Description>,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
    }

    #[ta_objects::Dto]
    pub struct GroupUpdate {
        pub name: Option<GroupName>,
        pub description: Option<Description>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = GroupDBWithNames))]
    #[inherits(GroupDBWithNames)]
    pub struct Group {
        #[dto(list(pagination_by = "+"))]
        pub id: GroupId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: GroupName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
    }
}

pub use definitions::*;
