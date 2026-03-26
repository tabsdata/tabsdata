//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, Description, Fixed, RoleId, RoleName, UserId, UserName};

    #[ta_objects::Dao]
    #[dao(sql_table = "roles")]
    #[dxo(
        builder(try_from = RoleCreate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct RoleDB {
        #[builder(default)]
        #[dxo(extractor)]
        pub id: RoleId,
        #[dxo(builder(include))]
        pub name: RoleName,
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
        #[builder(default = "Fixed::from(false)")]
        pub fixed: Fixed,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "roles__with_names")]
    #[inherits(RoleDB)]
    pub struct RoleDBWithNames {
        #[dxo(extractor)]
        pub id: RoleId,

        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[ta_objects::Dto]
    pub struct RoleCreate {
        pub name: RoleName,
        #[serde(default)]
        pub description: Description,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "roles")]
    #[dxo(
        builder(try_from = RoleUpdate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct RoleDBUpdate {
        #[dxo(builder(include))]
        pub name: Option<RoleName>,
        #[dxo(builder(include))]
        pub description: Option<Description>,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
    }

    #[ta_objects::Dto]
    pub struct RoleUpdate {
        pub name: Option<RoleName>,
        pub description: Option<Description>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = RoleDBWithNames))]
    #[inherits(RoleDBWithNames)]
    pub struct Role {
        #[dto(list(pagination_by = "+"))]
        pub id: RoleId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: RoleName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
    }
}
