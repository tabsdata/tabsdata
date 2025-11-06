//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, Description, Fixed, RoleId, RoleName, UserId, UserName};

    #[td_type::Dao]
    #[dao(sql_table = "roles")]
    #[td_type(
        builder(try_from = RoleCreate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct RoleDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: RoleId,
        #[td_type(builder(include))]
        pub name: RoleName,
        #[td_type(builder(include))]
        pub description: Description,
        #[td_type(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[builder(default = "Fixed::from(false)")]
        pub fixed: Fixed,
    }

    #[td_type::Dao]
    #[dao(sql_table = "roles__with_names")]
    #[inherits(RoleDB)]
    pub struct RoleDBWithNames {
        #[td_type(extractor)]
        pub id: RoleId,

        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[td_type::Dto]
    pub struct RoleCreate {
        pub name: RoleName,
        #[serde(default)]
        pub description: Description,
    }

    #[td_type::Dao]
    #[dao(sql_table = "roles")]
    #[td_type(
        builder(try_from = RoleUpdate, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct RoleDBUpdate {
        #[td_type(builder(include))]
        pub name: Option<RoleName>,
        #[td_type(builder(include))]
        pub description: Option<Description>,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
    }

    #[td_type::Dto]
    pub struct RoleUpdate {
        pub name: Option<RoleName>,
        pub description: Option<Description>,
    }

    #[td_type::Dto]
    #[dto(list(on = RoleDBWithNames))]
    #[td_type(builder(try_from = RoleDBWithNames))]
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
