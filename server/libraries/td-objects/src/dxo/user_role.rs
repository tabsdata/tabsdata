//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
pub mod defs {
    use crate::dxo::crudl::RequestContext;
    use crate::types::bool::Fixed;
    use crate::types::id::{RoleId, UserId, UserRoleId};
    use crate::types::string::{RoleName, UserName};
    use crate::types::timestamp::AtTime;

    #[td_type::Dao]
    #[dao(sql_table = "users_roles")]
    #[td_type(
        updater(try_from = RequestContext, skip_all),
        updater(try_from = FixedUserRole, skip_all)
    )]
    pub struct UserRoleDB {
        #[td_type(extractor)]
        #[builder(default)]
        pub id: UserRoleId,
        #[td_type(setter)]
        pub user_id: UserId,
        #[td_type(setter)]
        #[td_type(updater(include, try_from = FixedUserRole))]
        pub role_id: RoleId,
        #[td_type(updater(include, try_from = RequestContext, field = "time"))]
        pub added_on: AtTime,
        #[td_type(updater(include, try_from = RequestContext, field = "user_id"))]
        pub added_by_id: UserId,
        #[builder(default = "Fixed::from(false)")]
        #[td_type(updater(include, try_from = FixedUserRole))]
        pub fixed: Fixed,
    }

    #[td_type::Dao]
    #[dao(sql_table = "users_roles__with_names")]
    #[inherits(UserRoleDB)]
    pub struct UserRoleDBWithNames {
        #[td_type(extractor)]
        pub role_id: RoleId,

        pub user: UserName,
        pub role: RoleName,
        pub added_by: UserName,
    }

    #[td_type::Dlo]
    pub struct FixedUserRole {
        #[builder(default = "RoleId::user()")]
        pub role_id: RoleId,
        #[builder(default = "Fixed::from(true)")]
        pub fixed: Fixed,
    }

    #[td_type::Dto]
    pub struct UserRoleCreate {
        #[td_type(extractor)]
        pub user: UserName,
    }

    #[td_type::Dto]
    #[dto(list(on = UserRoleDBWithNames))]
    #[td_type(builder(try_from = UserRoleDBWithNames))]
    pub struct UserRole {
        #[dto(list(pagination_by = "+"))]
        pub id: UserRoleId,
        pub user_id: UserId,
        pub role_id: RoleId,
        pub added_on: AtTime,
        pub added_by_id: UserId,
        pub fixed: Fixed,

        #[dto(list(filter, filter_like, order_by))]
        pub user: UserName,
        #[dto(list(filter, filter_like, order_by))]
        pub role: RoleName,
        #[dto(list(filter, filter_like, order_by))]
        pub added_by: UserName,
    }
}
