//
// Copyright 2025 Tabsdata Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, Fixed, RoleId, RoleName, UserId, UserName, UserRoleId};

    #[ta_objects::Dao]
    #[dao(sql_table = "users_roles")]
    #[dxo(
        updater(try_from = RequestContext, skip_all),
        updater(try_from = FixedUserRole, skip_all)
    )]
    pub struct UserRoleDB {
        #[dxo(extractor)]
        #[builder(default)]
        pub id: UserRoleId,
        #[dxo(setter)]
        pub user_id: UserId,
        #[dxo(setter)]
        #[dxo(updater(include, try_from = FixedUserRole))]
        pub role_id: RoleId,
        #[dxo(updater(include, try_from = RequestContext, field = "time"))]
        pub added_on: AtTime,
        #[dxo(updater(include, try_from = RequestContext, field = "user_id"))]
        pub added_by_id: UserId,
        #[builder(default = "Fixed::from(false)")]
        #[dxo(updater(include, try_from = FixedUserRole))]
        pub fixed: Fixed,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "users_roles__with_names")]
    #[inherits(UserRoleDB)]
    pub struct UserRoleDBWithNames {
        #[dxo(extractor)]
        pub role_id: RoleId,

        pub user: UserName,
        pub role: RoleName,
        pub added_by: UserName,
    }

    #[ta_objects::Dlo]
    pub struct FixedUserRole {
        #[builder(default = "RoleId::default_role()")]
        pub role_id: RoleId,
        #[builder(default = "Fixed::from(true)")]
        pub fixed: Fixed,
    }

    #[ta_objects::Dto]
    pub struct UserRoleCreate {
        #[dxo(extractor)]
        pub user: UserName,
    }

    #[ta_objects::Dto]
    #[dto(list(on = UserRoleDBWithNames))]
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
