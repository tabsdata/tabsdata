//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::dxo]
mod definitions {
    use crate::dxo::auth::UserInfoUserRoleDB;
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::permission::Permission;
    use crate::types::basic::{
        AtTime, Email, FullName, Password, PasswordChangeTime, PasswordHash, PasswordMustChange,
        RoleId, RoleName, UserEnabled, UserId, UserName,
    };

    #[td_type::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "users")]
    #[td_type(
        builder(try_from = UserDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct UserDB {
        #[builder(default)]
        #[td_type(extractor)]
        pub id: UserId,
        pub name: UserName,
        pub full_name: FullName,
        pub email: Option<Email>,
        #[td_type(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[td_type(extractor, setter)]
        pub password_hash: PasswordHash,
        #[td_type(setter)]
        pub password_set_on: PasswordChangeTime,
        #[td_type(extractor, setter)]
        pub password_must_change: PasswordMustChange,
        #[td_type(extractor, setter)]
        pub enabled: UserEnabled,
    }

    #[td_type::Dao]
    #[dao(sql_table = "users__with_names")]
    #[inherits(UserDB)]
    pub struct UserDBWithNames {
        #[td_type(extractor)]
        pub id: UserId,

        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[td_type::Dto]
    pub struct UserCreate {
        pub name: UserName,
        pub full_name: FullName,
        pub email: Option<Email>,
        pub password: Password,
        #[serde(default)]
        pub enabled: UserEnabled,
    }

    #[td_type::Dto]
    pub struct UserUpdate {
        pub full_name: Option<FullName>,
        pub email: Option<Email>,
        pub password: Option<Password>,
        pub enabled: Option<UserEnabled>,
    }

    #[td_type::Dao]
    #[dao(sql_table = "users")]
    #[td_type(
        builder(try_from = UserDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct UserUpdateDB {
        pub full_name: FullName,
        pub email: Option<Email>,
        #[td_type(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[td_type(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        pub password_hash: PasswordHash,
        pub password_set_on: PasswordChangeTime,
        pub password_must_change: PasswordMustChange,
        pub enabled: UserEnabled,
    }

    #[td_type::Dto]
    #[dto(list(on = UserDBWithNames))]
    #[td_type(builder(try_from = UserDBWithNames))]
    #[inherits(UserDBWithNames)]
    pub struct UserRead {
        #[dto(list(pagination_by = "+"))]
        pub id: UserId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: UserName,
        #[dto(list(filter, filter_like, order_by))]
        pub full_name: FullName,
        #[dto(list(filter, filter_like))] // TODO should we allow order by on nullable??
        pub email: Option<Email>,
    }

    #[td_type::Dto]
    #[td_type(builder(try_from = UserDBWithNames))]
    #[inherits(UserDBWithNames)]
    pub struct UserInfo {
        #[td_type(builder(skip), setter)]
        pub current_role_id: RoleId,
        #[td_type(builder(skip), setter)]
        pub current_permissions: Vec<Permission>,
        #[td_type(builder(skip), setter)]
        pub user_roles: Vec<UserInfoRoleIdName>,
    }

    //NOTE: we cannot use RoleIdName as there is already one in the API.
    //      For OpenAPI 'there can be only one'
    #[td_type::Dto]
    #[td_type(builder(try_from = UserInfoUserRoleDB))]
    pub struct UserInfoRoleIdName {
        #[td_type(builder(field = "role_id"))]
        pub id: RoleId,
        #[td_type(builder(field = "role"))]
        pub name: RoleName,
    }
}
