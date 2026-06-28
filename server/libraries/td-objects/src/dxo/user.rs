//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::dxo::role::Role;
    use crate::types::basic::{
        AtTime, Email, FullName, Password, PasswordChangeTime, PasswordHash, PasswordMustChange,
        UserEnabled, UserId, UserName,
    };

    #[ta_objects::Dao]
    #[derive(Eq, PartialEq)]
    #[dao(sql_table = "users")]
    #[dxo(
        builder(try_from = UserDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct UserDB {
        #[builder(default)]
        #[dxo(extractor)]
        pub id: UserId,
        pub name: UserName,
        pub full_name: FullName,
        pub email: Option<Email>,
        #[dxo(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[dxo(extractor, setter)]
        pub password_hash: PasswordHash,
        #[dxo(setter)]
        pub password_set_on: PasswordChangeTime,
        #[dxo(extractor, setter)]
        pub password_must_change: PasswordMustChange,
        #[dxo(extractor, setter)]
        pub enabled: UserEnabled,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "users__with_names")]
    #[inherits(UserDB)]
    pub struct UserDBWithNames {
        #[dxo(extractor)]
        pub id: UserId,

        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[ta_objects::Dto]
    pub struct UserCreate {
        pub name: UserName,
        pub full_name: FullName,
        pub email: Option<Email>,
        pub password: Password,
        #[serde(default)]
        pub enabled: UserEnabled,
    }

    #[ta_objects::Dto]
    #[derive(Default)]
    #[serde(default)]
    pub struct UserUpdate {
        pub full_name: Option<FullName>,
        pub email: Option<Option<Email>>,
        pub password: Option<Password>,
        pub enabled: Option<UserEnabled>,
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "users")]
    #[dxo(
        builder(try_from = UserDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct UserUpdateDB {
        pub full_name: FullName,
        // Tri-state for update_values: outer `None` skips the column (unchanged),
        // `Some(None)` sets it NULL (clear), `Some(Some(v))` sets `v`. Seeded from
        // the current `UserDB`, overridden by the update layer.
        pub email: Option<Option<Email>>,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        pub password_hash: PasswordHash,
        pub password_set_on: PasswordChangeTime,
        pub password_must_change: PasswordMustChange,
        pub enabled: UserEnabled,
    }

    #[ta_objects::Dto]
    #[dto(list(on = UserDBWithNames))]
    #[inherits(UserDBWithNames)]
    pub struct UserRead {
        #[dto(list(pagination_by = "+"))]
        pub id: UserId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: UserName,
        #[dto(list(filter, filter_like, order_by))]
        pub full_name: FullName,
        #[dto(list(filter, filter_like))]
        pub email: Option<Email>,
    }

    #[ta_objects::Dto]
    #[dxo(builder(try_from = UserDBWithNames))]
    #[inherits(UserDBWithNames)]
    pub struct UserInfo {
        #[dxo(builder(skip), setter)]
        pub user_roles: Vec<Role>,
    }
}
