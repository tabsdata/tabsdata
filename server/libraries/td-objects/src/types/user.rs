//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::RequestContext;
use crate::types::basic::{
    AtTime, Email, FullName, Password, PasswordChangeTime, PasswordHash, PasswordMustChange,
    UserEnabled, UserId, UserName,
};

#[td_type::Dao(sql_table = "users")]
#[td_type(builder(try_from = UserDB))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct UserDB {
    #[builder(default)]
    #[td_type(extractor)]
    id: UserId,
    name: UserName,
    full_name: FullName,
    email: Option<Email>,
    #[td_type(updater(include, field = "time"))]
    created_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    created_by_id: UserId,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
    #[td_type(extractor, setter)]
    password_hash: PasswordHash,
    #[td_type(setter)]
    password_set_on: PasswordChangeTime,
    #[td_type(extractor, setter)]
    password_must_change: PasswordMustChange,
    #[td_type(extractor, setter)]
    enabled: UserEnabled,
}

#[td_type::Dao(sql_table = "users__with_names")]
pub struct UserDBWithNames {
    #[td_type(extractor)]
    id: UserId,
    name: UserName,
    full_name: FullName,
    email: Option<Email>,
    created_on: AtTime,
    created_by_id: UserId,
    created_by: UserName,
    modified_on: AtTime,
    modified_by_id: UserId,
    modified_by: UserName,
    password_set_on: AtTime,
    password_must_change: PasswordMustChange,
    enabled: UserEnabled,
}

#[td_type::Dto]
pub struct UserCreate {
    name: UserName,
    full_name: FullName,
    email: Option<Email>,
    password: Password,
    #[serde(default)]
    enabled: UserEnabled,
}

#[td_type::Dto]
pub struct UserUpdate {
    full_name: Option<FullName>,
    email: Option<Email>,
    password: Option<Password>,
    enabled: Option<UserEnabled>,
}

#[td_type::Dao]
#[td_type(builder(try_from = UserDB))]
#[td_type(updater(try_from = RequestContext, skip_all))]
pub struct UserUpdateDB {
    full_name: FullName,
    email: Option<Email>,
    #[td_type(updater(include, field = "time"))]
    modified_on: AtTime,
    #[td_type(updater(include, field = "user_id"))]
    modified_by_id: UserId,
    password_hash: PasswordHash,
    password_set_on: PasswordChangeTime,
    password_must_change: PasswordMustChange,
    enabled: UserEnabled,
}

#[td_type::Dto]
#[td_type(builder(try_from = UserDBWithNames))]
pub struct UserRead {
    id: UserId,
    name: UserName,
    full_name: FullName,
    email: Option<Email>,
    created_on: AtTime,
    created_by_id: UserId,
    created_by: UserName,
    modified_on: AtTime,
    modified_by_id: UserId,
    modified_by: UserName,
    password_set_on: AtTime,
    password_must_change: PasswordMustChange,
    enabled: UserEnabled,
}
