//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{
    AtTime, Email, PasswordChangeTime, PasswordHash, PasswordMustChange, UserEnabled, UserId,
    UserName,
};

#[td_type::Dao(sql_table = "users")]
#[td_type(builder(try_from = UserDB))]
pub struct UserDB {
    #[td_type(extractor)]
    id: UserId,
    name: UserName,
    full_name: UserName,
    email: String,
    created_on: AtTime,
    created_by_id: UserId,
    modified_on: AtTime,
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

#[td_type::Dao(sql_table = "users_with_names")]
pub struct UserDBWithNames {
    id: UserId,
    name: UserName,
    full_name: UserName,
    email: Email,
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
