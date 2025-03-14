//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{AtTime, UserId, UserName};

#[td_type::Dao(sql_table = "users")]
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
    password_hash: String,
    password_set_on: AtTime,
    password_must_change: bool,
    enabled: bool,
}
