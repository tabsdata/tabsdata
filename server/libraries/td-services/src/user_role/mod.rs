//
// Copyright 2025 Tabs Data Inc.
//

use td_error::td_error;
use td_objects::types::string::{RoleName, UserName};

mod layers;
pub mod services;

#[td_error]
pub enum UserRoleError {
    #[error("The role [{0}] for user [{1}] is a fixed user role and cannot be deleted")]
    FixedUserRole(UserName, RoleName) = 0,
}
