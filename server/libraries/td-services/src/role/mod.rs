//
// Copyright 2025 Tabs Data Inc.
//

use td_error::td_error;
use td_objects::types::string::RoleName;

mod layers;
pub mod services;

#[td_error]
pub enum RoleError {
    #[error("The role [{0}] is a fixed role and cannot be updated or deleted")]
    FixedRole(RoleName) = 0,
}
