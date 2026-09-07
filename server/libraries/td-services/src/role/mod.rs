//
// Copyright 2025 Tabsdata Inc.
//

use td_error::td_error;
use td_objects::types::basic::RoleName;

pub mod services;

#[td_error]
pub enum RoleError {
    #[error("The role [{0}] is a fixed role and cannot be updated or deleted")]
    FixedRole(RoleName) = 0,
}

use td_error::TdError;
use td_objects::dxo::role::RoleDBWithNames;

pub fn assert_not_fixed(role: &RoleDBWithNames) -> Result<(), TdError> {
    if *role.base.fixed {
        Err(RoleError::FixedRole(role.base.name.clone()))?
    }

    Ok(())
}
