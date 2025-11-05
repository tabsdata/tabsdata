//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::RoleError;
use td_error::TdError;
use td_objects::dxo::role::defs::RoleDBWithNames;
use td_tower::extractors::Input;

pub async fn assert_not_fixed(Input(role): Input<RoleDBWithNames>) -> Result<(), TdError> {
    if *role.fixed {
        Err(RoleError::FixedRole(role.name.clone()))?
    }

    Ok(())
}
