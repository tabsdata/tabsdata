//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::RoleError;
use td_error::TdError;
use td_objects::dxo::role::RoleDBWithNames;
use td_tower::extractors::Ref;

pub async fn assert_not_fixed(role: Ref<RoleDBWithNames>) -> Result<(), TdError> {
    if *role.fixed {
        Err(RoleError::FixedRole(role.name.clone()))?
    }

    Ok(())
}
