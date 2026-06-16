//
// Copyright 2025 Tabs Data Inc.
//

use crate::role::RoleError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::role::{RoleDBBuilder, RoleDBWithNames};
use td_objects::types::basic::RoleKind;
use td_tower::extractors::Ref;

pub async fn assert_not_fixed(role: Ref<RoleDBWithNames>) -> Result<(), TdError> {
    if *role.fixed {
        Err(RoleError::FixedRole(role.name.clone()))?
    }

    Ok(())
}

/// Stamp `kind = Custom` on a role being created through the RBAC `role create`
/// surface. The `RoleDBBuilder` defaults `kind` to `RoleKind::Default`; only the
/// system seed and the per-project minting set the built-in kinds explicitly, so
/// every API-created role must be tagged `Custom` to make it detectable by the
/// RBAC->Simple representability check (TEDC condition 1).
pub async fn set_custom_role_kind(builder: Ref<RoleDBBuilder>) -> Result<RoleDBBuilder, TdError> {
    let mut builder = builder.deref().clone();
    builder.kind(RoleKind::Custom);
    Ok(builder)
}
