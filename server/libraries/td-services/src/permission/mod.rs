//
// Copyright 2025 Tabsdata Inc.
//

use td_error::td_error;

mod layers;
pub mod services;

// The target-based permission authorization construct is shared with the
// Simple-permission facade (grant/revoke).
pub(crate) use layers::{PermissionTarget, PermissionTargetScope, authorize_permission_target};

#[td_error]
pub enum PermissionError {
    #[error("The permission is a system permission that cannot be deleted")]
    PermissionIsFixed = 0,
    #[error("The given role does not have the given permission")]
    RolePermissionMismatch = 1,
    #[error("Role '{0}' is system-managed; its permissions cannot be modified")]
    RoleIsFixed(td_objects::types::basic::RoleName) = 2,
    #[error(
        "Permission '{0}' is not registered in the authorization registry (no built-in or extension entity kind declares it)"
    )]
    UnknownPermission(td_objects::types::basic::PermissionType) = 3,
}
