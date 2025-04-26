//
// Copyright 2025 Tabs Data Inc.
//

use td_error::td_error;

mod layers;
pub mod services;

#[td_error]
pub enum PermissionError {
    #[error("The permission is a system permission that cannot be deleted")]
    PermissionIsFixed = 0,
    #[error("The given role does not have the given permission")]
    RolePermissionMismatch = 1,
}
