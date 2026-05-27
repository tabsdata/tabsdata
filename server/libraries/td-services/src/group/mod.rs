//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;
use td_objects::types::basic::GroupName;

mod layers;
pub mod services;

#[td_error]
pub enum GroupError {
    #[error("The group [{0}] is a system group and cannot be updated or deleted")]
    SystemGroup(GroupName) = 0,
}
