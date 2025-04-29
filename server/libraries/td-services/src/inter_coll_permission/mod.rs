//
// Copyright 2025. Tabs Data Inc.
//

use td_error::td_error;
mod layers;
pub mod services;

#[td_error]
pub enum InterCollectionPermissionError {
    #[error("The given collection does not have the given permission")]
    CollectionPermissionMismatch = 0,

    #[error("Cannot give inter collection permission to itself")]
    CannotGivePermissionToItself = 1,
}
