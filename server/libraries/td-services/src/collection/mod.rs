//
// Copyright 2024 Tabsdata Inc.
//

use td_error::td_error;

pub mod service;

#[td_error]
pub enum CollectionError {
    #[error("The collection update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 0,
}
