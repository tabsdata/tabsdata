//
// Copyright 2024 Tabsdata Inc.
//

use td_error::td_error;

mod resolve;
pub mod services;

pub use resolve::resolve_collection_id;

#[td_error]
pub enum CollectionError {
    #[error("The collection update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 0,
}
