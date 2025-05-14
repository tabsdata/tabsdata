//
// Copyright 2024 Tabs Data Inc.
//

use td_error::td_error;

pub mod service;

#[td_error]
pub enum CollectionError {
    #[error("The collection update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 0,
    #[error("Collection already exists")]
    AlreadyExists = 1,

    #[error("A non admin user cannot create a collection")]
    NotAllowedToCreateCollections = 2000,
    #[error("A non admin user cannot update a collection")]
    NotAllowedToUpdateCollections = 2001,
    #[error("A user cannot delete a collection")]
    NotAllowedToDeleteCollections = 2002,

    #[error("{0}")]
    ShouldNotHappen(String) = 5001,
}
