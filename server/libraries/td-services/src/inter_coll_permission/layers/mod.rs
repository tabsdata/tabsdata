//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::InterCollectionPermissionError;
use td_error::TdError;
use td_objects::types::basic::{CollectionId, CollectionIdName, ToCollectionId};
use td_objects::types::permission::InterCollectionPermissionDBWithNames;
use td_objects::types::IdOrName;
use td_tower::extractors::Input;

pub async fn assert_collection_in_permission(
    Input(collection_id_name): Input<CollectionIdName>,
    Input(permission): Input<InterCollectionPermissionDBWithNames>,
) -> Result<(), TdError> {
    if let Some(collection_id) = collection_id_name.id() {
        if collection_id != permission.from_collection_id() {
            Err(InterCollectionPermissionError::CollectionPermissionMismatch)?
        }
    }
    if let Some(collection_name) = collection_id_name.name() {
        if collection_name != permission.from_collection() {
            Err(InterCollectionPermissionError::CollectionPermissionMismatch)?
        }
    }
    Ok(())
}

pub async fn assert_collection_and_to_collection_are_different(
    Input(collection): Input<CollectionId>,
    Input(to_collection): Input<ToCollectionId>,
) -> Result<(), TdError> {
    let to_collection = (*to_collection).try_into()?;
    if *collection == to_collection {
        Err(InterCollectionPermissionError::CannotGivePermissionToItself)?
    }
    Ok(())
}
