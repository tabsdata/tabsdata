//
// Copyright 2025. Tabs Data Inc.
//

use crate::inter_coll_permission::InterCollectionPermissionError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDBWithNames;
use td_objects::types::basic::{CollectionId, CollectionIdName, ToCollectionId};
use td_tower::extractors::Ref;

pub async fn assert_collection_in_permission(
    collection_id_name: Ref<CollectionIdName>,
    permission: Ref<InterCollectionPermissionDBWithNames>,
) -> Result<(), TdError> {
    match collection_id_name.deref() {
        CollectionIdName::Id(id) => {
            if id != &permission.from_collection_id {
                Err(InterCollectionPermissionError::CollectionPermissionMismatch)?
            }
        }
        CollectionIdName::Name(name) => {
            if name != &permission.from_collection {
                Err(InterCollectionPermissionError::CollectionPermissionMismatch)?
            }
        }
    }

    Ok(())
}

pub async fn assert_collection_and_to_collection_are_different(
    collection: Ref<CollectionId>,
    to_collection: Ref<ToCollectionId>,
) -> Result<(), TdError> {
    let to_collection = (*to_collection).try_into()?;
    if *collection == to_collection {
        Err(InterCollectionPermissionError::CannotGivePermissionToItself)?
    }
    Ok(())
}
