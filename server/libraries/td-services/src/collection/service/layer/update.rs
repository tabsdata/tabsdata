//
// Copyright 2025 Tabs Data Inc.
//

use crate::collection::CollectionError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionUpdate, CollectionUpdateDBBuilder};
use td_objects::tower_service::with::With;
use td_tower::extractors::Ref;

pub async fn update_collection_validate(update: Ref<CollectionUpdate>) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() && update.base_path.is_none() {
        return Err(CollectionError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub trait UpdateCollectionDBBuilderUpdate {
    fn update_collection_update_db_builder(
        update: Ref<CollectionUpdate>,
        builder: Ref<CollectionUpdateDBBuilder>,
    ) -> impl Future<Output = Result<CollectionUpdateDBBuilder, TdError>>;
}

impl UpdateCollectionDBBuilderUpdate for With<CollectionUpdate> {
    async fn update_collection_update_db_builder(
        update: Ref<CollectionUpdate>,
        builder: Ref<CollectionUpdateDBBuilder>,
    ) -> Result<CollectionUpdateDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        if let Some(name) = &update.name {
            builder.name(name);
        }
        if let Some(description) = &update.description {
            builder.description(description);
        }
        if let Some(base_path) = &update.base_path {
            builder.base_path(base_path);
        }

        Ok(builder)
    }
}
