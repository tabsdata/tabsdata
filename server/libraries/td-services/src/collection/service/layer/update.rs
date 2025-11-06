//
// Copyright 2025 Tabs Data Inc.
//

use crate::collection::CollectionError;
use async_trait::async_trait;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionUpdate, CollectionUpdateDBBuilder};
use td_objects::tower_service::from::With;
use td_tower::extractors::Input;

pub async fn update_collection_validate(
    Input(update): Input<CollectionUpdate>,
) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() {
        return Err(CollectionError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

#[async_trait]
pub trait UpdateCollectionDBBuilderUpdate {
    async fn update_collection_update_db_builder(
        update: Input<CollectionUpdate>,
        builder: Input<CollectionUpdateDBBuilder>,
    ) -> Result<CollectionUpdateDBBuilder, TdError>;
}

#[async_trait]
impl UpdateCollectionDBBuilderUpdate for With<CollectionUpdate> {
    async fn update_collection_update_db_builder(
        Input(update): Input<CollectionUpdate>,
        Input(builder): Input<CollectionUpdateDBBuilder>,
    ) -> Result<CollectionUpdateDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        if update.name.is_some() {
            builder.name(update.name.as_ref().unwrap());
        }
        if update.description.is_some() {
            builder.description(update.description.as_ref().unwrap());
        }

        Ok(builder)
    }
}
