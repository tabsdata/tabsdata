//
// Copyright 2025 Tabs Data Inc.
//

use crate::collection::CollectionError;
use crate::collection::service::connection::ensure_collection_name_free;
use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::collection::{
    CollectionDB, CollectionDBWithNames, CollectionRead, CollectionReadBuilder, CollectionUpdate,
    CollectionUpdateDB, CollectionUpdateDBBuilder,
};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, ConnectionId};
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};

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

/// Everything the update transaction persists, resolved up front, plus the
/// store rename the orchestrator must perform before it (a
/// source/destination collection's name is its connection's name — the
/// connection follows the rename).
#[derive(Debug, Clone)]
pub struct CollectionUpdatePlan {
    pub collection_id: CollectionId,
    pub update: CollectionUpdateDB,
    pub rename: Option<ConnectionRename>,
}

/// A rename of a connected collection: the stored document's `name` and the
/// worker artifact must move with it. `old_name` is kept so a failed
/// transaction can put the store back.
#[derive(Debug, Clone)]
pub struct ConnectionRename {
    pub connection_id: ConnectionId,
    pub old_name: String,
    pub new_name: String,
}

pub async fn plan_collection_update(
    mut conn: MutRef<DynamicConnection<'static>>,
    collection: Ref<CollectionDB>,
    update: Ref<CollectionUpdate>,
    update_db: Ref<CollectionUpdateDB>,
) -> Result<CollectionUpdatePlan, TdError> {
    let rename = match (&update.name, collection.connection_id) {
        (Some(name), Some(connection_id)) if *name != collection.name => {
            // The store rename happens BEFORE the TX, so the target name must
            // be known free up front — otherwise the existing owner's worker
            // artifact would be overwritten (and deleted by the revert).
            ensure_collection_name_free(&mut conn, name).await?;
            Some(ConnectionRename {
                connection_id,
                old_name: collection.name.to_string(),
                new_name: name.to_string(),
            })
        }
        _ => None,
    };
    Ok(CollectionUpdatePlan {
        collection_id: collection.id,
        update: update_db.deref().clone(),
        rename,
    })
}

/// The update transaction: apply the (store-confirmed) update and read the
/// result back.
pub async fn apply_collection_update(
    mut conn: MutRef<DynamicConnection<'static>>,
    plan: Ref<CollectionUpdatePlan>,
) -> Result<CollectionRead, TdError> {
    conn.live()
        .update::<CollectionUpdateDB, CollectionDB>(&plan.update, plan.collection_id)
        .await?;
    let with_names: CollectionDBWithNames = conn.live().fetch_one(plan.collection_id).await?;
    Ok(CollectionReadBuilder::try_from(&with_names)?.build()?)
}
