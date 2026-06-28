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
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId};
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
        // Re-wrap as `Some(inner)` so `update_values` writes the column; the DTO's
        // outer `None` falls through here (unchanged).
        if let Some(base_path) = &update.base_path {
            builder.base_path(Some(base_path.clone()));
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
    pub collection_id: CollectionId,
    pub old_name: CollectionName,
    pub new_name: CollectionName,
}

pub async fn plan_collection_update(
    mut conn: MutRef<DynamicConnection<'static>>,
    collection: Ref<CollectionDB>,
    update: Ref<CollectionUpdate>,
    update_db: Ref<CollectionUpdateDB>,
) -> Result<CollectionUpdatePlan, TdError> {
    // A rename to a name already taken is rejected up front with a clean
    // "already exists" error for every collection flavour (not just connected),
    // instead of the raw unique-constraint violation from the update write.
    // Connected collections additionally need this before the store rename:
    // otherwise the existing owner's worker artifact would be overwritten (and
    // deleted by the revert). Renaming to the current name is a no-op.
    if let Some(name) = &update.name
        && *name != collection.name
    {
        ensure_collection_name_free(&mut conn, name).await?;
    }
    let rename = match (&update.name, collection.connection_id) {
        (Some(name), Some(connection_id)) if *name != collection.name => Some(ConnectionRename {
            connection_id,
            collection_id: collection.id,
            old_name: collection.name.clone(),
            new_name: name.clone(),
        }),
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
