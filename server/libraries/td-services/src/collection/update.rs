//
// Copyright 2025 Tabsdata Inc.
//

use crate::collection::CollectionError;
use crate::collection::services::connection::ensure_collection_name_free;
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::{
    ActiveCollectionDB, ActiveCollectionDBWithNames, CollectionRead, CollectionUpdate,
    CollectionUpdateDB,
};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId};
use td_queries::Queries;

pub fn update_collection_validate(update: &CollectionUpdate) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() && update.base_path.is_none() {
        Err(CollectionError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
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

pub async fn plan_collection_update<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection: &ActiveCollectionDB,
    update: &CollectionUpdate,
    update_db: &CollectionUpdateDB,
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
        ensure_collection_name_free(conn, name).await?;
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
        update: update_db.clone(),
        rename,
    })
}

/// The update transaction: apply the (store-confirmed) update and read the
/// result back.
pub async fn apply_collection_update<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    plan: &CollectionUpdatePlan,
) -> Result<CollectionRead, TdError> {
    conn.update::<CollectionUpdateDB>(&plan.update, plan.collection_id)
        .await?;
    let with_names: ActiveCollectionDBWithNames = conn.fetch_one(plan.collection_id).await?;
    CollectionRead::from_dao(with_names)
}
