//
// Copyright 2025 Tabs Data Inc.
//

use crate::{LiveEntry, QueryExecutor};
use ta_objects::Buildable;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionCreateDB, CollectionDB, CollectionDBBuilder};
use td_objects::types::basic::{AtTime, CollectionName, Description, GroupId, UserId};

pub(crate) async fn seed_collection(
    db: impl QueryExecutor,
    collection_name: &CollectionName,
    created_by: &UserId,
) -> Result<CollectionDB, TdError> {
    seed_collection_in_group(db, collection_name, created_by, GroupId::default_group()).await
}

/// Seed a collection assigned to a specific group. The plain
/// [`seed_collection`] entrypoint delegates here with the `default` group.
pub(crate) async fn seed_collection_in_group(
    db: impl QueryExecutor,
    collection_name: &CollectionName,
    created_by: &UserId,
    group_id: GroupId,
) -> Result<CollectionDB, TdError> {
    let created_on = AtTime::now();
    let collection = CollectionCreateDB::builder()
        .name(collection_name.clone())
        .description(Description::default())
        .created_on(created_on)
        .created_by_id(created_by)
        .modified_on(created_on)
        .modified_by_id(created_by)
        .group_id(group_id)
        .build()?;

    db.live().insert(&collection).await?;

    Ok(CollectionDBBuilder::try_from(&collection)
        .unwrap()
        .build()?)
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::types::basic::{CollectionName, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_collection(db: TdPool) {
        let collection = db
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        let found = db
            .live()
            .fetch_one::<CollectionDB>(&collection.id)
            .await
            .unwrap();
        assert_eq!(found.id, collection.id);
        assert_eq!(found.name, collection.name);
        assert_eq!(found.description, collection.description);
        assert_eq!(found.created_on, collection.created_on);
        assert_eq!(found.created_by_id, collection.created_by_id);
        assert_eq!(found.modified_on, collection.modified_on);
        assert_eq!(found.modified_by_id, collection.modified_by_id);
    }
}
