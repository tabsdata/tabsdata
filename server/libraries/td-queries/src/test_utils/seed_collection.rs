//
// Copyright 2025 Tabs Data Inc.
//

use crate::BasicQueries;
use ta_objects::Buildable;
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionCreateDB, CollectionDB, CollectionDBBuilder};
use td_objects::types::basic::{AtTime, CollectionName, Description, UserId};

pub(crate) async fn seed_collection(
    db: impl SeaExecutor + Send,
    collection_name: &CollectionName,
    created_by: &UserId,
) -> Result<CollectionDB, TdError> {
    let created_on = AtTime::now();
    let collection = CollectionCreateDB::builder()
        .name(collection_name.clone())
        .description(Description::default())
        .created_on(created_on)
        .created_by_id(created_by)
        .modified_on(created_on)
        .modified_by_id(created_by)
        .build()?;

    db.insert(&collection).await?;

    Ok(CollectionDBBuilder::try_from(&collection)
        .unwrap()
        .build()?)
}

#[cfg(test)]
mod tests {
    use crate::BasicQueries;
    use crate::test_utils::SeedQueries;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::types::basic::{CollectionName, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_seed_collection(db: TdPool) {
        let collection = db
            .seed_collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        let found = db.fetch_one::<CollectionDB>(&collection.id).await.unwrap();
        assert_eq!(found.id, collection.id);
        assert_eq!(found.name, collection.name);
        assert_eq!(found.description, collection.description);
        assert_eq!(found.created_on, collection.created_on);
        assert_eq!(found.created_by_id, collection.created_by_id);
        assert_eq!(found.modified_on, collection.modified_on);
        assert_eq!(found.modified_by_id, collection.modified_by_id);
    }
}
