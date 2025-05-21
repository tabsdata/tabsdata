//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::{DaoQueries, Insert};
use crate::types::basic::{AtTime, CollectionName, Description, UserId};
use crate::types::collection::CollectionDB;
use td_database::sql::DbPool;

pub async fn seed_collection(
    db: &DbPool,
    collection_name: &CollectionName,
    created_by: &UserId,
) -> CollectionDB {
    let created_on = AtTime::now().await;
    let collection = CollectionDB::builder()
        .name(collection_name)
        .description(Description::default())
        .created_on(&created_on)
        .created_by_id(created_by)
        .modified_on(&created_on)
        .modified_by_id(created_by)
        .build()
        .unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&collection)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    collection
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SelectBy;
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(sqlx)]
    async fn test_seed_collection(db: DbPool) {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

        let found: CollectionDB = DaoQueries::default()
            .select_by::<CollectionDB>(&collection.id())
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(found.id(), collection.id());
        assert_eq!(found.name(), collection.name());
        assert_eq!(found.description(), collection.description());
        assert_eq!(found.created_on(), collection.created_on());
        assert_eq!(found.created_by_id(), collection.created_by_id());
        assert_eq!(found.modified_on(), collection.modified_on());
        assert_eq!(found.modified_by_id(), collection.modified_by_id());
    }
}
