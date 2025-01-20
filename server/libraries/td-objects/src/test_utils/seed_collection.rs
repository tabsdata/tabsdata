//
// Copyright 2025 Tabs Data Inc.
//

use crate::collections::dao::CollectionBuilder;
use td_common::id;
use td_common::id::Id;
use td_common::time::UniqueUtc;
use td_database::sql::DbPool;

pub async fn seed_collection(db: &DbPool, creator_id: Option<String>, name: &str) -> Id {
    let creator_id = if let Some(creator_id) = creator_id {
        creator_id
    } else {
        td_database::test_utils::user_role_ids(db, td_security::ADMIN_USER)
            .await
            .0
    };

    let now = UniqueUtc::now_millis().await;

    let collection = CollectionBuilder::default()
        .id(id::id())
        .name(name)
        .description(format!("Description: {}", name))
        .created_on(now)
        .created_by_id(&creator_id)
        .modified_on(now)
        .modified_by_id(&creator_id)
        .build()
        .unwrap();

    const INSERT_SQL: &str = r#"
              INSERT INTO collections (
                    id,
                    name,
                    description,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#;

    sqlx::query(INSERT_SQL)
        .bind(collection.id())
        .bind(collection.name())
        .bind(collection.description())
        .bind(collection.created_on())
        .bind(collection.created_by_id())
        .bind(collection.modified_on())
        .bind(collection.modified_by_id())
        .execute(db)
        .await
        .unwrap();

    Id::try_from(collection.id()).unwrap()
}

#[cfg(test)]
pub mod tests {
    use crate::collections::dao::Collection;
    use crate::crudl::select_by;
    use crate::test_utils::seed_collection::seed_collection;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_seed_collection() {
        let before = UniqueUtc::now_millis().await;
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;

        let collection: Collection = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM collections WHERE id = ?",
            &collection_id.to_string(),
        )
        .await
        .unwrap();

        let creator_id = td_database::test_utils::user_role_ids(&db, td_security::ADMIN_USER)
            .await
            .0;

        assert_eq!(collection.id(), &collection_id.to_string());
        assert_eq!(collection.name(), "collection");
        assert_eq!(collection.description(), "Description: collection");
        assert!(collection.created_on() >= &before);
        assert_eq!(collection.created_by_id(), &creator_id);
        assert!(collection.modified_on() >= &before);
        assert_eq!(collection.modified_by_id(), &creator_id);
    }
}
