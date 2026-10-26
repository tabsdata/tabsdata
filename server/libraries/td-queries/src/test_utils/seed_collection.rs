//
// Copyright 2025 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use crate::test_utils::topology::seed_instant;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::collection::{CollectionDB, CollectionInsertDB};
use td_objects::dxo::group::GroupDB;
use td_objects::types::basic::{
    CollectionId, CollectionName, CollectionType, CollectionVersionId, Description, GroupName,
    UserId,
};

pub(crate) async fn seed_collection(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    collection_name: &CollectionName,
    created_by: &UserId,
) -> Result<CollectionDB, TdError> {
    // Assign the collection to its project's own `default` group: the name is
    // unique only within the project, hence the `InProject` executor.
    let default_group = executor
        .reuse()
        .scd2()
        .fetch_one::<GroupDB>(&GroupName::default_group())
        .await?;
    seed_collection_in_group(executor, collection_name, created_by, &default_group).await
}

/// Seed a collection assigned to a specific group. The plain
/// [`seed_collection`] entrypoint delegates here with the `default` group. The
/// field-less `CollectionInsertDB` defaults its `project_id` from the
/// executor's scope, so the caller picks the project via `db.scoped(p)`.
pub(crate) async fn seed_collection_in_group(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject>,
    collection_name: &CollectionName,
    created_by: &UserId,
    group: &GroupDB,
) -> Result<CollectionDB, TdError> {
    let collection_type = CollectionType::from_group_kind(&group.base.kind);

    let at = seed_instant();
    let collection = CollectionInsertDB {
        id: CollectionVersionId::default(),
        collection_id: CollectionId::default(),
        name: collection_name.clone(),
        description: Description::default(),
        base_path: None,
        group_id: group.base.group_id,
        collection_type,
        connection_id: None,
        defined_by_id: *created_by,
    };

    executor.reuse().scd2().open(&collection, at).await?;

    // Read the row back so the returned model carries the DB-assigned
    // `project_id` (the scope's project), rather than guessing it. The `?`
    // converts `SqlError` into `TdError`.
    Ok(executor
        .scd2()
        .fetch_one::<CollectionDB>(&collection.collection_id)
        .await?)
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::types::basic::{CollectionName, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_seed_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.base.project_id)
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        // Read it back through a project-scoped executor: serving connections
        // run as `td_app` under RLS, so an unscoped read would see nothing.
        let found = db
            .scoped(p.base.project_id)
            .scd2()
            .fetch_one::<CollectionDB>(&collection.base.collection_id)
            .await
            .unwrap();
        assert_eq!(found.base.collection_id, collection.base.collection_id);
        assert_eq!(found.base.name, collection.base.name);
        assert_eq!(found.base.description, collection.base.description);
        assert_eq!(found.interval, collection.interval);
        assert_eq!(found.base.defined_by_id, collection.base.defined_by_id);
    }
}
