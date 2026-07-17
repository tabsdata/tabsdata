//
// Copyright 2025 Tabs Data Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use ta_objects::Buildable;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::collection::{CollectionCreateDB, CollectionDB};
use td_objects::dxo::group::GroupDB;
use td_objects::types::basic::{
    AtTime, CollectionName, CollectionType, Description, GroupId, GroupName, UserId,
};

pub(crate) async fn seed_collection(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection_name: &CollectionName,
    created_by: &UserId,
) -> Result<CollectionDB, TdError> {
    // Assign the collection to its project's own `default` group. On a
    // project-scoped executor (`db.scoped(p)`) the group resolves by name within
    // that project, so a hardcoded id could belong to the wrong project.
    let default_group = executor
        .reuse()
        .live()
        .fetch_one::<GroupDB>(&GroupName::default_group())
        .await?;
    seed_collection_in_group(executor, collection_name, created_by, default_group.id).await
}

/// Seed a collection assigned to a specific group. The plain
/// [`seed_collection`] entrypoint delegates here with the `default` group. The
/// field-less `CollectionCreateDB` defaults its `project_id` from the
/// executor's scope, so the caller picks the project via `db.scoped(p)`.
pub(crate) async fn seed_collection_in_group(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver>,
    collection_name: &CollectionName,
    created_by: &UserId,
    group_id: GroupId,
) -> Result<CollectionDB, TdError> {
    // Infer the flavour from the group id, mirroring the create service, so
    // seeded source/destination collections carry the right type.
    let collection_type = CollectionType::from_group_id(&group_id);

    let created_on = AtTime::now();
    let collection = CollectionCreateDB::builder()
        .name(collection_name.clone())
        .description(Description::default())
        .created_on(created_on)
        .created_by_id(created_by)
        .modified_on(created_on)
        .modified_by_id(created_by)
        .group_id(group_id)
        .collection_type(collection_type)
        .build()?;

    executor.reuse().live().insert(&collection).await?;

    // Read the row back so the returned model carries the DB-assigned
    // `project_id` (the scope's project), rather than guessing it. The `?`
    // converts `SqlError` into `TdError`.
    Ok(executor
        .live()
        .fetch_one::<CollectionDB>(&collection.id)
        .await?)
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
        let p = db.seed().test_project().await.unwrap();
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

        // Read it back through a project-scoped executor: serving connections
        // run as `td_app` under RLS, so an unscoped `db.live()` would see nothing.
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
