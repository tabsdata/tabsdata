//
// Copyright 2025 Tabs Data Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use ta_objects::{Buildable, TryUpdater};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDB;
use td_objects::types::basic::{AccessTokenId, CollectionId, RoleId, ToCollectionId, UserId};

pub(crate) async fn seed_inter_collection_permission(
    mut executor: impl QueryExecutor,
    from_collection: &CollectionId,
    to_collection: &ToCollectionId,
) -> Result<InterCollectionPermissionDB, TdError> {
    let request_context = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
    );

    let permission_db = InterCollectionPermissionDB::builder()
        .try_update_from(&request_context)?
        .from_collection_id(from_collection)
        .to_collection_id(to_collection)
        .build()?;

    // Field-less row; the caller's executor decides the scope, so `project_id`
    // defaults from it (use `db.scoped(p)` to land it in `p`).
    executor.reuse().live().insert(&permission_db).await?;
    Ok(permission_db)
}

#[cfg(test)]
pub mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDB;
    use td_objects::types::basic::{CollectionName, ProjectId, ToCollectionId, UserId};

    #[td_test::test(db)]
    async fn test_seed_inter_collection_permission(db: TdPool) {
        let c0 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c0").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let c1 = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("c1").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let permission = db
            .scoped(ProjectId::sandbox())
            .seed()
            .inter_collection_permission(&c0.id, &ToCollectionId::try_from(&c1.id).unwrap())
            .await
            .unwrap();

        let found = db
            .live()
            .fetch_all::<InterCollectionPermissionDB>(&permission.id)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(permission.id, found[0].id);
    }
}
