//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{handle_sql_err, ReadRequest, RequestContext};
use crate::sql::{DaoQueries, Insert, SelectBy};
use crate::types::basic::{AccessTokenId, CollectionId, RoleId, ToCollectionId, UserId};
use crate::types::permission::{InterCollectionPermissionDB, InterCollectionPermissionDBBuilder};
use crate::types::SqlEntity;
use td_database::sql::DbPool;
use td_error::TdError;

pub async fn seed_inter_collection_permission(
    db: &DbPool,
    from_collection: &CollectionId,
    to_collection: &ToCollectionId,
) -> InterCollectionPermissionDB {
    let request_context: ReadRequest<String> = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sec_admin(),
        true,
    )
    .read("");
    let request_context = request_context.context();

    let mut builder = InterCollectionPermissionDB::builder();
    builder
        .from_collection_id(from_collection)
        .to_collection_id(to_collection);
    let builder = InterCollectionPermissionDBBuilder::try_from((request_context, builder)).unwrap();
    let builder = InterCollectionPermissionDBBuilder::from((from_collection, builder));

    let permission_db = builder.build().unwrap();

    let queries = DaoQueries::default();
    queries
        .insert(&permission_db)
        .unwrap()
        .build()
        .execute(db)
        .await
        .unwrap();

    permission_db
}

pub async fn get_inter_collection_permissions<E: SqlEntity>(
    db: &DbPool,
    by: &E,
) -> Result<Vec<InterCollectionPermissionDB>, TdError> {
    let queries = DaoQueries::default();
    queries
        .select_by::<InterCollectionPermissionDB>(&by)?
        .build_query_as()
        .fetch_all(db)
        .await
        .map_err(handle_sql_err)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::types::basic::CollectionName;

    #[tokio::test]
    async fn test_seed_inter_collection_permission() {
        let db = td_database::test_utils::db().await.unwrap();
        let c0 = seed_collection(
            &db,
            &CollectionName::try_from("c0").unwrap(),
            &UserId::admin(),
        )
        .await;
        let c1 = seed_collection(
            &db,
            &CollectionName::try_from("c1").unwrap(),
            &UserId::admin(),
        )
        .await;
        let permission = seed_inter_collection_permission(
            &db,
            c0.id(),
            &ToCollectionId::try_from(c1.id()).unwrap(),
        )
        .await;

        let found = get_inter_collection_permissions(&db, permission.id())
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(permission.id(), found[0].id());
    }
}
