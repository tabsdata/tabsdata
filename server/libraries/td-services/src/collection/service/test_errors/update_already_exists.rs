//
// Copyright 2025 Tabs Data Inc.
//

use crate::collection::service::update::UpdateCollectionService;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::tower_service::sql::SqlError;
use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
use td_objects::types::collection::CollectionUpdate;
use td_tower::td_service::TdService;

#[td_test::test(sqlx)]
#[tokio::test]
async fn test_update_already_existing(db: DbPool) {
    let name0 = CollectionName::try_from("ds0").unwrap();
    let _ = seed_collection(&db, &name0, &UserId::admin()).await;
    let name1 = CollectionName::try_from("ds1").unwrap();
    let _ = seed_collection(&db, &name1, &UserId::admin()).await;

    let service = UpdateCollectionService::with_defaults(db).service().await;

    let update = CollectionUpdate::builder()
        .name(Some(name0.clone()))
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    )
    .update(
        CollectionParam::builder()
            .try_collection(name1.to_string())
            .unwrap()
            .build()
            .unwrap(),
        update,
    );

    assert_service_error(service, request, |err| match err {
        SqlError::UpdateError(_, _, _, _) => {}
        other => panic!("Expected 'UpdateError', got {other:?}"),
    })
    .await;
}
