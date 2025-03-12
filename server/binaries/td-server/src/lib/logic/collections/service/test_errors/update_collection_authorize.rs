//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::error::CollectionError;
use crate::logic::collections::service::update_collection::UpdateCollectionService;
use td_error::assert_service_error;
use td_objects::collections::dto::CollectionUpdate;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_not_allowed_to_update_collection() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;
    seed_collection(&db, None, "ds0").await;

    let service = UpdateCollectionService::new(db.clone()).service().await;

    let update = CollectionUpdate::builder().name("ds00").build().unwrap();

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .update(CollectionParam::new("ds0"), update);

    assert_service_error(service, request, |err| match err {
        CollectionError::NotAllowedToUpdateCollections => {}
        other => panic!("Expected 'NotAllowedToUpdateCollections', got {:?}", other),
    })
    .await;
}
