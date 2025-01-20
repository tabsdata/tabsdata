//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::error::CollectionError;
use crate::logic::collections::service::delete_collection::DeleteCollectionService;
use td_common::error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_not_allowed_to_delete_collection() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;
    let collection_id = seed_collection(&db, None, "ds0").await;

    let service = DeleteCollectionService::new(db.clone()).service().await;

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .delete(CollectionParam::new(collection_id));

    assert_service_error(service, request, |err| match err {
        CollectionError::NotAllowedToDeleteCollections => {}
        other => panic!("Expected 'NotAllowedToDeleteCollections', got {:?}", other),
    })
    .await;
}
