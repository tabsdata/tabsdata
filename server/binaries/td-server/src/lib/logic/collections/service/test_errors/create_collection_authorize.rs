//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::error::CollectionError;
use crate::logic::collections::service::create_collection::CreateCollectionService;
use td_error::assert_service_error;
use td_objects::collections::dto::CollectionCreateBuilder;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_user::seed_user;

#[tokio::test]
async fn test_not_allowed_to_create_collection() {
    let db = td_database::test_utils::db().await.unwrap();
    let user_id = seed_user(&db, None, "u0", false).await;

    let service = CreateCollectionService::new(db.clone()).service().await;

    let create = CollectionCreateBuilder::default()
        .name("ds0")
        .description("DS0")
        .build()
        .unwrap();

    let request = RequestContext::with(&user_id.to_string(), "r", false)
        .await
        .create((), create);

    assert_service_error(service, request, |err| match err {
        CollectionError::NotAllowedToCreateCollections => {}
        other => panic!("Expected 'NotAllowedToCreateCollections', got {:?}", other),
    })
    .await;
}
