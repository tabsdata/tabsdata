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
use td_objects::test_utils::seed_user::admin_user;

#[tokio::test]
async fn test_update_collection_validate() {
    let db = td_database::test_utils::db().await.unwrap();
    let admin_id = admin_user(&db).await;
    seed_collection(&db, None, "ds0").await;

    let service = UpdateCollectionService::new(db.clone()).service().await;

    let update = CollectionUpdate::builder().build().unwrap();

    let request = RequestContext::with(&admin_id.to_string(), "r", true)
        .await
        .update(CollectionParam::new("ds0"), update);

    assert_service_error(service, request, |err| match err {
        CollectionError::UpdateRequestHasNothingToUpdate => {}
        other => panic!(
            "Expected 'UpdateRequestHasNothingToUpdate', got {:?}",
            other
        ),
    })
    .await;
}
