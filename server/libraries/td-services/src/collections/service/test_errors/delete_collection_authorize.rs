//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::collections::service::delete_collection::DeleteCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};

#[tokio::test]
async fn test_not_allowed_to_delete_collection() {
    let db = td_database::test_utils::db().await.unwrap();
    let _ = seed_collection(&db, None, "ds0").await;

    let service = DeleteCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::user(),
        false,
    )
    .delete(CollectionParam::new("ds0"));

    assert_service_error(service, request, |err| match err {
        AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'Unauthorized', got {:?}", other),
    })
    .await;
}
