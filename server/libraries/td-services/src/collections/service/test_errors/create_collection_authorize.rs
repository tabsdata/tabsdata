//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::service::create_collection::CreateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::collections::dto::CollectionCreateBuilder;
use td_objects::crudl::RequestContext;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};

#[tokio::test]
async fn test_not_allowed_to_create_collection() {
    let db = td_database::test_utils::db().await.unwrap();

    let service = CreateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let create = CollectionCreateBuilder::default()
        .name("ds0")
        .description("DS0")
        .build()
        .unwrap();

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::user(),
        false,
    )
    .create((), create);

    assert_service_error(service, request, |err| match err {
        td_objects::tower_service::authz::AuthzError::UnAuthorized(_) => {}
        other => panic!("Expected 'Unauthorized', got {:?}", other),
    })
    .await;
}
