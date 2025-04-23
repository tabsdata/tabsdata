//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::error::CollectionError;
use crate::collections::service::create_collection::CreateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::collections::dto::CollectionCreateBuilder;
use td_objects::crudl::RequestContext;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};

#[tokio::test]
async fn test_create_already_existing() {
    let db = td_database::test_utils::db().await.unwrap();
    seed_collection(&db, None, "ds0").await;

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
        RoleId::sys_admin(),
        false,
    )
    .create((), create);

    assert_service_error(service, request, |err| match err {
        CollectionError::AlreadyExists => {}
        other => panic!("Expected 'AlreadyExists', got {:?}", other),
    })
    .await;
}
