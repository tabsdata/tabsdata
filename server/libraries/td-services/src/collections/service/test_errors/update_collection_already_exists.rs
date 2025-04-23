//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::error::CollectionError;
use crate::collections::service::update_collection::UpdateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_error::assert_service_error;
use td_objects::collections::dto::CollectionUpdateBuilder;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::types::basic::{AccessTokenId, RoleId, UserId};

#[tokio::test]
async fn test_update_already_existing() {
    let db = td_database::test_utils::db().await.unwrap();

    seed_collection(&db, None, "ds0").await;
    seed_collection(&db, None, "ds1").await;

    let service = UpdateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let update = CollectionUpdateBuilder::default()
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
    .update(CollectionParam::new("ds1"), update);

    assert_service_error(service, request, |err| match err {
        CollectionError::AlreadyExists => {}
        other => panic!("Expected 'AlreadyExists', got {:?}", other),
    })
    .await;
}
