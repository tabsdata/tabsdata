//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::update::UpdateCollectionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection::seed_collection;
use td_objects::tower_service::authz::AuthzError;
use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
use td_objects::types::collection::CollectionUpdate;

#[td_test::test(sqlx)]
async fn test_not_allowed_to_update_collection(db: DbPool) {
    let name = CollectionName::try_from("ds0").unwrap();
    let _ = seed_collection(&db, &name, &UserId::admin()).await;

    let service = UpdateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
        .service()
        .await;

    let name = CollectionName::try_from("ds00").unwrap();

    let update = CollectionUpdate::builder()
        .name(Some(name.clone()))
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
        .update(
            CollectionParam::builder()
                .try_collection(name.to_string())
                .unwrap()
                .build()
                .unwrap(),
            update,
        );

    assert_service_error(service, request, |err| match err {
        AuthzError::Forbidden(_) => {}
        other => panic!("Expected 'Forbidden', got {other:?}"),
    })
    .await;
}
