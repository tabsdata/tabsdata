//
// Copyright 2024 Tabs Data Inc.
//

use crate::collections::service::update::UpdateCollectionService;
use crate::collections::CollectionError;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::assert_service_error;
use td_objects::crudl::RequestContext;
use td_objects::rest_urls::CollectionParam;
use td_objects::test_utils::seed_collection2::seed_collection;
use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
use td_objects::types::collection::CollectionUpdate;

#[td_test::test(sqlx)]
async fn test_update_collection_validate(db: DbPool) {
    let name = CollectionName::try_from("c0").unwrap();
    let _ = seed_collection(&db, &name, &UserId::admin()).await;

    let update = CollectionUpdate::builder()
        .name(None)
        .description(None)
        .build()
        .unwrap();

    let request = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
        false,
    )
    .update(
        CollectionParam::builder()
            .try_collection(name.to_string())
            .unwrap()
            .build()
            .unwrap(),
        update,
    );

    let service = UpdateCollectionService::new(db, Arc::new(AuthzContext::default()))
        .service()
        .await;
    assert_service_error(service, request, |err| match err {
        CollectionError::UpdateRequestHasNothingToUpdate => {}
        other => panic!(
            "Expected 'UpdateRequestHasNothingToUpdate', got {:?}",
            other
        ),
    })
    .await;
}
